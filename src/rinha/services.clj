(ns rinha.services
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [rinha.db :as db]
            [muuntaja.core :as m])
  (:import [java.time Instant]))

(def ^:private payment-processor-default-url "http://payment-processor-default:8080")
(def ^:private payment-processor-fallback-url "http://payment-processor-fallback:8080")

(defn get-hello-world
  "Returns a hello world message"
  []
  {:message   "Hello, World!"
   :status    "success"
   :timestamp (System/currentTimeMillis)})

(defn ^:private send-payment-to-processor
  "Sends payment to a specific processor"
  [processor-url processor-type correlation-id amount requested-at]
  (let [url (str processor-url "/payments")
        payload {:correlationId correlation-id
                 :amount amount
                 :requestedAt requested-at}]
    (try
      (let [response @(http/post url 
                                 {:headers {"Content-Type" "application/json"}
                                  :body (m/encode m/instance "application/json" payload)})]
        (case (:status response)
          200 (try
                (db/execute!
                 "INSERT INTO payments (correlation_id, amount, requested_at, processor) VALUES (?::uuid, ?, ?::timestamp, ?)"
                 correlation-id amount requested-at (name processor-type))
                {:success true :processor processor-type}
                (catch Exception e
                  {:success false :error (str "Database error: " (.getMessage e))}))
          422 {:success false :error "Payment already exists"}
          {:success false :error (str "Payment processor error: HTTP " (:status response))}))
      (catch Exception e
        {:success false :error (str "Network error: " (.getMessage e))}))))

(defn ^:private process-payment-with-fallback
  "Processes payment with fallback logic - tries default first, then fallback"
  [correlation-id amount requested-at]
  (let [default-result (send-payment-to-processor 
                        payment-processor-default-url
                        :default
                        correlation-id 
                        amount 
                        requested-at)]
    (if (:success default-result)
      default-result
      (let [fallback-result (send-payment-to-processor 
                             payment-processor-fallback-url
                             :fallback
                             correlation-id 
                             amount 
                             requested-at)]
        fallback-result))))

(defn ^:private process-payment-async
  "Processes a payment asynchronously with fallback"
  [correlation-id amount requested-at result-chan]
  (async/go
    (try
      (let [result (process-payment-with-fallback correlation-id amount requested-at)]
        (async/>! result-chan result))
      (catch Exception e
        (async/>! result-chan {:success false :error (str "Processing error: " (.getMessage e))})))))

(defn ^:private valid-uuid?
  "Validates if string is a valid UUID"
  [uuid-string]
  (some? (parse-uuid uuid-string)))

(defn ^:private valid-payment-data?
  "Basic validation for payment data"
  [{:keys [correlationId amount]}]
  (and
   (string? correlationId)
   (not (str/blank? correlationId))
   (valid-uuid? correlationId)
   (number? amount)
   (pos? amount)))

(defn process-payment
  "Creates a new payment - handles business logic and validation"
  [correlation-id amount]
  (if-not (valid-payment-data? {:correlationId correlation-id :amount amount})
    {:success false :error "Invalid payment data"}
    (let [requested-at (str (Instant/now))
          result-chan (async/chan 1)]
      (process-payment-async correlation-id amount requested-at result-chan)
      (async/alt!!
        result-chan ([result] result)
        (async/timeout 8000) {:success false :error "Processing timeout"}))))

(defn ^:private build-summary-query
  "Builds the SQL query for payments summary with optional date filters"
  [from to]
  (let [base-query "SELECT processor, COUNT(*) as total_requests, COALESCE(SUM(amount), 0) as total_amount FROM payments"
        where-conditions (cond
                           (and from to) " WHERE requested_at >= ?::timestamp AND requested_at <= ?::timestamp"
                           from " WHERE requested_at >= ?::timestamp"
                           to " WHERE requested_at <= ?::timestamp"
                           :else "")
        group-by " GROUP BY processor"
        full-query (str base-query where-conditions group-by)
        params (cond
                 (and from to) [from to]
                 from [from]
                 to [to]
                 :else [])]
    {:query full-query :params params}))

(defn ^:private execute-summary-query
  "Executes the summary query with proper error handling"
  [query params]
  (try
    (if (empty? params)
      (db/execute! query)
      (apply db/execute! query params))
    (catch Exception e
      (println "Database error in summary query:" (.getMessage e))
      [])))

(defn ^:private format-summary-results
  "Formats the database results into the expected summary format"
  [results]
  (let [summary-map (reduce
                     (fn [acc row]
                       (let [processor (keyword (:payments/processor row))
                             requests (:total_requests row)
                             amount (double (:total_amount row))]
                         (assoc acc processor {:totalRequests requests
                                               :totalAmount amount})))
                     {}
                     results)]
    (merge {:default {:totalRequests 0 :totalAmount 0.0}
            :fallback {:totalRequests 0 :totalAmount 0.0}}
           summary-map)))

(defn get-payments-summary
  "Gets payments summary with optional date filters"
  [from to]
  (let [{:keys [query params]} (build-summary-query from to)
        results (execute-summary-query query params)]
    (format-summary-results results)))
