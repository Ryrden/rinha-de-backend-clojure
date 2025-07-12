(ns rinha.services
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [rinha.db :as db]
            [rinha.logic :as logic]
            [muuntaja.core :as m])
  (:import [java.time Instant]))

(def ^:private payment-processor-default-url (System/getenv "PROCESSOR_DEFAULT_URL"))
(def ^:private payment-processor-fallback-url (System/getenv "PROCESSOR_FALLBACK_URL"))

(defn get-hello-world!
  "Returns a hello world message"
  []
  {:message   "Hello, World!"
   :status    "success"
   :timestamp (System/currentTimeMillis)})

(defn ^:private send-payment-to-processor!
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
          200 (do
                (db/execute!
                 "INSERT INTO payments (correlation_id, amount, requested_at, processor) VALUES (?::uuid, ?, ?::timestamp, ?)"
                 correlation-id amount requested-at (name processor-type))
                {:status 200 :processor processor-type})
          422 {:status 422 :error "Payment already exists"}
          {:status (:status response) :error (str "Payment processor error:" (:body response))}))
      (catch Exception e
        (println "Network error with" processor-type "processor:" (.getMessage e))
        {:status 500 :error (str "Network error: " (.getMessage e))}))))

(defn ^:private process-payment-with-fallback!
  "Processes payment with fallback logic - tries default first, then fallback"
  [correlation-id amount requested-at]
  (let [default-result (send-payment-to-processor! payment-processor-default-url
                                                   :default
                                                   correlation-id
                                                   amount
                                                   requested-at)
        fallback-result (delay (send-payment-to-processor! payment-processor-fallback-url
                                                           :fallback
                                                           correlation-id
                                                           amount
                                                           requested-at))]
    (if (= (:status default-result) 200)
      default-result
      @fallback-result)))

(defn ^:private process-payment-async!
  "Processes a payment asynchronously with fallback"
  [correlation-id amount requested-at result-chan]
  (async/go
    (try
      (let [result (process-payment-with-fallback! correlation-id amount requested-at)]
        (async/>! result-chan result))
      (catch Exception e
        (println "Processing error:" (.getMessage e))
        (async/>! result-chan {:status 500 :error (str "Processing error: " (.getMessage e))})))))

(defn process-payment!
  "Creates a new payment - handles business logic processing"
  [correlation-id amount]
  (let [requested-at (str (Instant/now))
        result-chan (async/chan 1)]
    (process-payment-async! correlation-id amount requested-at result-chan)
    (async/alt!!
      result-chan ([result] result)
      (async/timeout 8000) {:status 408 :error "Processing timeout"})))

(defn ^:private execute-summary-query!
  "Executes the summary query with proper error handling"
  [query params]
  (try
    (if (empty? params)
      (db/execute! query)
      (apply db/execute! query params))
    (catch Exception e
      (println "Database error in summary query:" (.getMessage e))
      [])))

(defn get-payments-summary
  "Gets payments summary with optional date filters"
  [from to]
  (let [{:keys [query params]} (logic/build-summary-query from to)
        results (execute-summary-query! query params)]
    (logic/format-summary-results results)))
