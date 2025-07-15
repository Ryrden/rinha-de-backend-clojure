(ns rinha.services
  (:require [clojure.core.async :as async]
            [org.httpkit.client :as http]
            [rinha.db :as db]
            [rinha.redis :as redis]
            [rinha.logic :as logic]
            [rinha.strategy :as strategy]
            [rinha.queue :as queue]
            [rinha.worker :as worker]
            [muuntaja.core :as m])
  (:import [java.time Instant]))

<<<<<<< HEAD
;; Payment processing is now handled by workers
;; All payment processing logic has been moved to worker.clj
=======
(def ^:private payment-processor-default-url "http://payment-processor-default:8080")
(def ^:private payment-processor-fallback-url "http://payment-processor-fallback:8080")
>>>>>>> parent of 4de3624 (v1 done)

(defn get-hello-world!
  "Returns a hello world message"
  []
  {:message   "Hello, World!"
   :status    "success"
   :timestamp (System/currentTimeMillis)})

<<<<<<< HEAD
(defn process-payment!
  "Creates a new payment - enqueues it for processing by workers"
  [correlation-id amount]
  (try
    (let [enqueue-result (queue/enqueue-payment! correlation-id amount)]
      (if (:success enqueue-result)
        {:status 202 
         :message "Payment accepted for processing"
         :message-id (:message-id enqueue-result)
         :correlation-id correlation-id}
        {:status 500 
         :error "Failed to enqueue payment"
         :details (:error enqueue-result)}))
    (catch Exception e
      (println "Error enqueuing payment:" (.getMessage e))
      {:status 500 :error (str "Enqueuing error: " (.getMessage e))})))
=======
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
    (if (:success default-result)
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
        (async/>! result-chan {:success false :error (str "Processing error: " (.getMessage e))})))))

(defn process-payment!
  "Creates a new payment - handles business logic and validation"
  [correlation-id amount]
  (if-not (logic/valid-payment-data? {:correlationId correlation-id :amount amount})
    {:success false :error "Invalid payment data"}
    (let [requested-at (str (Instant/now))
          result-chan (async/chan 1)]
      (process-payment-async! correlation-id amount requested-at result-chan)
      (async/alt!!
        result-chan ([result] result)
        (async/timeout 8000) {:success false :error "Processing timeout"}))))
>>>>>>> parent of 4de3624 (v1 done)

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

;; Worker management functions
(defn start-payment-workers!
  "Starts payment processing workers"
  [num-workers]
  (worker/start-workers! num-workers))

(defn stop-payment-workers!
  "Stops payment processing workers"
  [workers]
  (worker/stop-workers! workers))

(defn get-queue-stats
  "Gets queue statistics"
  []
  (queue/get-queue-stats))

(defn get-worker-stats
  "Gets worker statistics"
  []
  (worker/get-worker-stats))

(defn get-failed-payments
  "Gets failed payments for inspection"
  [limit]
  (queue/get-failed-payments limit))

(defn retry-failed-payment!
  "Retries a failed payment"
  [message-id]
  (queue/retry-payment! message-id 3))

(defn clear-queue!
  "Clears a queue - USE WITH CAUTION"
  [queue-type]
  (queue/clear-queue! queue-type))
