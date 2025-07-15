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

;; Payment processing is now handled by workers
;; All payment processing logic has been moved to worker.clj

(defn get-hello-world!
  "Returns a hello world message"
  []
  {:message   "Hello, World!"
   :status    "success"
   :timestamp (System/currentTimeMillis)})

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
