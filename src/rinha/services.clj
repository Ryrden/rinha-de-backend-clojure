(ns rinha.services
  (:require [rinha.db :as db]
            [rinha.logic :as logic]
            [rinha.queue :as queue])
  (:import [java.time Instant]))

(defn get-hello-world!
  "Returns a hello world message"
  []
  {:message   "Hello, World!"
   :status    "success"
   :timestamp (System/currentTimeMillis)})

(defn process-payment!
  "Enqueues a payment for processing by workers"
  [correlation-id amount]
  (if-not (logic/valid-payment-data? {:correlationId correlation-id :amount amount})
    {:success false :error "Invalid payment data"}
    (let [enqueue-result (queue/enqueue-payment! correlation-id amount)]
      (if (:success enqueue-result)
        {:success true}
        {:success false :error "Failed to enqueue payment"}))))

(defn ^:private execute-summary-query!
  "Executes the summary query with proper error handling"
  [query params]
  (try
    (if (empty? params)
      (db/execute! query) ; TODO: Check if with apply has same effect
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
