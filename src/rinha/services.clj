(ns rinha.services
  (:require [rinha.redis-db :as storage]
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

(defn get-payments-summary
  "Gets payments summary with optional date filters from Redis"
  [from to]
  (try
    (storage/get-payments-summary from to)
    (catch Exception e
      (println "Redis error in summary query:" (.getMessage e))
      {:default {:totalRequests 0 :totalAmount 0.0}
       :fallback {:totalRequests 0 :totalAmount 0.0}})))
