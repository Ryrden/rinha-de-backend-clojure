(ns rinha.services
  (:require [rinha.redis :as redis]
            [rinha.utils :as utils])
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
  (if-not (utils/valid-payment-data? {:correlationId correlation-id :amount amount})
    {:success false :error "Invalid payment data"}
    
    (let [enqueue-result (redis/enqueue-payment! correlation-id amount)]
      (if (:success enqueue-result)
        {:success true}
        {:success false :error "Failed to enqueue payment"}))))

(defn get-payments-summary
  "Gets payments summary with optional date filters from Redis"
  [from to]
  (try
    (redis/get-payments-summary from to)
    (catch Exception e
      (println "Redis error in summary query:" (.getMessage e))
      {:default {:totalRequests 0 :totalAmount 0.0}
       :fallback {:totalRequests 0 :totalAmount 0.0}}))) 