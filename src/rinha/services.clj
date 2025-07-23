(ns rinha.services
  (:require [rinha.redis-db :as storage]
            [rinha.logic :as logic]
            [rinha.queue :as queue]
            [rinha.circuit-breaker :as cb])
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
    
    ;; Check circuit breaker before enqueuing
    (if (cb/circuit-open?)
      (let [cb-status (cb/get-circuit-breaker-status)]
        {:success false 
         :error "Service temporarily unavailable" 
         :circuit-breaker true
         :reason "System is in protection mode to prevent overheating"
         :retry-after-ms (:remaining-ms cb-status)})
      
      ;; Circuit breaker is closed, proceed with enqueuing
      (let [enqueue-result (queue/enqueue-payment! correlation-id amount)]
        (if (:success enqueue-result)
          {:success true}
          {:success false :error "Failed to enqueue payment"})))))

(defn get-payments-summary
  "Gets payments summary with optional date filters from Redis"
  [from to]
  (try
    (storage/get-payments-summary from to)
    (catch Exception e
      (println "Redis error in summary query:" (.getMessage e))
      {:default {:totalRequests 0 :totalAmount 0.0}
       :fallback {:totalRequests 0 :totalAmount 0.0}})))
