(ns rinha.startup
  (:require [rinha.services :as services]
            [rinha.worker :as worker]
            [rinha.queue :as queue]))

;; Example of how to start the payment processing system with workers

(defn start-payment-system!
  "Starts the payment processing system with workers"
  [num-workers]
  (println "Starting payment processing system with" num-workers "workers")
  
  ;; Start workers
  (let [workers (services/start-payment-workers! num-workers)]
    (println "Payment system started successfully")
    
    ;; Return workers reference for later cleanup
    {:workers workers
     :started-at (System/currentTimeMillis)}))

(defn stop-payment-system!
  "Stops the payment processing system"
  [system]
  (println "Stopping payment processing system")
  
  ;; Stop workers
  (services/stop-payment-workers! (:workers system))
  
  (println "Payment system stopped"))

(defn get-system-status
  "Gets the current status of the payment system"
  []
  (let [queue-stats (services/get-queue-stats)
        worker-stats (services/get-worker-stats)]
    {:queue-stats queue-stats
     :worker-stats worker-stats
     :timestamp (System/currentTimeMillis)}))

(defn process-sample-payment!
  "Processes a sample payment (for testing)"
  []
  (let [correlation-id (str (java.util.UUID/randomUUID))
        amount 100.50
        result (services/process-payment! correlation-id amount)]
    (println "Sample payment result:" result)
    result))

(defn monitor-system
  "Monitors the system and prints statistics"
  []
  (let [status (get-system-status)]
    (println "=== Payment System Status ===")
    (println "Queue Stats:" (:queue-stats status))
    (println "Worker Stats:" (:worker-stats status))
    (println "Timestamp:" (:timestamp status))
    (println "==============================")
    status))

;; Example usage:
(comment
  ;; Start the system with 3 workers
  (def system (start-payment-system! 3))
  
  ;; Process some payments
  (process-sample-payment!)
  (process-sample-payment!)
  
  ;; Monitor the system
  (monitor-system)
  
  ;; Check failed payments
  (services/get-failed-payments 10)
  
  ;; Retry a failed payment
  ;; (services/retry-failed-payment! "some-message-id")
  
  ;; Stop the system
  (stop-payment-system! system)
  ) 