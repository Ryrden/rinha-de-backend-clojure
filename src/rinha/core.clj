(ns rinha.core
  (:require [rinha.routes :as routes]
            [rinha.redis :as redis]
            [rinha.db :as db]
            [rinha.services :as services]
            [rinha.startup :as startup]
            [org.httpkit.server :as server])
  (:gen-class))

;; System state to track running components
(def system-state (atom {:server nil :workers nil :monitoring nil}))

(defn start-monitoring!
  "Starts system monitoring thread"
  []
  (let [monitoring-thread
        (Thread. 
         (fn []
           (while (not (Thread/interrupted))
             (try
               (Thread/sleep 10000) ; Monitor every 10 seconds
               (when-not (Thread/interrupted)
                 (let [status (startup/get-system-status)]
                   (println "=== System Monitoring ===")
                   (println "Queue Stats:" (:queue-stats status))
                   (println "Worker Stats:" (:worker-stats status))
                   (println "=========================")))
               (catch InterruptedException e
                 (.interrupt (Thread/currentThread))
                 (println "Monitoring thread interrupted"))
               (catch Exception e
                 (println "Monitoring error:" (.getMessage e)))))))]
    (.start monitoring-thread)
    (println "System monitoring started")
    monitoring-thread))

(defn stop-monitoring!
  "Stops system monitoring"
  [monitoring-thread]
  (when monitoring-thread
    (.interrupt monitoring-thread)
    (println "System monitoring stopped")))

(defn stop-system!
  "Stops the complete system"
  []
  (let [{:keys [server workers monitoring]} @system-state]
    (println "Stopping Rinha Payment System...")
    
    ;; Stop monitoring
    (stop-monitoring! monitoring)
    
    ;; Stop workers
    (when workers
      (services/stop-payment-workers! workers)
      (println "Payment workers stopped"))
    
    ;; Stop server
    (when server
      (server)
      (println "HTTP server stopped"))
    
    ;; Reset state
    (reset! system-state {:server nil :workers nil :monitoring nil})
    (println "System stopped successfully")))

(defn start-system!
  "Starts the complete system with HTTP server, workers, and monitoring"
  [port num-workers]
  (let [app (routes/create-app)
        server (server/run-server app {:port port :join? false})
        workers (services/start-payment-workers! num-workers)
        monitoring (start-monitoring!)]
    
    (swap! system-state assoc 
           :server server
           :workers workers
           :monitoring monitoring)
    
    (println (str "=== Rinha Payment System Started ==="))
    (println (str "HTTP Server: localhost:" port))
    (println (str "Workers: " num-workers " payment processing workers"))
    (println (str "Monitoring: Active (every 30 seconds)"))
    (println (str "====================================="))
    
    ;; Add shutdown hook
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. #(stop-system!)))
    
    system-state))

(defn system-status
  "Gets current system status"
  []
  (startup/get-system-status))

(defn manual-monitor
  "Manually trigger system monitoring"
  []
  (startup/monitor-system))

(defn process-test-payment
  "Process a test payment for debugging"
  []
  (startup/process-sample-payment!))

(defn get-failed-payments
  "Get failed payments for inspection"
  [limit]
  (services/get-failed-payments limit))

(defn retry-payment
  "Retry a failed payment by message ID"
  [message-id]
  (services/retry-failed-payment! message-id))

(defn clear-queue
  "Clear a queue - USE WITH CAUTION"
  [queue-type]
  (services/clear-queue! queue-type))

(defn test-queue
  "Test queue operations"
  []
  ((requiring-resolve 'rinha.queue/test-queue-operations)))

;; REPL convenience functions
(comment
  ;; Check system status
  (system-status)
  
  ;; Manual monitoring
  (manual-monitor)
  
  ;; Test queue operations
  (test-queue)
  
  ;; Process test payment
  (process-test-payment)
  
  ;; Check failed payments
  (get-failed-payments 10)
  
  ;; Retry failed payment
  ;; (retry-payment "some-message-id")
  
  ;; Clear queues (be careful!)
  ;; (clear-queue :failed)
  ;; (clear-queue :processing)
  ;; (clear-queue :pending)
  )

(defn -main
  "Main entry point - starts the complete payment system"
  []
  (let [port (Integer/parseInt (System/getenv "PORT"))
        num-workers (Integer/parseInt (or (System/getenv "NUM_WORKERS") "3"))]
    
    (println "=== Rinha Payment System Initialization ===")
    
    ;; Check connections
    (if (db/ping)
      (println "✓ Database connection successful")
      (do
        (println "✗ Database connection failed")
        (System/exit 1)))
    
    (if (redis/ping)
      (println "✓ Redis connection successful")
      (do
        (println "✗ Redis connection failed")
        (System/exit 1)))
    
    (println "✓ All connections established")
    (println "==========================================")
    
    ;; Start the complete system
    (start-system! port num-workers)
    
    ;; Keep main thread alive
    (let [keep-running (atom true)]
      (while @keep-running
        (try
          (Thread/sleep 1000)
          (catch InterruptedException e
            (println "Main thread interrupted, shutting down...")
            (reset! keep-running false)
            (stop-system!)))))))
