(ns rinha.worker
  (:require [clojure.core.async :as async]
            [org.httpkit.client :as http]
            [rinha.db :as db]
            [rinha.redis :as redis]
            [rinha.logic :as logic]
            [rinha.strategy :as strategy]
            [rinha.queue :as queue]
            [muuntaja.core :as m])
  (:import [java.time Instant]))

(def ^:private payment-processor-default-url (System/getenv "PROCESSOR_DEFAULT_URL"))
(def ^:private payment-processor-fallback-url (System/getenv "PROCESSOR_FALLBACK_URL"))
(def ^:private max-retries 3)
(def ^:private worker-timeout 1) ; seconds to wait for messages

(defn ^:private send-payment-to-processor!
  "Sends payment to a specific processor - only does HTTP POST"
  [processor-url correlation-id amount requested-at]
  (let [url (str processor-url "/payments")
        payload {:correlationId correlation-id
                 :amount amount
                 :requestedAt requested-at}]
    (try
      (let [response @(http/post url
                                 {:headers {"Content-Type" "application/json"}
                                  :body (let [encoded (m/encode m/instance "application/json" payload)]
                                          (if (string? encoded)
                                            encoded
                                            (slurp encoded)))
                                  :timeout 200})]
        {:status (:status response)
         :body (:body response)})
      (catch Exception e
        (println "HTTP request failed:" (.getMessage e))
        {:status nil :error (.getMessage e)}))))

(defn ^:private save-payment-to-db!
  "Saves payment to database"
  [correlation-id amount requested-at processor]
  (try
    (db/execute!
     "INSERT INTO payments (correlation_id, amount, requested_at, processor) VALUES (?::uuid, ?, ?::timestamp, ?)"
     correlation-id amount requested-at (name processor))
    true
    (catch Exception e
      (println "Database save failed:" (.getMessage e))
      false)))

(defn ^:private get-processor-choice!
  "Gets processor choice using cache if valid, otherwise evaluates fresh and caches"
  []
  (let [cached-processor (redis/get-cached-processor)
        default-health (redis/get-processor-health :default)
        fallback-health (redis/get-processor-health :fallback)
        {:keys [choice from-cache reason]} (logic/get-processor-choice-with-cache
                                             cached-processor
                                             default-health
                                             fallback-health
                                             payment-processor-default-url
                                             payment-processor-fallback-url)]
    (when-not from-cache
      (println "Cache miss - evaluating fresh processor choice:" reason)
      (redis/set-cached-processor! choice reason))
    choice))

(defn ^:private handle-processor-failure!
  "Handles processor failure by invalidating cache and choosing new processor"
  [failed-processor]
  (let [cached-processor (redis/get-cached-processor)]
    (when (logic/should-invalidate-cache-on-failure? failed-processor cached-processor)
      (println "Invalidating cache due to processor failure:" failed-processor)
      (redis/invalidate-cached-processor!))))

(defn ^:private check-and-handle-circuit-breaker!
  "Checks and handles circuit breaker state"
  []
  (let [default-health (redis/get-processor-health :default)
        fallback-health (redis/get-processor-health :fallback)
        circuit-breaker-result (strategy/handle-circuit-breaker-state! 
                                 default-health 
                                 fallback-health
                                 payment-processor-default-url
                                 payment-processor-fallback-url)]
    (cond
      ;; Circuit breaker was just activated
      (:circuit-breaker-activated circuit-breaker-result)
      {:circuit-breaker-active true}
      
      ;; Test request succeeded - processor is back online
      (:success circuit-breaker-result)
      (let [working-processor (:processor circuit-breaker-result)]
        (println "Circuit breaker test successful - caching working processor:" (:processor working-processor))
        (redis/set-cached-processor! working-processor "circuit-breaker-recovery")
        {:circuit-breaker-recovered true :processor working-processor})
      
      ;; Circuit breaker is active but test requests failed
      (logic/is-circuit-breaker-active? (redis/get-circuit-breaker-state))
      {:circuit-breaker-active true}
      
      ;; Normal operation
      :else
      {:circuit-breaker-active false})))

(defn ^:private process-payment-with-routing!
  "Processes payment with smart routing based on business rules"
  [correlation-id amount requested-at]
  (let [circuit-breaker-state (check-and-handle-circuit-breaker!)]
    (cond
      ;; Circuit breaker is active - return 503
      (:circuit-breaker-active circuit-breaker-state)
      {:status 503 :error "Service temporarily unavailable - circuit breaker active"}
      
      ;; Circuit breaker just recovered - use the recovered processor
      (:circuit-breaker-recovered circuit-breaker-state)
      (let [processor-choice (:processor circuit-breaker-state)
            response (send-payment-to-processor! (:url processor-choice)
                                                 correlation-id
                                                 amount
                                                 requested-at)
            {:keys [status error processor]} (logic/parse-payment-response response (:processor processor-choice))]
        (when (= status 200)
          (save-payment-to-db! correlation-id amount requested-at processor))
        {:status status :processor processor :error error})
      
      ;; Normal processing
      :else
      (let [processor-choice (get-processor-choice!)
            
            ;; Try primary processor
            primary-response (send-payment-to-processor! (:url processor-choice)
                                                         correlation-id
                                                         amount
                                                         requested-at)
            {:keys [status error processor]} (logic/parse-payment-response primary-response (:processor processor-choice))]
        
        (cond
          ;; Success or already exists - save to DB if success
          (or (= status 200) (= status 422))
          (do
            (when (= status 200)
              (save-payment-to-db! correlation-id amount requested-at processor))
            {:status status :processor processor :error error})
          
          ;; Status 500 - Processor failed, invalidate cache and choose by minResponseTime
          (logic/should-check-both-processors-after-500? status)
          (do
            (handle-processor-failure! processor)
            (strategy/check-both-processors! payment-processor-default-url payment-processor-fallback-url)
            (let [updated-default-health (redis/get-processor-health :default)
                  updated-fallback-health (redis/get-processor-health :fallback)]
              ;; Check if both processors are failing after health check
              (if (logic/both-processors-failing? updated-default-health updated-fallback-health)
                (do
                  (println "Both processors failing after 500 error - will activate circuit breaker on next request")
                  {:status 500 :error "All processors failed"})
                (let [best-choice-after-check (logic/choose-processor-by-min-response-time 
                                               updated-default-health 
                                               updated-fallback-health
                                               payment-processor-default-url
                                               payment-processor-fallback-url)
                      _ (redis/set-cached-processor! best-choice-after-check "min-response-time-after-500")
                      retry-response (send-payment-to-processor! (:url best-choice-after-check)
                                                                 correlation-id
                                                                 amount
                                                                 requested-at)
                      {:keys [status error processor]} (logic/parse-payment-response retry-response (:processor best-choice-after-check))]
                  (when (= status 200)
                    (save-payment-to-db! correlation-id amount requested-at processor))
                  {:status status :processor processor :error error}))))
          
          ;; Status nil - Timeout, try fallback
          (logic/should-try-fallback-after-timeout? status)
          (do
            (handle-processor-failure! processor)
            (let [fallback-choice (logic/get-fallback-processor (:processor processor-choice)
                                                                payment-processor-default-url
                                                                payment-processor-fallback-url)
                  fallback-response (send-payment-to-processor! (:url fallback-choice)
                                                                correlation-id
                                                                amount
                                                                requested-at)
                  {:keys [status error processor]} (logic/parse-payment-response fallback-response (:processor fallback-choice))]
              (cond
                ;; Fallback succeeded
                (or (= status 200) (= status 422))
                (do
                  (redis/set-cached-processor! fallback-choice "fallback-after-timeout")
                  (when (= status 200)
                    (save-payment-to-db! correlation-id amount requested-at processor))
                  {:status status :processor processor :error error})
                
                ;; Fallback also timed out - check both processors and choose by minResponseTime
                :else
                (do
                  (strategy/check-both-processors! payment-processor-default-url payment-processor-fallback-url)
                  (let [updated-default-health (redis/get-processor-health :default)
                        updated-fallback-health (redis/get-processor-health :fallback)]
                    ;; Check if both processors are failing after health check
                    (if (logic/both-processors-failing? updated-default-health updated-fallback-health)
                      (do
                        (println "Both processors failing after double timeout - will activate circuit breaker on next request")
                        {:status 503 :error "All processors failed - service temporarily unavailable"})
                      (let [best-choice-after-check (logic/choose-processor-by-min-response-time 
                                                     updated-default-health 
                                                     updated-fallback-health
                                                     payment-processor-default-url
                                                     payment-processor-fallback-url)
                            _ (redis/set-cached-processor! best-choice-after-check "min-response-time-after-double-timeout")
                            retry-response (send-payment-to-processor! (:url best-choice-after-check)
                                                                       correlation-id
                                                                       amount
                                                                       requested-at)
                            {:keys [status error processor]} (logic/parse-payment-response retry-response (:processor best-choice-after-check))]
                        (when (= status 200)
                          (save-payment-to-db! correlation-id amount requested-at processor))
                        {:status status :processor processor :error error})))))))
          
          ;; Other errors
          :else
          (do
            (handle-processor-failure! processor)
            {:status status :processor processor :error error}))))))

(defn ^:private process-payment-message!
  "Processes a single payment message from the queue"
  [message]
  (let [{:keys [id correlation-id amount requested-at retry-count]} message]
    (try
      (println "Processing payment message:" id "correlation-id:" correlation-id "retry:" retry-count)
      (let [result (process-payment-with-routing! correlation-id amount requested-at)
            {:keys [status error]} result]
        (cond
          ;; Success
          (or (= status 200) (= status 422))
          (do
            (queue/mark-payment-completed! id)
            (println "Payment processed successfully:" id "status:" status))
          
          ;; Temporary failures - retry
          (or (= status 503) (= status 500) (nil? status))
          (let [retry-result (queue/retry-payment! id max-retries)]
            (if (:success retry-result)
              (println "Payment queued for retry:" id "retry count:" (:retry-count retry-result))
              (do
                (queue/mark-payment-failed! id (str "Processing failed after retries: " error))
                (println "Payment failed after max retries:" id))))
          
          ;; Permanent failures
          :else
          (do
            (queue/mark-payment-failed! id (str "Processing failed: " error))
            (println "Payment failed permanently:" id "status:" status))))
      (catch Exception e
        (println "Error processing payment message:" id "error:" (.getMessage e))
        (queue/mark-payment-failed! id (str "Processing exception: " (.getMessage e)))))))

(defn ^:private worker-loop!
  "Main worker loop that processes messages from the queue"
  [worker-id stop-chan]
  (println "Worker" worker-id "started")
  (async/go
    (loop []
      (println "Worker" worker-id "waiting for message...")
      (let [dequeue-chan (async/thread 
                          (try
                            (queue/dequeue-payment! worker-timeout)
                            (catch Exception e
                              (println "Worker" worker-id "dequeue error:" (.getMessage e))
                              nil)))]
        (async/alt!
          stop-chan
          (do
            (println "Worker" worker-id "stopping")
            :stopped)
          
          dequeue-chan
          ([message]
           (if message
             (do
               (println "Worker" worker-id "got message:" (:id message))
               ;; Process message in a separate thread to avoid blocking
               (async/thread (process-payment-message! message)))
             (println "Worker" worker-id "no message, continuing..."))
           (recur)))))))

(defn start-worker!
  "Starts a payment processing worker"
  [worker-id]
  (let [stop-chan (async/chan)
        worker-go-chan (worker-loop! worker-id stop-chan)]
    {:worker-id worker-id
     :stop-chan stop-chan
     :worker-go-chan worker-go-chan
     :started-at (System/currentTimeMillis)}))

(defn stop-worker!
  "Stops a payment processing worker"
  [worker-info]
  (let [{:keys [worker-id stop-chan worker-go-chan]} worker-info]
    (println "Stopping worker" worker-id)
    (async/close! stop-chan)
    ;; Wait for worker to finish (with timeout)
    (async/alt!!
      worker-go-chan ([_] (println "Worker" worker-id "stopped"))
      (async/timeout 5000) (println "Worker" worker-id "stop timeout"))))

(defn start-workers!
  "Starts multiple payment processing workers"
  [num-workers]
  (println "Starting" num-workers "payment processing workers")
  (doall
    (for [i (range num-workers)]
      (start-worker! (str "payment-worker-" i)))))

(defn stop-workers!
  "Stops all payment processing workers"
  [workers]
  (println "Stopping all workers")
  (doseq [worker workers]
    (stop-worker! worker))
  (println "All workers stopped"))

(defn get-worker-stats
  "Gets worker statistics combined with queue stats"
  []
  (let [queue-stats (queue/get-queue-stats)]
    (assoc queue-stats
           :workers-active true
           :max-retries max-retries
           :worker-timeout worker-timeout))) 