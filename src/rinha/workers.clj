(ns rinha.workers
  (:require [clojure.core.async :as async]
            [org.httpkit.client :as http]
            [rinha.redis-db :as storage]
            [rinha.queue :as queue]
            [rinha.health :as health]
            [rinha.circuit-breaker :as cb]
            [muuntaja.core :as m])
  (:import [java.time Instant]))

(def ^:private payment-processor-default-url (System/getenv "PROCESSOR_DEFAULT_URL"))
(def ^:private payment-processor-fallback-url (System/getenv "PROCESSOR_FALLBACK_URL"))
(def ^:private worker-timeout 5)
(def ^:private max-retries 3)
(def ^:private retry-delay-ms 3000)

(defn ^:private save-payment-to-redis!
  "Saves payment to Redis"
  [correlation-id amount requested-at processor]
  (try
    (let [result (storage/save-payment! correlation-id amount requested-at processor)]
      (when-not (:success result)
        (println "Payment already exists in Redis:" correlation-id)))
    (catch Exception e
      (println "Redis save failed:" (.getMessage e)))))

(defn ^:private send-payment-to-processor!
  "Sends payment to a specific processor"
  [processor-url processor correlation-id amount timeout]
  (let [url (str processor-url "/payments")
        payload {:correlationId correlation-id
                 :amount amount}]
    (try
      (let [{:keys [status]} @(http/post url
                                         {:headers {"Content-Type" "application/json"}
                                          :body (m/encode m/instance "application/json" payload)
                                          :timeout timeout})]
        (condp = status
          200 (do
                (save-payment-to-redis! correlation-id amount (str (Instant/now)) processor)
                {:status 200 :message "Payment processed"})
          422 {:status 422 :message "Payment already exists"}
          500 {:status 500 :message "Processor failed"}
          nil {:status nil :message "Request timeout"}
          {:status status :message (str "HTTP error: " status)}))
      (catch Exception e
        {:status 500 :message (.getMessage e)}))))

(defn ^:private should-retry?
  "Determines if a payment should be retried based on status and retry count"
  [status retry-count]
  (and (< retry-count max-retries)
       (or (= status 500) (nil? status))))

(defn ^:private get-processor-url
  "Gets the URL for a processor"
  [processor]
  (condp = processor
    :default payment-processor-default-url
    :fallback payment-processor-fallback-url
    payment-processor-default-url))

(defn ^:private process-payment-with-retries!
  "Processes a single payment message with retry logic and health monitoring"
  [message]
  (let [{:keys [correlation-id amount retry-count]} message
        current-retry-count (or retry-count 0)]
    
    ;; Check circuit breaker first
    (if (cb/circuit-open?)
      (do
        (println "Payment rejected - Circuit breaker is OPEN")
        {:status :circuit-open 
         :message "Circuit breaker active - system protection engaged"})
      
      ;; Circuit breaker is closed, proceed normally
      (let [best-processor (health/get-best-processor)]
        
        (println "Processing payment with best processor:" best-processor)
        
        ;; Try the best processor first
        (let [primary-url (get-processor-url best-processor)
              primary-result (send-payment-to-processor! primary-url
                                                         best-processor
                                                         correlation-id
                                                         amount
                                                         5000)]
          (if (= (:status primary-result) 200)
            primary-result
            
            ;; Primary processor failed - trigger health check and try fallback
            (do
              (println "Primary processor" best-processor "failed, checking health and trying fallback")
              
              ;; Trigger health check for the failed processor
              (async/go (health/check-processor-health! primary-url best-processor))
              
              ;; Try the other processor
              (let [fallback-processor (if (= best-processor :default) :fallback :default)
                    fallback-url (get-processor-url fallback-processor)
                    fallback-result (send-payment-to-processor! fallback-url
                                                                fallback-processor
                                                                correlation-id
                                                                amount
                                                                5000)]
                
                ;; If fallback also failed, check its health too
                (when (not= (:status fallback-result) 200)
                  (async/go (health/check-processor-health! fallback-url fallback-processor)))
                
                (if (= (:status fallback-result) 200)
                  fallback-result
                  
                  ;; Both processors failed - consider circuit breaker activation
                  (do
                    (println "Both processors failed - evaluating circuit breaker activation")
                    
                    ;; Activate circuit breaker when both processors fail
                    (cb/activate-circuit-breaker!)
                    
                    ;; Check if we should retry
                    (if (should-retry? (:status fallback-result) current-retry-count)
                      {:status :retry 
                       :message "Will retry after delay (both processors failed)"
                       :retry-count (inc current-retry-count)}
                      
                      ;; Max retries reached - final failure
                      {:status (:status fallback-result) 
                       :message (:message fallback-result)
                       :final-failure true})))))))))))

(defn ^:private handle-retry-payment!
  "Handles a payment that needs to be retried"
  [message worker-id]
  (println "Worker" worker-id "scheduling retry for payment. Retry count:" (:retry-count message))
  
  ;; Wait 3 seconds before re-enqueuing
  (async/go
    (async/<! (async/timeout retry-delay-ms))
    (let [retry-message (assoc message :retry-count (:retry-count message))]
      (queue/enqueue-payment! (:correlation-id retry-message) (:amount retry-message) (:retry-count retry-message))
      (println "Worker" worker-id "re-enqueued payment after delay. Retry count:" (:retry-count retry-message)))))

(defn ^:private worker-loop!
  "Main worker loop that processes messages from the queue"
  [worker-id stop-chan]
  (println "Worker" worker-id "started")
  (async/go
    (loop []
      (let [dequeue-chan (async/thread (queue/dequeue-payment!))]
        (async/alt!
          stop-chan
          (do
            (println "Worker" worker-id "stopping")
            :stopped)

          dequeue-chan
          ([message]
           (when message
             (async/thread
               (let [result (process-payment-with-retries! (dissoc message :original_serialized_message))]
                 (condp = (:status result)
                   200
                   (do
                     (queue/mark-payment-as-completed! message)
                     (println "Worker" worker-id "processed payment successfully: 200"))
                   
                   :circuit-open
                   (do
                     ;; Circuit breaker is open - re-enqueue after short delay
                     (async/go
                       (async/<! (async/timeout 1000)) ; 1 second delay
                       (queue/enqueue-payment! (get (dissoc message :original_serialized_message) :correlation-id)
                                               (get (dissoc message :original_serialized_message) :amount)
                                               (or (get (dissoc message :original_serialized_message) :retry-count) 0)))
                     (queue/mark-payment-as-completed! message)
                     (println "Worker" worker-id "payment re-queued due to circuit breaker"))
                   
                   :retry
                   (do
                     (handle-retry-payment! (merge (dissoc message :original_serialized_message) 
                                                   {:retry-count (:retry-count result)}) worker-id)
                     (queue/mark-payment-as-completed! message))
                   
                   ;; Default case - final failure
                   (do
                     (queue/enqueue-failed-payment! (dissoc message :original_serialized_message))
                     (queue/mark-payment-as-completed! message)
                     (println "Worker" worker-id "payment failed permanently after retries:" (:status result)))))))
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

(defn start-workers!
  "Starts multiple payment processing workers"
  [num-workers]
  (println "Starting" num-workers "payment processing workers")
  (doall
   (for [i (range num-workers)]
     (start-worker! (str "payment-worker-" i)))))
