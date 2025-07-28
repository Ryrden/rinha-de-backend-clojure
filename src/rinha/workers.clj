(ns rinha.workers
  (:require [clojure.core.async :as async]
            [org.httpkit.client :as http]
            [rinha.redis :as redis]
            [rinha.monitoring :as monitoring]
            [muuntaja.core :as m])
  (:import [java.time Instant]))

(def ^:private payment-processor-default-url (System/getenv "PROCESSOR_DEFAULT_URL"))
(def ^:private payment-processor-fallback-url (System/getenv "PROCESSOR_FALLBACK_URL"))
(def ^:private max-retries 3)
(def ^:private retry-delay-ms 3000)

(defn ^:private save-payment-to-redis!
  "Saves payment to Redis"
  [correlation-id amount requested-at processor]
  (try
    (let [result (redis/save-payment! correlation-id amount requested-at processor)]
      (when (= result 0)
        (println "Payment already exists in Redis:" correlation-id)))
    (catch Exception e
      (println "Redis save failed:" (.getMessage e)))))

(defn ^:private send-payment-to-processor!
  "Sends payment to a specific processor"
  [processor-url processor correlation-id amount timeout]
  (let [url (str processor-url "/payments")
        requested-at (str (Instant/now))
        payload {:correlationId correlation-id
                 :amount amount
                 :requested_at requested-at}]
    (try
      (let [{:keys [status]} @(http/post url
                                         {:headers {"Content-Type" "application/json"}
                                          :body (m/encode m/instance "application/json" payload)
                                          :timeout timeout})]
        (condp = status
          200 (do
                (println "Payment successfully processed by" processor "for correlation-id:" correlation-id)
                (save-payment-to-redis! correlation-id amount requested-at processor)
                {:status 200 :message "Payment processed"})
          422 {:status 422 :message "Payment already exists"}
          500 {:status 500 :message "Processor failed"}
          nil {:status nil :message "Request timeout"}
          {:status status :message (str "HTTP error: " status)}))
      (catch Exception e
        (println "Payment processor" processor "exception for correlation-id:" correlation-id "Error:" (.getMessage e))
        {:status 500 :message (.getMessage e)}))))

(defn ^:private should-retry?
  "Determines if a payment should be retried based on status and retry count"
  [status retry-count]
  (and (< retry-count max-retries)
       (contains? #{500 nil} status)))

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
        current-retry-count (or retry-count 0)
        best-processor (monitoring/get-best-processor)]

    (let [primary-url (get-processor-url best-processor)
          primary-result (send-payment-to-processor! primary-url
                                                     best-processor
                                                     correlation-id
                                                     amount
                                                     5000)]
      (case (:status primary-result)
        200 primary-result
        422 primary-result
        (do
          (async/go (monitoring/check-processor-health! primary-url best-processor))

          (let [fallback-processor (if (= best-processor :default) :fallback :default)
                fallback-url (get-processor-url fallback-processor)
                fallback-result (send-payment-to-processor! fallback-url
                                                            fallback-processor
                                                            correlation-id
                                                            amount
                                                            5000)]

            (when (not= (:status fallback-result) 200)
              (async/go (monitoring/check-processor-health! fallback-url fallback-processor)))

            (if (= (:status fallback-result) 200)
              fallback-result

              (do
                (println "Both processors failed - evaluating circuit breaker activation")

                (monitoring/activate-circuit-breaker!)

                (if (should-retry? (:status fallback-result) current-retry-count)
                  {:status :retry
                   :message "Will retry after delay (both processors failed)"
                   :retry-count (inc current-retry-count)}

                  {:status (:status fallback-result)
                   :message (:message fallback-result)
                   :final-failure true})))))))))

(defn ^:private handle-retry-payment!
  "Handles a payment that needs to be retried"
  [message worker-id]
  (async/go
    (async/<! (async/timeout retry-delay-ms))
    (let [retry-message (assoc message :retry-count (:retry-count message))]
      (redis/enqueue-payment! (:correlation-id retry-message) (:amount retry-message) (:retry-count retry-message))
      (println "Worker" worker-id "re-enqueued payment after delay. Retry count:" (:retry-count retry-message)))))

(defn ^:private worker-loop!
  "Main worker loop that processes messages from the queue"
  [worker-id stop-chan]
  (println "Worker" worker-id "started")
  (async/go
    (loop []
      (let [should-continue?
            (try
              (when (async/poll! stop-chan)
                (println "Worker" worker-id "received stop signal")
                (throw (ex-info "stop-requested" {})))

              (if (monitoring/circuit-open?)
                (do
                  (println "Worker" worker-id "paused - circuit breaker is OPEN")
                  (async/<! (async/timeout 1000))
                  true)


                (let [message (try
                                (redis/dequeue-payment!)
                                (catch Exception e
                                  (println "Worker" worker-id "dequeue failed:" (.getMessage e))
                                  nil))]
                  (if message
                    (do
                      (try
                        (let [result (process-payment-with-retries! (dissoc message :original_serialized_message))]
                          (condp = (:status result)
                            200 (redis/mark-payment-as-completed! message)

                            :retry
                            (do
                              (handle-retry-payment! (merge (dissoc message :original_serialized_message)
                                                            {:retry-count (:retry-count result)}) worker-id)
                              (redis/mark-payment-as-completed! message))

                            (do
                              (redis/enqueue-failed-payment! (dissoc message :original_serialized_message))
                              (redis/mark-payment-as-completed! message)
                              (println "Worker" worker-id "payment failed permanently after retries:" (:status result)))))
                        (catch Exception e
                          (println "Worker" worker-id "error processing payment:" (.getMessage e))
                          (redis/mark-payment-as-completed! message)))
                      true)
                    (do
                      (async/<! (async/timeout 100))
                      true))))

              (catch Exception e
                (if (= "stop-requested" (.getMessage e))
                  false
                  (do
                    (println "Worker" worker-id "critical error in main loop:" (.getMessage e))
                    (async/<! (async/timeout 2000))
                    true))))]

        (if should-continue?
          (recur)
          :stopped)))))

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
