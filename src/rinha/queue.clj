(ns rinha.queue
  (:require [rinha.redis :as redis]
            [muuntaja.core :as m]
            [clojure.core.async :as async])
  (:import [java.time Instant]
           [java.util UUID]))

(def ^:private queue-name "payment-queue")
(def ^:private processing-queue-name "payment-processing")
(def ^:private failed-queue-name "payment-failed")

(defn- serialize-message
  "Serializes a message to JSON string"
  [message]
  (try
    (let [encoded (m/encode m/instance "application/json" message)]
      (if (string? encoded)
        encoded
        (slurp encoded)))
    (catch Exception e
      (println "Failed to serialize message:" (.getMessage e))
      (throw e))))

(defn- deserialize-message
  "Deserializes a message from JSON string"
  [message-str]
  (try
    (when (and message-str (not (empty? message-str)))
      (m/decode m/instance "application/json" message-str))
    (catch Exception e
      (println "Failed to deserialize message:" (.getMessage e))
      nil)))

(defn- generate-message-id
  "Generates a unique message ID"
  []
  (str (UUID/randomUUID)))

(defn enqueue-payment!
  "Enqueues a payment for processing"
  [correlation-id amount]
  (let [message-id (generate-message-id)
        message {:id message-id
                 :correlation-id correlation-id
                 :amount amount
                 :requested-at (str (Instant/now))
                 :created-at (System/currentTimeMillis)
                 :retry-count 0}
        serialized-message (serialize-message message)]
    (try
      (redis/lpush! queue-name serialized-message)
      (println "Enqueued payment:" correlation-id "with message-id:" message-id)
      {:success true :message-id message-id}
      (catch Exception e
        (println "Failed to enqueue payment:" (.getMessage e))
        {:success false :error (.getMessage e)}))))

(defn dequeue-payment!
  "Dequeues a payment for processing (blocking pop with timeout)"
  [timeout-seconds]
  (try
    (println "Attempting to dequeue with timeout:" timeout-seconds)
    (let [result (redis/brpop! queue-name timeout-seconds)]
      (println "Dequeue result:" result)
      (when result
        (let [message-str (second result)
              message (deserialize-message message-str)]
          (println "Deserialized message:" message)
          (when message
            ;; Move to processing queue for reliability
            (redis/lpush! processing-queue-name (serialize-message message))
            (println "Moved message to processing queue:" (:id message))
            message))))
    (catch Exception e
      (println "Failed to dequeue payment:" (.getMessage e))
      nil)))

(defn mark-payment-completed!
  "Marks a payment as completed and removes from processing queue"
  [message-id]
  (try
    ;; Remove from processing queue
    (let [processing-messages (redis/lrange! processing-queue-name 0 -1)]
      (doseq [msg-str processing-messages]
        (let [msg (deserialize-message msg-str)]
          (when (= (:id msg) message-id)
            (redis/lrem! processing-queue-name 1 msg-str)
            (println "Marked payment as completed:" message-id)))))
    (catch Exception e
      (println "Failed to mark payment as completed:" (.getMessage e)))))

(defn mark-payment-failed!
  "Marks a payment as failed and moves to failed queue"
  [message-id error-reason]
  (try
    ;; Find and remove from processing queue, add to failed queue
    (let [processing-messages (redis/lrange! processing-queue-name 0 -1)]
      (doseq [msg-str processing-messages]
        (let [msg (deserialize-message msg-str)]
          (when (= (:id msg) message-id)
            (redis/lrem! processing-queue-name 1 msg-str)
            (let [failed-message (assoc msg 
                                        :failed-at (System/currentTimeMillis)
                                        :error-reason error-reason)]
              (redis/lpush! failed-queue-name (serialize-message failed-message))
              (println "Marked payment as failed:" message-id "reason:" error-reason))))))
    (catch Exception e
      (println "Failed to mark payment as failed:" (.getMessage e)))))

(defn retry-payment!
  "Retries a failed payment by moving it back to the main queue"
  [message-id max-retries]
  (try
    (let [processing-messages (redis/lrange! processing-queue-name 0 -1)]
      (doseq [msg-str processing-messages]
        (let [msg (deserialize-message msg-str)]
          (when (= (:id msg) message-id)
            (let [retry-count (inc (:retry-count msg 0))]
              (if (< retry-count max-retries)
                (do
                  (redis/lrem! processing-queue-name 1 msg-str)
                  (let [retry-message (assoc msg 
                                            :retry-count retry-count
                                            :retried-at (System/currentTimeMillis))]
                    (redis/lpush! queue-name (serialize-message retry-message))
                    (println "Retried payment:" message-id "retry count:" retry-count)
                    {:success true :retry-count retry-count}))
                (do
                  (mark-payment-failed! message-id "Max retries exceeded")
                  (println "Max retries exceeded for payment:" message-id)
                  {:success false :reason "Max retries exceeded"})))))))
    (catch Exception e
      (println "Failed to retry payment:" (.getMessage e))
      {:success false :error (.getMessage e)})))

(defn get-queue-stats
  "Gets queue statistics"
  []
  (try
    {:pending-payments (redis/llen! queue-name)
     :processing-payments (redis/llen! processing-queue-name)
     :failed-payments (redis/llen! failed-queue-name)
     :timestamp (System/currentTimeMillis)}
    (catch Exception e
      (println "Failed to get queue stats:" (.getMessage e))
      {:error (.getMessage e)})))

(defn get-failed-payments
  "Gets failed payments for manual inspection"
  [limit]
  (try
    (let [failed-messages (redis/lrange! failed-queue-name 0 (dec limit))]
      (map deserialize-message failed-messages))
    (catch Exception e
      (println "Failed to get failed payments:" (.getMessage e))
      [])))

(defn clear-queue!
  "Clears a specific queue - USE WITH CAUTION"
  [queue-type]
  (try
    (let [queue-to-clear (case queue-type
                          :pending queue-name
                          :processing processing-queue-name
                          :failed failed-queue-name)]
      (redis/del! queue-to-clear)
      (println "Cleared queue:" queue-type)
      {:success true})
    (catch Exception e
      (println "Failed to clear queue:" (.getMessage e))
      {:success false :error (.getMessage e)})))

(defn test-queue-serialization
  "Test queue serialization - for debugging"
  []
  (try
    (println "=== Testing Queue Serialization ===")
    (let [test-message {:id "test-123"
                       :correlation-id "test-correlation"
                       :amount 100.0
                       :requested-at "2024-01-01T00:00:00Z"
                       :created-at (System/currentTimeMillis)
                       :retry-count 0}]
      (println "Original message:" test-message)
      (let [serialized (serialize-message test-message)]
        (println "Serialized type:" (type serialized))
        (println "Serialized content:" serialized)
        (let [deserialized (deserialize-message serialized)]
          (println "Deserialized:" deserialized)
          (println "Round-trip successful:" (= (str test-message) (str deserialized)))
          {:success true 
           :serialized serialized 
           :deserialized deserialized})))
         (catch Exception e
       (println "Test failed:" (.getMessage e))
       {:success false :error (.getMessage e)})))

(defn test-queue-operations
  "Test basic queue operations"
  []
  (try
    (println "=== Testing Queue Operations ===")
    
    ;; Test enqueue
    (let [enqueue-result (enqueue-payment! "test-queue-123" 99.99)]
      (println "Enqueue result:" enqueue-result)
      
      (if (:success enqueue-result)
        (do
          (println "Enqueue successful, testing dequeue...")
          ;; Test dequeue
          (let [dequeue-result (dequeue-payment! 2)]
            (println "Dequeue result:" dequeue-result)
            
            (if dequeue-result
              (do
                (println "Dequeue successful!")
                (println "Message ID:" (:id dequeue-result))
                (println "Correlation ID:" (:correlation-id dequeue-result))
                {:success true :message "Queue operations working"})
              {:success false :error "Dequeue failed - no message"})))
        {:success false :error "Enqueue failed"}))
    
    (catch Exception e
      (println "Test failed:" (.getMessage e))
      {:success false :error (.getMessage e)}))) 