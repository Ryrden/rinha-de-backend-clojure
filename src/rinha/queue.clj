(ns rinha.queue
  (:require [taoensso.carmine :as car]
            [rinha.redis-db :as redis]
            [rinha.utils :as utils]))

(def ^:private queue-name "payment-queue")
(def ^:private processing-queue-name "payment-processing-queue")
(def ^:private failed-queue-name "payment-failed")

(defn enqueue-payment!
  "Enqueues a payment for processing"
  [correlation-id amount]
  (let [message {:correlation-id correlation-id
                 :amount amount
                 :created-at (System/currentTimeMillis)}
        serialized-message (utils/serialize-message message)]
    (try
      (redis/redis-cmd (car/lpush queue-name serialized-message))
      {:success true :correlation-id correlation-id}
      (catch Exception e
        (println "Failed to enqueue payment:" (.getMessage e))
        {:success false :error (.getMessage e)}))))

(defn dequeue-payment!
  "Dequeues a payment for processing"
  []
  (try
    (when-let[result (redis/redis-cmd (car/brpoplpush queue-name processing-queue-name 5))] 
      (merge (utils/deserialize-message result) {:original_serialized_message result}))
    (catch Exception e
      (println "Failed to dequeue payment:" (.getMessage e)))))

(defn mark-payment-as-completed!
  "Marks a payment as completed"
  [message]
  (redis/redis-cmd
   (car/lrem processing-queue-name 1 (:original_serialized_message message))))

(defn enqueue-failed-payment!
  "Enqueues a failed payment"
  [message]
  (redis/redis-cmd
   (car/lpush failed-queue-name
               (utils/serialize-message message))))