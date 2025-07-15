(ns rinha.queue
  (:require [rinha.redis :as redis]
            [rinha.utils :as utils]))

(def ^:private queue-name "payment-queue")
#_(def ^:private failed-queue-name "payment-failed")

(defn enqueue-payment!
  "Enqueues a payment for processing"
  [correlation-id amount]
  (let [message {:correlation-id correlation-id
                 :amount amount 
                 :created-at (System/currentTimeMillis)}
        serialized-message (utils/serialize-message message)]
    (try
      (redis/lpush! queue-name serialized-message)
      {:success true :correlation-id correlation-id}
      (catch Exception e
        (println "Failed to enqueue payment:" (.getMessage e))
        {:success false :error (.getMessage e)}))))

(defn dequeue-payment!
  "Dequeues a payment for processing (blocking pop with timeout)"
  [timeout-seconds]
  (try
    (let [result (redis/brpop! queue-name timeout-seconds)]
      (when result
        (let [message-str (second result)]
          (utils/deserialize-message message-str))))
    (catch Exception e
      (println "Failed to dequeue payment:" (.getMessage e)))))