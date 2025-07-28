(ns rinha.redis
  (:require [taoensso.carmine :as car]
            [clojure.java.io :as io]
            [rinha.utils :as utils]))

(def redis-conn {:pool {} :spec {:uri (System/getenv "REDIS_URL")}})

(defmacro redis-cmd [& body]
  `(car/wcar redis-conn ~@body))

(def ^:private queue-name "payment-queue")
(def ^:private processing-queue-name "payment-processing-queue")
(def ^:private failed-queue-name "payment-failed")

(def summary-lua (slurp (io/resource "summary.lua")))

(defn save-payment!
  [correlation_id amount requested_at processor]
  (let [key "payments:summary"
        payload (utils/serialize-message {:correlation_id correlation_id
                                          :amount amount
                                          :requested_at (utils/iso->unix-ts requested_at)
                                          :processor (name processor)})]
    (redis-cmd (car/zadd key (utils/iso->unix-ts requested_at) payload))))

(defn payment-summary
  [from to]
  (redis-cmd
   (car/lua
    summary-lua
    {:key "payments:summary"}
    {:from (str from)
     :to (str to)})))

(defn get-payments-summary
  "Gets payments summary from Redis counters with optional date filtering"
  [from to]
  (let [result (payment-summary (utils/iso->unix-ts from) (utils/iso->unix-ts to))]
    {:default {:totalRequests (nth result 0)
               :totalAmount (parse-double (nth result 1))}
     :fallback {:totalRequests (nth result 2)
                :totalAmount (parse-double (nth result 3))}}))

(defn enqueue-payment!
  "Enqueues a payment for processing"
  ([correlation-id amount]
   (enqueue-payment! correlation-id amount 0))
  ([correlation-id amount retry-count]
   (let [message {:correlation-id correlation-id
                  :amount amount
                  :retry-count retry-count}
         serialized-message (utils/serialize-message message)]
     (try
       (redis-cmd (car/lpush queue-name serialized-message))
       {:success true :correlation-id correlation-id}
       (catch Exception e
         (println "Failed to enqueue payment:" (.getMessage e))
         {:success false :error (.getMessage e)})))))

(defn dequeue-payment!
  "Dequeues a payment for processing"
  []
  (try
    (when-let [result (redis-cmd (car/brpoplpush queue-name processing-queue-name 5))]
      (merge (utils/deserialize-message result) {:original_serialized_message result}))
    (catch Exception e
      (println "Failed to dequeue payment:" (.getMessage e)))))

(defn mark-payment-as-completed!
  "Marks a payment as completed"
  [message]
  (redis-cmd
   (car/lrem processing-queue-name 1 (:original_serialized_message message))))

(defn enqueue-failed-payment!
  "Enqueues a failed payment"
  [message]
  (redis-cmd
   (car/lpush failed-queue-name
              (utils/serialize-message message))))