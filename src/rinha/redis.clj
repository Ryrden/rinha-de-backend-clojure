(ns rinha.redis
  (:require [taoensso.carmine :as car]
            [rinha.utils :as utils]))

(def redis-conn {:pool {} :spec {:uri (System/getenv "REDIS_URL")}})

(defmacro redis-cmd [& body]
  `(car/wcar redis-conn ~@body))

(def ^:private queue-name "payment-queue")
(def ^:private processing-queue-name "payment-processing-queue")
(def ^:private failed-queue-name "payment-failed")

(defn save-payment!
  [correlation_id amount requested_at processor]
  (let [key (str "payments:" (name processor))
        payload (utils/serialize-message {:correlation_id correlation_id
                                          :amount amount
                                          :requested_at (utils/iso->unix-ts requested_at)})]
    (redis-cmd (car/zadd key (utils/iso->unix-ts requested_at) payload))))

(defn payment-summary
  [processor from to]
  (redis-cmd
   (car/lua "
      local items = redis.call('ZRANGEBYSCORE', _:key, _:from, _:to)
      local totalRequests = 0
      local totalAmount = 0.0

      for _, item in ipairs(items) do
        local ok, data = pcall(cjson.decode, item)
        if ok and data.amount then
        totalRequests = totalRequests + 1
        totalAmount = totalAmount + tonumber(data.amount)
        end
      end

      return {totalRequests, totalAmount}"
            {:key (str "payments:" (name processor))}
            {:from (str from) :to (str to)})))

(defn get-payments-summary
  "Gets payments summary from Redis counters with optional date filtering"
  [from to]
  {:default  (zipmap [:totalRequests :totalAmount]
                     (payment-summary :default (utils/iso->unix-ts from) (utils/iso->unix-ts to)))
   :fallback (zipmap [:totalRequests :totalAmount]
                     (payment-summary :fallback (utils/iso->unix-ts from) (utils/iso->unix-ts to)))})

(defn enqueue-payment!
  "Enqueues a payment for processing"
  ([correlation-id amount]
   (enqueue-payment! correlation-id amount 0))
  ([correlation-id amount retry-count]
   (let [message {:correlation-id correlation-id
                  :amount amount
                  :retry-count retry-count
                  :created-at (System/currentTimeMillis)}
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
    (when-let[result (redis-cmd (car/brpoplpush queue-name processing-queue-name 5))] 
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