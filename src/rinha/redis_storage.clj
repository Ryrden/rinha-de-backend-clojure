(ns rinha.redis-storage
  (:require [taoensso.carmine :as car]
            [rinha.redis :as redis]
            [muuntaja.core :as m]
            [rinha.utils :as utils]))

(defn save-payment!
  [correlation_id amount requested_at processor]
  (let [key (str "payments:" (name processor))
        payload (utils/serialize-message {:correlation_id correlation_id
                                          :amount amount
                                          :requested_at (utils/iso->unix-ts requested_at)})]
    (redis/redis-cmd (car/zadd key (utils/iso->unix-ts requested_at) payload))))

(defn payment-summary
  [processor from to] 
  (redis/redis-cmd 
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