(ns rinha.redis
  (:require [taoensso.carmine :as car]))

(def redis-conn {:pool {} :spec {:uri (System/getenv "REDIS_URL")}})

(defmacro redis-cmd [& body]
  `(car/wcar redis-conn ~@body))

(defn ping
  "Pings Redis to check connectivity"
  []
  (try
    (redis-cmd (car/ping))
    (catch Exception e
      (println "Redis ping failed:" (.getMessage e))
      false)))

(defn lpush!
  "Pushes a value to the left of a Redis list"
  [key value]
  (redis-cmd (car/lpush key value)))

(defn brpop!
  "Blocking right pop from Redis list with timeout"
  [key timeout-seconds]
  (redis-cmd (car/brpop key timeout-seconds)))