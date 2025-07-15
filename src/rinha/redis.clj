(ns rinha.redis
  (:require [taoensso.carmine :as car]))

(def redis-conn {:pool {} :spec {:uri (System/getenv "REDIS_URL")}})

(defmacro redis-cmd [& body]
  `(car/wcar redis-conn ~@body))

(defn processor-health-key
  "Returns the Redis key for processor health"
  [processor-type]
  (str "processor-health:" (name processor-type)))

(defn circuit-breaker-key
  "Returns the Redis key for circuit breaker state"
  []
  "circuit-breaker:state")

(defn cached-processor-key
  "Returns the Redis key for cached processor choice"
  []
  "current-best-processor")

(defn get-processor-health
  "Gets processor health from Redis"
  [processor-type]
  (try
    (let [health-key (processor-health-key processor-type)
          health-map (->> (redis-cmd (car/hgetall health-key))
                          (apply hash-map))] 
      (if-not (empty? health-map)
        {:failing (= "true" (get health-map "failing"))
         :minResponseTime (Integer/parseInt (get health-map "minResponseTime" "0"))
         :last-check (Long/parseLong (get health-map "last-check" "0"))}
        {:failing false :minResponseTime 0 :last-check 0}))
    (catch Exception e
      (println "Error reading health from Redis:" (.getMessage e) "full error:" e)
      {:failing false :minResponseTime 0 :last-check 0})))

(defn set-processor-health!
  "Sets processor health in Redis"
  [processor-type health-data]
  (try
    (let [health-key (processor-health-key processor-type)]
      (redis-cmd
       (car/hset health-key
                 "failing" (str (:failing health-data))
                 "minResponseTime" (str (:minResponseTime health-data))
                 "last-check" (str (System/currentTimeMillis)))))
    (catch Exception e
      (println "Error writing health to Redis:" (.getMessage e)))))

(defn get-cached-processor
  "Gets cached processor choice from Redis"
  []
  (try
    (let [cache-key (cached-processor-key)
          cached-data (->> (redis-cmd (car/hgetall cache-key))
                           (apply hash-map))]
      (if-not (empty? cached-data)
        {:processor (keyword (get cached-data "processor"))
         :url (get cached-data "url")
         :decided-at (Long/parseLong (get cached-data "decided-at" "0"))
         :reason (get cached-data "reason")}
        nil))
    (catch Exception e
      (println "Error reading cached processor from Redis:" (.getMessage e))
      nil)))

(defn set-cached-processor!
  "Sets cached processor choice in Redis with 10-second TTL"
  [processor-choice reason]
  (try
    (let [cache-key (cached-processor-key)
          current-time (System/currentTimeMillis)]
      (redis-cmd
       (car/hset cache-key
                 "processor" (name (:processor processor-choice))
                 "url" (:url processor-choice)
                 "decided-at" (str current-time)
                 "reason" reason)
       (car/expire cache-key 10))) ; 10-second TTL
    (catch Exception e
      (println "Error caching processor choice:" (.getMessage e)))))

(defn invalidate-cached-processor!
  "Invalidates cached processor choice"
  []
  (try
    (let [cache-key (cached-processor-key)]
      (redis-cmd (car/del cache-key)))
    (catch Exception e
      (println "Error invalidating cached processor:" (.getMessage e)))))

(defn get-circuit-breaker-state
  "Gets circuit breaker state from Redis"
  []
  (try
    (let [cb-key (circuit-breaker-key)
          cb-map (->> (redis-cmd (car/hgetall cb-key))
                      (apply hash-map))]
      (if-not (empty? cb-map)
        {:active (= "true" (get cb-map "active"))
         :activated-at (Long/parseLong (get cb-map "activated-at" "0"))
         :last-test (Long/parseLong (get cb-map "last-test" "0"))}
        {:active false :activated-at 0 :last-test 0}))
    (catch Exception e
      (println "Error reading circuit breaker state from Redis:" (.getMessage e))
      {:active false :activated-at 0 :last-test 0})))

(defn set-circuit-breaker-state!
  "Sets circuit breaker state in Redis with 3-second TTL"
  [active]
  (try
    (let [cb-key (circuit-breaker-key)
          current-time (System/currentTimeMillis)]
      (redis-cmd
       (car/hset cb-key
                 "active" (str active)
                 "activated-at" (str current-time)
                 "last-test" (str current-time))
       (car/expire cb-key 2))) ; 2-second TTL for circuit breaker
    (catch Exception e
      (println "Error writing circuit breaker state to Redis:" (.getMessage e)))))

(defn reset-circuit-breaker!
  "Resets circuit breaker to inactive state"
  []
  (try
    (let [cb-key (circuit-breaker-key)]
      (redis-cmd (car/del cb-key)))
    (catch Exception e
      (println "Error resetting circuit breaker:" (.getMessage e)))))

(defn ping
  "Pings Redis to check connectivity"
  []
  (try
    (redis-cmd (car/ping))
    (catch Exception e
      (println "Redis ping failed:" (.getMessage e))
      false)))

;; Queue operations for payment processing
(defn lpush!
  "Pushes a value to the left of a Redis list"
  [key value]
  (redis-cmd (car/lpush key value)))

(defn brpop!
  "Blocking right pop from Redis list with timeout"
  [key timeout-seconds]
  (redis-cmd (car/brpop key timeout-seconds)))

(defn lrange!
  "Gets a range of elements from a Redis list"
  [key start stop]
  (redis-cmd (car/lrange key start stop)))

(defn lrem!
  "Removes count occurrences of value from a Redis list"
  [key count value]
  (redis-cmd (car/lrem key count value)))

(defn llen!
  "Gets the length of a Redis list"
  [key]
  (redis-cmd (car/llen key)))

(defn del!
  "Deletes a Redis key"
  [key]
  (redis-cmd (car/del key))) 