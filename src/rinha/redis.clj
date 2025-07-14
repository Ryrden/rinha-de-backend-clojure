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

(defn get-processor-health
  "Gets processor health from Redis"
  [processor-type]
  (try
    (let [health-key (processor-health-key processor-type)
          health-map (->> (redis-cmd (car/hgetall health-key))
                          (apply hash-map))]
      (println "Health map:" health-map)
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

(defn should-check-health?
  "Checks if enough time has passed since last health check"
  [processor-type]
  (let [current-time (System/currentTimeMillis)
        health-data (get-processor-health processor-type)
        last-check (:last-check health-data)
        time-since-last-check (- current-time last-check)]
    (>= time-since-last-check 5100)))

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
  "Sets circuit breaker state in Redis"
  [active]
  (try
    (let [cb-key (circuit-breaker-key)
          current-time (System/currentTimeMillis)]
      (redis-cmd
       (car/hset cb-key
                 "active" (str active)
                 "activated-at" (str current-time)
                 "last-test" (str current-time))))
    (catch Exception e
      (println "Error writing circuit breaker state to Redis:" (.getMessage e)))))

(defn should-circuit-breaker-be-active?
  "Checks if circuit breaker should be active based on both processors health"
  [default-health fallback-health]
  (and (:failing default-health) (:failing fallback-health)))

(defn is-circuit-breaker-active?
  "Checks if circuit breaker is currently active and should block requests"
  []
      (let [cb-state (get-circuit-breaker-state)
          current-time (System/currentTimeMillis)
          time-since-activation (- current-time (:activated-at cb-state))
          circuit-breaker-duration 3] ; 3ms
      (and (:active cb-state) (< time-since-activation circuit-breaker-duration))))

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