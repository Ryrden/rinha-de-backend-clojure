(ns rinha.monitoring
  (:require [org.httpkit.client :as http]
            [rinha.redis :as redis]
            [taoensso.carmine :as car]
            [muuntaja.core :as m])
  (:import [java.time Instant]))

(def ^:private health-cache-ttl 5)
(def ^:private health-check-timeout 3000)
(def ^:private circuit-breaker-key "circuit-breaker:status")
(def ^:private circuit-breaker-ttl 5)

(defn ^:private current-timestamp
  "Gets current timestamp in milliseconds"
  []
  (System/currentTimeMillis))

(defn ^:private get-health-cache-key
  "Gets the Redis cache key for processor health"
  [processor]
  (str "health:" (name processor)))

(defn ^:private get-last-health-check-key
  "Gets the Redis cache key for last health check timestamp"
  [processor]
  (str "health:last-check:" (name processor)))

(defn ^:private set-circuit-breaker!
  "Sets the circuit breaker to open state for TTL seconds"
  [reason]
  (let [timestamp (current-timestamp)]
    (redis/redis-cmd
     (car/setex circuit-breaker-key circuit-breaker-ttl 
                (str timestamp ":" reason)))
    (println "CIRCUIT BREAKER ACTIVATED - Reason:" reason)
    (println "   System will reject requests for" circuit-breaker-ttl "seconds to prevent overheating")))

(defn circuit-open?
  "Checks if the circuit breaker is currently open"
  []
  (try
    (let [circuit-data (redis/redis-cmd (car/get circuit-breaker-key))]
      (if circuit-data
        (let [[timestamp reason] (clojure.string/split circuit-data #":" 2)
              breaker-time (Long/parseLong timestamp)
              time-elapsed (- (current-timestamp) breaker-time)]
          (if (< time-elapsed (* circuit-breaker-ttl 1000))
            (do
              (println "Circuit breaker is OPEN - Time remaining:" 
                       (- (* circuit-breaker-ttl 1000) time-elapsed) "ms")
              true)
            (do
              (println "Circuit breaker timeout expired, closing circuit breaker")
              false)))
        false))
    (catch Exception e
      (println "Circuit breaker check failed, assuming closed:" (.getMessage e))
      false)))

(defn activate-circuit-breaker!
  "Activates circuit breaker when both processors fail"
  []
  (set-circuit-breaker! "Both processors failing"))

(defn ^:private call-processor-health!
  "Calls the health endpoint of a processor and measures response time"
  [processor-url processor]
  (let [url (str processor-url "/payments/service-health")
        start-time (System/currentTimeMillis)]
    (try
      (let [{:keys [status body]} @(http/get url {:timeout health-check-timeout})]
        (let [end-time (System/currentTimeMillis)
              response-time (- end-time start-time)]
          (if (= status 200)
            (try
              (let [health-data (m/decode m/instance "application/json" body)
                    processor-failing (get health-data "failing" false)]
                {:processor processor
                 :minResponseTime response-time
                 :failing processor-failing
                 :healthy (not processor-failing)
                 :data health-data
                 :checked-at (System/currentTimeMillis)})
              (catch Exception e
                (println "Failed to parse health response for" processor ":" (.getMessage e))
                {:processor processor
                 :minResponseTime 0
                 :failing false
                 :healthy false
                 :error "Invalid health response"
                 :checked-at (System/currentTimeMillis)}))
            {:processor processor
             :minResponseTime 0
             :failing true
             :healthy false
             :error (str "HTTP " status)
             :checked-at (System/currentTimeMillis)})))
      (catch Exception e
        (println "Health check failed for" processor ":" (.getMessage e))
        {:processor processor
         :minResponseTime 0
         :failing true
         :healthy false
         :error (.getMessage e)
         :checked-at (System/currentTimeMillis)}))))

(defn ^:private store-health-status!
  "Stores processor health status in Redis cache"
  [health-status]
  (let [cache-key (get-health-cache-key (:processor health-status))
        last-check-key (get-last-health-check-key (:processor health-status))
        serialized-status (m/encode m/instance "application/json" health-status)]
    (redis/redis-cmd
     (car/setex cache-key health-cache-ttl (slurp serialized-status))
     (car/setex last-check-key health-cache-ttl (str (System/currentTimeMillis))))))

(defn ^:private get-cached-health-status
  "Gets cached processor health status from Redis"
  [processor]
  (let [cache-key (get-health-cache-key processor)]
    (try
      (when-let [cached-data (redis/redis-cmd (car/get cache-key))]
        (m/decode m/instance "application/json" cached-data))
      (catch Exception e
        (println "Failed to get cached health for" processor ":" (.getMessage e))
        nil))))

(defn ^:private should-check-health?
  "Determines if health check should be performed (respecting cooldown)"
  [processor]
  (let [last-check-key (get-last-health-check-key processor)]
    (try
      (let [last-check-time (redis/redis-cmd (car/get last-check-key))]
        (if last-check-time
          (let [time-since-check (- (System/currentTimeMillis) (Long/parseLong last-check-time))]
            (> time-since-check (* health-cache-ttl 1000)))
          true))
      (catch Exception e
        (println "Failed to check health cooldown for" processor ":" (.getMessage e))
        true))))

(defn check-processor-health!
  "Checks and caches processor health status with cooldown logic"
  [processor-url processor]
  (if (should-check-health? processor)
    (do
      (println "Performing health check for" processor)
      (let [health-status (call-processor-health! processor-url processor)]
        (store-health-status! health-status)
        (println "Health check completed for" processor "- healthy:" (:healthy health-status) 
                 ", response-time:" (:minResponseTime health-status) "ms")
        health-status))
    (do
      (println "Health check for" processor "skipped due to cooldown")
      (get-cached-health-status processor))))

(defn get-processor-health-status
  "Gets current processor health status from cache"
  [processor]
  (or (get-cached-health-status processor)
      {:processor processor
       :minResponseTime 0
       :failing false
       :healthy false
       :error "No health data available"
       :checked-at 0}))

(defn get-best-processor
  "Determines the best processor based on health metrics"
  []
  (let [default-health (get-processor-health-status :default)
        fallback-health (get-processor-health-status :fallback)
        default-has-data (> (:checked-at default-health) 0)
        fallback-has-data (> (:checked-at fallback-health) 0)]

    (when (and default-has-data fallback-has-data
               (:failing default-health) (:failing fallback-health))
      (println "Both processors showing failing status - activating circuit breaker")
      (activate-circuit-breaker!))

    (cond
      (and (:healthy default-health) (:healthy fallback-health)
           default-has-data fallback-has-data)
      (if (<= (:minResponseTime default-health) (:minResponseTime fallback-health))
        :default
        :fallback)

      (and (:healthy default-health) default-has-data)
      :default

      (and (:healthy fallback-health) fallback-has-data)
      :fallback
      
      :else
      :default))) 