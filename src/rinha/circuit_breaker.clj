(ns rinha.circuit-breaker
  (:require [rinha.redis-db :as redis]
            [taoensso.carmine :as car])
  (:import [java.time Instant]))

(def ^:private circuit-breaker-key "circuit-breaker:status")
(def ^:private circuit-breaker-ttl 5) ; 5 seconds TTL when circuit is open

(defn ^:private current-timestamp
  "Gets current timestamp in milliseconds"
  []
  (System/currentTimeMillis))

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
            false))
        false))
    (catch Exception e
      (println "Circuit breaker check failed:" (.getMessage e))
      false)))

(defn activate-circuit-breaker!
  "Activates circuit breaker when both processors fail"
  []
  (set-circuit-breaker! "Both processors failing"))

(defn get-circuit-breaker-status
  "Gets current circuit breaker status for monitoring"
  []
  (try
    (if-let [circuit-data (redis/redis-cmd (car/get circuit-breaker-key))]
      (let [[timestamp reason] (clojure.string/split circuit-data #":" 2)
            breaker-time (Long/parseLong timestamp)
            time-elapsed (- (current-timestamp) breaker-time)
            remaining-time (- (* circuit-breaker-ttl 1000) time-elapsed)]
        {:open true
         :reason reason
         :activated-at breaker-time
         :remaining-ms (max 0 remaining-time)})
      {:open false
       :reason nil
       :activated-at nil
       :remaining-ms 0})
    (catch Exception e
      (println "Failed to get circuit breaker status:" (.getMessage e))
      {:open false
       :reason "Error checking status"
       :activated-at nil
       :remaining-ms 0}))) 