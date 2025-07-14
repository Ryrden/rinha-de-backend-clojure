(ns rinha.redis
  (:require [taoensso.carmine :as car]
            [clojure.string :as str])
  (:import [java.time Instant]
           [java.time.format DateTimeFormatter]))

(def redis-conn {:pool {} :spec {:uri (System/getenv "REDIS_URL")}})

(defmacro redis-cmd [& body]
  `(car/wcar redis-conn ~@body))

;; ===============================================
;; PROCESSOR HEALTH FUNCTIONS
;; ===============================================

(defn processor-health-key
  "Returns the Redis key for processor health"
  [processor-type]
  (str "processor-health:" (name processor-type)))

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

(defn should-check-health?
  "Checks if enough time has passed since last health check"
  [processor-type]
  (let [current-time (System/currentTimeMillis)
        health-data (get-processor-health processor-type)
        last-check (:last-check health-data)
        time-since-last-check (- current-time last-check)]
    (>= time-since-last-check 5100)))

;; ===============================================
;; CIRCUIT BREAKER FUNCTIONS
;; ===============================================

(defn circuit-breaker-key
  "Returns the Redis key for circuit breaker state"
  []
  "circuit-breaker:state")

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
          circuit-breaker-duration 10] 
      (and (:active cb-state) (< time-since-activation circuit-breaker-duration))))

(defn reset-circuit-breaker!
  "Resets circuit breaker to inactive state"
  []
  (try
    (let [cb-key (circuit-breaker-key)]
      (redis-cmd (car/del cb-key)))
    (catch Exception e
      (println "Error resetting circuit breaker:" (.getMessage e)))))

;; ===============================================
;; PAYMENT STORAGE FUNCTIONS
;; ===============================================

(defn payment-key
  "Returns the Redis key for a payment"
  [correlation-id]
  (str "payment:" correlation-id))

(defn payment-time-index-key
  "Returns the Redis key for payment time index"
  []
  "payments:time-index")

(defn payment-processor-index-key
  "Returns the Redis key for payment processor index"
  [processor]
  (str "payments:processor:" (name processor)))

(defn payment-summary-key
  "Returns the Redis key for payment summary"
  [processor]
  (str "payments:summary:" (name processor)))

(defn store-payment!
  "Stores a payment in Redis using multiple data structures for efficient querying"
  [correlation-id amount requested-at processor]
  (try
    (let [payment-data {:correlation-id correlation-id
                        :amount (str amount)
                        :requested-at requested-at
                        :processor (name processor)}
          timestamp (-> (Instant/parse requested-at)
                       (.toEpochMilli))]
      (redis-cmd
       ;; Store payment details as hash
       (car/hset (payment-key correlation-id)
                 "correlation-id" correlation-id
                 "amount" (str amount)
                 "requested-at" requested-at
                 "processor" (name processor))
       
       ;; Add to time-based sorted set for range queries
       (car/zadd (payment-time-index-key) timestamp correlation-id)
       
       ;; Add to processor-specific sorted set
       (car/zadd (payment-processor-index-key processor) timestamp correlation-id)
       
       ;; Update summary counters atomically
       (car/hincrby (payment-summary-key processor) "total-requests" 1)
       (car/hincrbyfloat (payment-summary-key processor) "total-amount" (double amount))))
    true
    (catch Exception e
      (println "Error storing payment to Redis:" (.getMessage e))
      false)))

(defn payment-exists?
  "Checks if a payment with given correlation-id already exists"
  [correlation-id]
  (try
    (let [exists-result (redis-cmd (car/exists (payment-key correlation-id)))]
      (= 1 exists-result))
    (catch Exception e
      (println "Error checking payment existence:" (.getMessage e))
      false)))

(defn get-payment
  "Retrieves a payment by correlation-id"
  [correlation-id]
  (try
    (let [payment-map (->> (redis-cmd (car/hgetall (payment-key correlation-id)))
                          (apply hash-map))]
      (when-not (empty? payment-map)
        {:correlation-id (get payment-map "correlation-id")
         :amount (Double/parseDouble (get payment-map "amount"))
         :requested-at (get payment-map "requested-at")
         :processor (keyword (get payment-map "processor"))}))
    (catch Exception e
      (println "Error retrieving payment:" (.getMessage e))
      nil)))

;; ===============================================
;; PAYMENT SUMMARY FUNCTIONS
;; ===============================================

(defn ^:private parse-timestamp
  "Parses ISO timestamp string to epoch milliseconds"
  [timestamp-str]
  (-> (Instant/parse timestamp-str)
      (.toEpochMilli)))

(defn ^:private get-payments-in-range
  "Gets payment correlation IDs within a time range"
  [from-ts to-ts]
  (try
    (let [min-score (if from-ts from-ts "-inf")
          max-score (if to-ts to-ts "+inf")]
      (redis-cmd (car/zrangebyscore (payment-time-index-key) min-score max-score)))
    (catch Exception e
      (println "Error getting payments in range:" (.getMessage e))
      [])))

(defn ^:private calculate-summary-for-processor
  "Calculates summary for a specific processor within date range"
  [processor from-ts to-ts]
  (try
    (let [processor-key (payment-processor-index-key processor)
          min-score (if from-ts from-ts "-inf")
          max-score (if to-ts to-ts "+inf")
          correlation-ids (redis-cmd (car/zrangebyscore processor-key min-score max-score))]
      
      (if (empty? correlation-ids)
        {:totalRequests 0 :totalAmount 0.0}
        (let [payments (map get-payment correlation-ids)
              valid-payments (filter identity payments)
              total-requests (count valid-payments)
              total-amount (reduce + (map :amount valid-payments))]
          {:totalRequests total-requests
           :totalAmount total-amount})))
    (catch Exception e
      (println "Error calculating summary for processor:" (.getMessage e))
      {:totalRequests 0 :totalAmount 0.0})))

(defn get-payments-summary
  "Gets payments summary with optional date filters"
  [from to]
  (try
    (let [from-ts (when from (parse-timestamp from))
          to-ts (when to (parse-timestamp to))]
      
      (if (and (nil? from) (nil? to))
        ;; No date filters - use cached summary counters
        (let [default-summary (redis-cmd (car/hgetall (payment-summary-key :default)))
              fallback-summary (redis-cmd (car/hgetall (payment-summary-key :fallback)))
              
              default-map (apply hash-map default-summary)
              fallback-map (apply hash-map fallback-summary)]
          
          {:default {:totalRequests (Integer/parseInt (get default-map "total-requests" "0"))
                     :totalAmount (Double/parseDouble (get default-map "total-amount" "0.0"))}
           :fallback {:totalRequests (Integer/parseInt (get fallback-map "total-requests" "0"))
                      :totalAmount (Double/parseDouble (get fallback-map "total-amount" "0.0"))}})
        
        ;; Date filters present - calculate from individual payments
        {:default (calculate-summary-for-processor :default from-ts to-ts)
         :fallback (calculate-summary-for-processor :fallback from-ts to-ts)}))
    (catch Exception e
      (println "Error getting payments summary:" (.getMessage e))
      {:default {:totalRequests 0 :totalAmount 0.0}
       :fallback {:totalRequests 0 :totalAmount 0.0}})))

;; ===============================================
;; UTILITY FUNCTIONS
;; ===============================================

(defn ping
  "Pings Redis to check connectivity"
  []
  (try
    (redis-cmd (car/ping))
    (catch Exception e
      (println "Redis ping failed:" (.getMessage e))
      false))) 