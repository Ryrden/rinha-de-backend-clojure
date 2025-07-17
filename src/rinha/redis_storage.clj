(ns rinha.redis-storage
  (:require [taoensso.carmine :as car]
            [rinha.redis :as redis]
            [muuntaja.core :as m])
  (:import [java.time Instant]))

;; Redis-only storage implementation for maximum optimization
;; Replaces PostgreSQL entirely

(defn save-payment!
  "Saves payment to Redis with duplicate prevention and summary updates"
  [correlation-id amount requested-at processor]
  (let [timestamp (-> (Instant/parse requested-at) (.toEpochMilli))
        processor-name (name processor)
        payment-key (str "payment:" correlation-id)
        duplicate-check-key (str "payments:processed:" processor-name)
        summary-key (str "summary:" processor-name)
        time-index-key (str "payments:time:" processor-name)]
    
    (car/wcar redis/redis-conn
      ;; Check for duplicate using Redis SET
      (let [is-duplicate (car/sismember duplicate-check-key correlation-id)]
        (if (= is-duplicate 1)
          ;; Payment already exists
          {:success false :error "Payment already exists"}
          
          ;; Save payment atomically using Redis transaction
          (do
            (car/multi)
            
            ;; 1. Store individual payment as hash
            (car/hset payment-key
                     "correlation_id" correlation-id
                     "amount" amount
                     "requested_at" requested-at
                     "processor" processor-name
                     "timestamp" timestamp)
            
            ;; 2. Add to duplicate prevention set
            (car/sadd duplicate-check-key correlation-id)
            
            ;; 3. Add to time-based sorted set (for date range queries)
            (car/zadd time-index-key timestamp correlation-id)
            
            ;; 4. Update summary counters atomically
            (car/hincrby summary-key "total_requests" 1)
            (car/hincrbyfloat summary-key "total_amount" amount)
            
            ;; 5. Set expiration for individual payment (optional cleanup)
            (car/expire payment-key (* 7 24 60 60)) ; 7 days
            
            (car/exec)
            {:success true}))))))

(defn get-payments-summary
  "Gets payments summary from Redis counters with optional date filtering"
  [from to]
  (let [from-ms (when from (-> (Instant/parse from) (.toEpochMilli)))
        to-ms (when to (-> (Instant/parse to) (.toEpochMilli)))]
    
    (if (or from-ms to-ms)
      ;; Date filtering required - need to aggregate from time index
      (let [results (car/wcar redis/redis-conn
                      (if (and from-ms to-ms)
                        [(car/zrangebyscore "payments:time:default" from-ms to-ms)
                         (car/zrangebyscore "payments:time:fallback" from-ms to-ms)]
                        (if from-ms 
                          [(car/zrangebyscore "payments:time:default" from-ms "+inf")
                           (car/zrangebyscore "payments:time:fallback" from-ms "+inf")]
                          [(car/zrangebyscore "payments:time:default" "-inf" to-ms)
                           (car/zrangebyscore "payments:time:fallback" "-inf" to-ms)])))
            [default-payments fallback-payments] results
            
            ;; Get payment amounts for filtered payments
            default-amounts (when (seq default-payments)
                             (car/wcar redis/redis-conn
                               (doall (map #(car/hget (str "payment:" %) "amount") default-payments))))
            
            fallback-amounts (when (seq fallback-payments)
                              (car/wcar redis/redis-conn
                                (doall (map #(car/hget (str "payment:" %) "amount") fallback-payments))))]
        
        ;; Calculate totals from filtered data
        {:default {:totalRequests (count default-payments)
                   :totalAmount (reduce + (map #(Double/parseDouble %) (filter some? default-amounts)))}
         :fallback {:totalRequests (count fallback-payments)
                    :totalAmount (reduce + (map #(Double/parseDouble %) (filter some? fallback-amounts)))}})
      
      ;; No date filtering - use fast summary counters
      (let [results (car/wcar redis/redis-conn
                      [(car/hgetall "summary:default")
                       (car/hgetall "summary:fallback")])
            [default-summary fallback-summary] results
            
            ;; Convert Redis hash arrays to maps
            default-map (apply hash-map default-summary)
            fallback-map (apply hash-map fallback-summary)]
        
        {:default {:totalRequests (Integer/parseInt (get default-map "total_requests" "0"))
                   :totalAmount (Double/parseDouble (get default-map "total_amount" "0.0"))}
         :fallback {:totalRequests (Integer/parseInt (get fallback-map "total_requests" "0"))
                    :totalAmount (Double/parseDouble (get fallback-map "total_amount" "0.0"))}}))))

(defn payment-exists?
  "Checks if payment already exists (for duplicate prevention)"
  [correlation-id processor]
  (let [duplicate-check-key (str "payments:processed:" (name processor))]
    (car/wcar redis/redis-conn
      (= (car/sismember duplicate-check-key correlation-id) 1))))

(defn get-payment
  "Gets individual payment details"
  [correlation-id]
  (let [payment-key (str "payment:" correlation-id)]
    (car/wcar redis/redis-conn
      (let [payment-data (car/hgetall payment-key)]
        (when (seq payment-data)
          {:correlation-id (get payment-data "correlation_id")
           :amount (Double/parseDouble (get payment-data "amount"))
           :requested-at (get payment-data "requested_at")
           :processor (get payment-data "processor")})))))

(defn reset-all-data!
  "Resets all payment data (for testing)"
  []
  (car/wcar redis/redis-conn
    (car/flushdb)))

(defn get-stats
  "Gets Redis storage stats for monitoring"
  []
  (car/wcar redis/redis-conn
    (let [default-count (car/scard "payments:processed:default")
          fallback-count (car/scard "payments:processed:fallback")
          memory-usage (car/info "memory")]
      {:default-payments default-count
       :fallback-payments fallback-count
       :total-payments (+ default-count fallback-count)
       :memory-info memory-usage})))

;; Performance optimization: Pre-warm summary counters
(defn initialize-summaries!
  "Initializes summary counters to avoid nil values"
  []
  (car/wcar redis/redis-conn
    (car/hsetnx "summary:default" "total_requests" 0)
    (car/hsetnx "summary:default" "total_amount" 0.0)
    (car/hsetnx "summary:fallback" "total_requests" 0)
    (car/hsetnx "summary:fallback" "total_amount" 0.0))) 