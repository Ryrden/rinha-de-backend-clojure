(ns rinha.logic
  (:require [clojure.string :as str]
            [rinha.utils :as utils]))

(defn valid-payment-data?
  "Basic validation for payment data"
  [{:keys [correlationId amount]}]
  (and
   (string? correlationId)
   (not (str/blank? correlationId))
   (utils/valid-uuid? correlationId)
   (number? amount)
   (pos? amount)))

(defn is-cached-processor-valid?
  "Checks if cached processor is still valid (within 10 seconds)"
  [cached-processor]
  (when cached-processor
    (let [current-time (System/currentTimeMillis)
          decided-at (:decided-at cached-processor)]
      (when decided-at
        (let [time-since-decision (- current-time decided-at)]
          (< time-since-decision 5000)))))) ; 5 seconds = 5000ms

(defn should-use-cached-processor?
  "Determines if cached processor should be used"
  [cached-processor]
  (and cached-processor
       (is-cached-processor-valid? cached-processor)))

(defn should-invalidate-cache-on-failure?
  "Determines if cache should be invalidated when current processor fails"
  [failed-processor cached-processor]
  (and cached-processor
       (= failed-processor (:processor cached-processor))))

(defn both-processors-failing?
  "Checks if both processors are failing"
  [default-health fallback-health]
  (and (:failing default-health) (:failing fallback-health)))

(defn is-circuit-breaker-active?
  "Checks if circuit breaker is currently active (within 3 seconds)"
  [circuit-breaker-state]
  (when circuit-breaker-state
    (let [current-time (System/currentTimeMillis)
          activated-at (:activated-at circuit-breaker-state)
          time-since-activation (- current-time activated-at)]
      (and (:active circuit-breaker-state)
           (< time-since-activation 3000))))) ; 3 seconds = 3000ms

(defn should-activate-circuit-breaker?
  "Determines if circuit breaker should be activated"
  [default-health fallback-health circuit-breaker-state]
  (and (both-processors-failing? default-health fallback-health)
       (not (is-circuit-breaker-active? circuit-breaker-state))))

(defn should-make-test-request?
  "Determines if test request should be made during circuit breaker"
  [circuit-breaker-state]
  (is-circuit-breaker-active? circuit-breaker-state))

(defn get-test-request-order
  "Returns processors in order for testing (default first, then fallback)"
  [default-url fallback-url]
  [{:processor :default :url default-url}
   {:processor :fallback :url fallback-url}])

(defn should-retry-payment?
  "Determines if a payment should be retried based on status and attempt count"
  [http-status attempt max-attempts]
  (and (not (contains? #{200 422} http-status))
       (< attempt max-attempts)))

(defn get-fallback-processor
  "Returns the fallback processor info based on the primary processor"
  [primary-processor default-url fallback-url]
  (if (= primary-processor :default)
    {:processor :fallback :url fallback-url}
    {:processor :default :url default-url}))

(defn should-try-fallback?
  "Determines if fallback processor should be tried based on primary result"
  [primary-result]
  (let [status (:status primary-result)]
    (and (not (contains? #{200 422} status))
         (not (nil? status)))))

(defn calculate-retry-delay
  "Calculates exponential backoff delay for retry attempts"
  [attempt base-delay]
  (* base-delay (Math/pow 2 (dec attempt))))

(defn choose-processor-by-min-response-time
  "Chooses processor with smaller minResponseTime"
  [default-health fallback-health default-url fallback-url]
  (let [default-response-time (:minResponseTime default-health 0)
        fallback-response-time (:minResponseTime fallback-health 0)]
    (if (<= default-response-time fallback-response-time)
      {:processor :default :url default-url}
      {:processor :fallback :url fallback-url})))

(defn should-check-both-processors-after-500?
  "Determines if both processors should be checked after 500 error"
  [status]
  (= status 500))

(defn should-try-fallback-after-timeout?
  "Determines if fallback should be tried after timeout"
  [status]
  (nil? status))

(defn ^:private calculate-processor-score
  "Calculates a comprehensive score for a processor considering multiple factors:
   - Performance: Response time with exponential penalty for high latency
   - Availability: Failure state with partial penalty rather than complete exclusion
   - Cost: Processing fees (lower is better)
   
   Returns a score between 0-100 (higher is better)"
  [health base-fee-rate]
  (let [response-time (:minResponseTime health)
        is-failing (:failing health)

        ;; Performance scoring (0-40 points) - exponential penalty for high response times
        performance-score (cond
                            (>= response-time 5000) 0     ; Timeout - unusable
                            (>= response-time 2000) 5     ; Very slow - heavily penalized
                            (>= response-time 1000) 15    ; Slow - moderately penalized
                            (>= response-time 500) 25     ; Moderate - lightly penalized
                            (>= response-time 100) 35     ; Good - minor penalty
                            (> response-time 50) 38      ; Very good - minimal penalty
                            :else 40)                    ; Excellent - no penalty

        ;; Availability scoring (0-35 points) - partial penalty for failures
        availability-score (if is-failing 10 35)  ; Failing processors get 10/35 instead of 0

        ;; Cost scoring (0-25 points) - based on fee rate
        cost-score (cond
                     (<= base-fee-rate 0.05) 25   ; 5% fee - excellent
                     (<= base-fee-rate 0.10) 20   ; 10% fee - good
                     (<= base-fee-rate 0.15) 15   ; 15% fee - acceptable
                     (<= base-fee-rate 0.20) 10   ; 20% fee - poor
                     :else 5)                     ; >20% fee - very poor

        total-score (+ performance-score availability-score cost-score)]

    {:total-score total-score
     :performance-score performance-score
     :availability-score availability-score
     :cost-score cost-score}))

(defn get-best-processor
  "Determines the best processor based on health status and performance bonus optimization
   
   Decision priority:
   1. Availability (failing processors are avoided)
   2. Expected value calculation (considering fees + performance bonus)
   
   Fees: Default=5%, Fallback=15%
   Performance bonus: up to 20% for sub-11ms response times"
  [default-health fallback-health default-url fallback-url]
  (let [default-score (calculate-processor-score default-health 0.05)  ; 5% fee
        fallback-score (calculate-processor-score fallback-health 0.15) ; 15% fee
        choice (if (> (:total-score default-score) (:total-score fallback-score))
                 {:processor :default :url default-url}
                 {:processor :fallback :url fallback-url})]
    choice))

(defn get-processor-choice-with-cache
  "Gets processor choice, using cache if valid, otherwise evaluating fresh"
  [cached-processor default-health fallback-health default-url fallback-url]
  (if (should-use-cached-processor? cached-processor)
    {:choice cached-processor
     :from-cache true
     :reason (:reason cached-processor)}
    {:choice (get-best-processor default-health fallback-health default-url fallback-url)
     :from-cache false
     :reason "fresh-evaluation"}))

(defn build-summary-query
  "Builds the SQL query for payments summary with optional date filters"
  [from to]
  (let [base-query "SELECT processor, COUNT(*) as total_requests, COALESCE(SUM(amount), 0) as total_amount FROM payments"
        where-conditions (cond
                           (and from to) " WHERE requested_at >= ?::timestamp AND requested_at <= ?::timestamp"
                           from " WHERE requested_at >= ?::timestamp"
                           to " WHERE requested_at <= ?::timestamp"
                           :else "")
        group-by " GROUP BY processor"
        full-query (str base-query where-conditions group-by)
        params (cond
                 (and from to) [from to]
                 from [from]
                 to [to]
                 :else [])]
    {:query full-query :params params}))

(defn format-summary-results
  "Formats the database results into the expected summary format"
  [results]
  (let [summary-map (reduce
                     (fn [acc row]
                       (let [processor (keyword (:payments/processor row))
                             requests (:total_requests row)
                             amount (double (:total_amount row))]
                         (assoc acc processor {:totalRequests requests
                                               :totalAmount amount})))
                     {}
                     results)]
    (merge {:default {:totalRequests 0 :totalAmount 0.0}
            :fallback {:totalRequests 0 :totalAmount 0.0}}
           summary-map))) 