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