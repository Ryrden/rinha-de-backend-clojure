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