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
