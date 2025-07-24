(ns rinha.utils
  (:require [muuntaja.core :as m]
            [clojure.string :as str])
  (:import [java.time Instant]))

(defn valid-uuid?
  "Validates if string is a valid UUID"
  [uuid-string]
  (some? (parse-uuid uuid-string))) 

(defn valid-payment-data?
  "Basic validation for payment data"
  [{:keys [correlationId amount]}]
  (and
   (string? correlationId)
   (not (str/blank? correlationId))
   (valid-uuid? correlationId)
   (number? amount)
   (pos? amount)))

(defn iso->unix-ts [iso-str]
  (-> (Instant/parse iso-str)
      (.getEpochSecond)))

(defn serialize-message
  "Serializes a message to JSON string"
  [message]
  (try
    (slurp (m/encode m/instance "application/json" message))
    (catch Exception e
      (println "Failed to serialize message:" (.getMessage e))
      (throw e))))

(defn deserialize-message
  "Deserializes a message from JSON string"
  [message]
  (try 
    (m/decode m/instance "application/json" message)
    (catch Exception e
      (println "Failed to deserialize message:" (.getMessage e))
      nil)))
