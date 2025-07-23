(ns rinha.utils
  (:require [muuntaja.core :as m])
  (:import [java.time Instant]))

(defn valid-uuid?
  "Validates if string is a valid UUID"
  [uuid-string]
  (some? (parse-uuid uuid-string))) 

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
