(ns rinha.utils
  (:require [muuntaja.core :as m]))

(defn valid-uuid?
  "Validates if string is a valid UUID"
  [uuid-string]
  (some? (parse-uuid uuid-string))) 

(defn serialize-message
  "Serializes a message to JSON string"
  [message]
  (try
    (let [encoded (m/encode m/instance "application/json" message)]
      (if (string? encoded)
        encoded
        (slurp encoded)))
    (catch Exception e
      (println "Failed to serialize message:" (.getMessage e))
      (throw e))))

(defn deserialize-message
  "Deserializes a message from JSON string"
  [message-str]
  (try
    (when (and message-str (not (empty? message-str)))
      (m/decode m/instance "application/json" message-str))
    (catch Exception e
      (println "Failed to deserialize message:" (.getMessage e))
      nil)))
