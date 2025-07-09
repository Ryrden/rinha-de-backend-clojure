(ns rinha.handler
  (:require [rinha.services :as services]))

(defn handle-hello-world
  "Pure function that handles hello world logic"
  [_request]
  (services/get-hello-world))

(defn handle-health-check
  "Pure function that handles health check logic"
  [_request]
  (services/get-health-check))

(defn handle-not-found
  "Pure function that handles not found logic"
  [_request]
  {:error       "Not found"
   :status      "error"
   :message     "The requested resource was not found"
   :http-status 404})
