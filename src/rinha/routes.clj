(ns rinha.routes
  (:require [rinha.handler :as handler]
            [reitit.ring :as ring]
            [reitit.ring.middleware.exception :as exception]
            [jsonista.core :as json]
            [ring.util.response :as response]))

(defn json-response
  "Helper function to create JSON responses"
  [data & [status]]
  (-> (response/response (json/write-value-as-string data))
      (response/content-type "application/json")
      (response/status (or status 200))))

(defn wrap-json-response
  "Middleware to wrap handler responses in JSON"
  [handler-fn]
  (fn [request]
    (let [result        (handler-fn request)
          status        (or (:http-status result) 200)
          response-data (dissoc result :http-status)]
      (json-response response-data status))))

(defn hello-world-route
  "HTTP adapter for hello world endpoint"
  [request]
  ((wrap-json-response handler/handle-hello-world) request))

(defn health-check-route
  "HTTP adapter for health check endpoint"
  [request]
  ((wrap-json-response handler/handle-health-check) request))

(defn not-found-route
  "HTTP adapter for not found responses"
  [request]
  ((wrap-json-response handler/handle-not-found) request))

(def exception-middleware
  "Exception handling middleware"
  exception/exception-middleware)

(def routes
  "Route definitions"
  [["/" {:get hello-world-route}]
   ["/health" {:get health-check-route}]])

(defn create-router
  "Creates the router with middleware"
  []
  (ring/router
   routes
   {:data {:middleware [exception-middleware]}}))

(defn create-app
  "Creates the complete ring application"
  []
  (ring/ring-handler
   (create-router)
   (ring/routes
    (ring/create-default-handler
     {:not-found not-found-route}))))
