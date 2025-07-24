(ns rinha.routes
  (:require [rinha.handler :as handler]
            [reitit.ring :as ring]
            [reitit.ring.middleware.muuntaja :as muuntaja]
            [reitit.ring.middleware.parameters :as parameters]
            [reitit.ring.middleware.exception :as exception]
            [muuntaja.core :as m]))

(def routes
  [["/" {:get handler/hello-world}]
   ["/payments" {:post handler/create-payment}]
   ["/payments-summary" {:get handler/payments-summary}]])

(defn create-app
  "Creates the complete ring application with built-in Reitit middleware"
  []
  (ring/ring-handler
   (ring/router
    routes
    {:data {:muuntaja m/instance
            :middleware [parameters/parameters-middleware
                         muuntaja/format-middleware
                         exception/exception-middleware]}})
   (ring/routes
    (ring/create-default-handler
     {:not-found (constantly {:status 404 :body {:error "Not found"}})})))) 