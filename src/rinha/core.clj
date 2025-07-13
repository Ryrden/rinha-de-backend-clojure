(ns rinha.core
  (:require [rinha.routes :as routes]
            [rinha.redis :as redis]
            [rinha.db :as db]
            [org.httpkit.server :as server])
  (:gen-class))

(defn start-server
  "Starts the HTTP server with the ring application"
  [port]
  (let [app (routes/create-app)
        server (server/run-server app {:port port :join? false})]
    (println (str "Server started on port " port))
    server))

(defn -main
  "Main entry point - starts the HTTP server"
  []
  (let [port (Integer/parseInt (System/getenv "PORT"))]
    (if (db/ping)
      (println "Database connection successful")
      (println "Database connection failed"))
    (if (redis/ping)
      (println "Redis connection successful")
      (println "Redis connection failed"))
    (start-server port)
    (println "Press Ctrl+C to stop")))
