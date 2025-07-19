(ns rinha.core
  (:require [rinha.routes :as routes]
            [rinha.redis :as redis]
            [rinha.redis-storage :as storage]
            [rinha.workers :as workers]
            [org.httpkit.server :as server])
  (:gen-class))

(defn start-system!
  "Starts the complete system with HTTP server, workers, and monitoring"
  [port num-workers]
  (let [app (routes/create-app)]
    (server/run-server app {:port port :join? false})
    (workers/start-workers! num-workers)

    (println (str "=== Rinha Payment System Started ==="))
    (println (str "HTTP Server: localhost:" port))
    (println (str "Workers: " num-workers " payment processing workers"))
    (println (str "====================================="))))

(defn -main
  "Main entry point - starts the complete payment system"
  []
  (let [port (Integer/parseInt (System/getenv "PORT"))
        num-workers (Integer/parseInt (System/getenv "NUM_WORKERS"))]
    (println "=== Rinha Payment System Initialization ===")
    (when (redis/ping)
      (println "Redis connection successful"))
    (println "==========================================")
    (println "Starting system...")
    (start-system! port num-workers)))
