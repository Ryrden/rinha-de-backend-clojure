(ns rinha.core
  (:require [rinha.routes :as routes]
            [rinha.workers :as workers]
            [rinha.monitoring :as monitoring]
            [org.httpkit.server :as server])
  (:gen-class))

(defn ^:private initialize-health-monitoring!
  "Performs initial health checks on startup"
  []
  (println "=== Initializing Health Monitoring ===")
  (let [default-url (System/getenv "PROCESSOR_DEFAULT_URL")
        fallback-url (System/getenv "PROCESSOR_FALLBACK_URL")]
    
    (println "Checking default processor health...")
    (future (monitoring/check-processor-health! default-url :default))
    
    (println "Checking fallback processor health...")  
    (future (monitoring/check-processor-health! fallback-url :fallback))
    
    (println "Health monitoring initialized")))

(defn start-system!
  "Starts the complete system with HTTP server, workers, and monitoring"
  [port num-workers]
  (let [app (routes/create-app)]
    (server/run-server app {:port port :join? false})
    (workers/start-workers! num-workers)
    (initialize-health-monitoring!)

    (println (str "=== Rinha Payment System Started ==="))
    (println (str "HTTP Server: localhost:" port))
    (println (str "Workers: " num-workers " payment processing workers"))
    (println (str "====================================="))))

(defn -main
  "Main entry point - starts the complete payment system"
  []
  (let [port (Integer/parseInt (System/getenv "PORT"))
        num-workers (Integer/parseInt (System/getenv "NUM_WORKERS"))] 
    (println "Starting system...")
    (start-system! port num-workers)))
