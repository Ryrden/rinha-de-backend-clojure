(ns rinha.core
  (:require [rinha.routes :as routes]
            [org.httpkit.server :as server])
  (:gen-class))

(defn create-server-config
  "Creates server configuration"
  [port]
  {:port     port
   :join?    false
   :max-body (* 1024 1024)})

(defn start-server
  "Starts the HTTP server with the ring application"
  [& {:keys [port] :or {port 8080}}]
  (let [app    (routes/create-app)
        config (create-server-config port)
        server (server/run-server app config)]
    (println (str "🚀 Server started on port " port))
    server))

(defn stop-server
  "Stops the HTTP server gracefully"
  [server]
  (when server
    (println "Stopping server...")
    (server :timeout 100)
    (println "Server stopped")))

(defn setup-shutdown-hook
  "Sets up graceful shutdown hook"
  [server]
  (.addShutdownHook (Runtime/getRuntime)
                    (Thread. #(do
                                (println "Shutting down server...")
                                (stop-server server)))))

(defn -main
  "Main entry point - starts the HTTP server"
  []
  (let [server (start-server :port 8080)]
    (setup-shutdown-hook server)
    server))
