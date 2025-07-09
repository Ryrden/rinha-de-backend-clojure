(ns rinha.services)

(defn get-hello-world
  "Returns a hello world message"
  []
  {:message   "Hello, World!"
   :status    "success"
   :timestamp (System/currentTimeMillis)})

(defn get-health-check
  "Returns health check information"
  []
  {:status    "ok"
   :service   "rinha-clojure-api"
   :version   "1.0.0"
   :timestamp (System/currentTimeMillis)})
