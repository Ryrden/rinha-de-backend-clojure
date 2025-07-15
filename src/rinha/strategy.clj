(ns rinha.strategy
  (:require [rinha.redis :as redis]
            [org.httpkit.client :as http]
            [muuntaja.core :as m]))

(defn should-circuit-breaker-be-active?
  "Checks if circuit breaker should be active based on both processors health"
  [default-health fallback-health]
  (and (:failing default-health) (:failing fallback-health)))

(defn is-circuit-breaker-active?
  "Checks if circuit breaker is currently active and should block requests"
  []
  (let [cb-state (redis/get-circuit-breaker-state)
        current-time (System/currentTimeMillis)
        time-since-activation (- current-time (:activated-at cb-state))
        circuit-breaker-duration 500]
    (and (:active cb-state) (< time-since-activation circuit-breaker-duration))))

(defn evaluate-circuit-breaker!
  "Evaluates and updates circuit breaker state based on processors health"
  [default-health fallback-health]
  (let [should-activate (should-circuit-breaker-be-active? default-health fallback-health)
        is-active (is-circuit-breaker-active?)]
    (cond
      (and should-activate (not is-active))
      (redis/set-circuit-breaker-state! true)
      (and (not should-activate) is-active)
      (redis/reset-circuit-breaker!))))

(defn should-check-health?
  "Checks if enough time has passed since last health check"
  [processor-type]
  (let [current-time (System/currentTimeMillis)
        health-data (redis/get-processor-health processor-type)
        last-check (:last-check health-data)
        time-since-last-check (- current-time last-check)]
    (>= time-since-last-check 5100)))

(defn check-processor-health!
  "Checks processor health - respects 5-second rate limit"
  [processor-url processor-type]
  (when (should-check-health? processor-type)
    (try
      (let [response @(http/get (str processor-url "/payments/service-health"))]
        (condp = (:status response)
          200 (let [health-data (m/decode m/instance "application/json" (:body response))]
                (redis/set-processor-health! processor-type
                                             {:failing (:failing health-data)
                                              :minResponseTime (:minResponseTime health-data)
                                              :last-check (System/currentTimeMillis)}))
          429 (println "Payment processor is rate limited")
          (do
            (println "Error in health check for" (name processor-type) ":" response)
            (redis/set-processor-health! processor-type
                                         {:failing true
                                          :minResponseTime 5000
                                          :last-check (System/currentTimeMillis)}))))
      (catch Exception e
        (println "Health check failed for" (name processor-type) ":" (.getMessage e))))))

(defn check-both-processors!
  "Checks both processors health"
  [default-url fallback-url]
  (check-processor-health! default-url :default)
  (check-processor-health! fallback-url :fallback)) 