(ns rinha.strategy
  (:require [rinha.redis :as redis]
            [rinha.logic :as logic]
            [org.httpkit.client :as http]
            [muuntaja.core :as m]))

(defn should-circuit-breaker-be-active?
  "Checks if circuit breaker should be active based on both processors health"
  [default-health fallback-health]
  (and (:failing default-health) (:failing fallback-health)))

(defn evaluate-and-activate-circuit-breaker!
  "Evaluates and activates circuit breaker if both processors are failing"
  [default-health fallback-health]
  (let [circuit-breaker-state (redis/get-circuit-breaker-state)]
    (when (logic/should-activate-circuit-breaker? default-health fallback-health circuit-breaker-state)
      (println "Both processors failing - activating circuit breaker for 3 seconds")
      (redis/set-circuit-breaker-state! true)
      true)))

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

(defn make-test-request!
  "Makes a test request to a processor during circuit breaker"
  [processor-url processor-type]
  (let [url (str processor-url "/payments")
        ;; Use a test payload that should be rejected with 422
        test-payload {:correlationId "test-circuit-breaker-request"
                      :amount 1.0
                      :requestedAt (str (java.time.Instant/now))}]
    (try
      (let [response @(http/post url
                                 {:headers {"Content-Type" "application/json"}
                                  :body (m/encode m/instance "application/json" test-payload)
                                  :timeout 200})]
        (let [status (:status response)]
          (println "Test request to" (name processor-type) "returned status:" status)
          ;; Consider 200 and 422 as successful responses (processor is working)
          (if (contains? #{200 422} status)
            {:success true :processor processor-type :status status}
            {:success false :processor processor-type :status status})))
      (catch Exception e
        (println "Test request failed for" (name processor-type) ":" (.getMessage e))
        {:success false :processor processor-type :error (.getMessage e)}))))

(defn try-test-requests!
  "Tries test requests to processors in order during circuit breaker"
  [default-url fallback-url]
  (let [test-order (logic/get-test-request-order default-url fallback-url)]
    (loop [processors test-order]
      (when-let [processor (first processors)]
        (let [result (make-test-request! (:url processor) (:processor processor))]
          (if (:success result)
            (do
              (println "Test request successful for" (name (:processor processor)) "- deactivating circuit breaker")
              (redis/reset-circuit-breaker!)
              {:success true :processor processor :result result})
            (recur (rest processors))))))))

(defn cleanup-expired-circuit-breaker!
  "Cleans up expired circuit breaker state (after 3 seconds)"
  []
  (let [circuit-breaker-state (redis/get-circuit-breaker-state)]
    (when (and (:active circuit-breaker-state)
               (not (logic/is-circuit-breaker-active? circuit-breaker-state)))
      (println "Circuit breaker expired - cleaning up")
      (redis/reset-circuit-breaker!))))

(defn handle-circuit-breaker-state!
  "Handles circuit breaker state - activates if needed, tries test requests if active"
  [default-health fallback-health default-url fallback-url]
  (let [circuit-breaker-state (redis/get-circuit-breaker-state)]
    ;; First, clean up expired circuit breaker
    (cleanup-expired-circuit-breaker!)
    
    ;; Get fresh state after cleanup
    (let [fresh-circuit-breaker-state (redis/get-circuit-breaker-state)]
      (cond
        ;; Circuit breaker is active - try test requests
        (logic/should-make-test-request? fresh-circuit-breaker-state)
        (try-test-requests! default-url fallback-url)
        
        ;; Both processors failing - activate circuit breaker
        (logic/should-activate-circuit-breaker? default-health fallback-health fresh-circuit-breaker-state)
        (do
          (evaluate-and-activate-circuit-breaker! default-health fallback-health)
          {:circuit-breaker-activated true})
        
        ;; Normal operation
        :else
        nil)))) 