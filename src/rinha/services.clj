(ns rinha.services
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [rinha.db :as db]
            [rinha.redis :as redis]
            [rinha.logic :as logic]
            [muuntaja.core :as m])
  (:import [java.time Instant]))

(def ^:private payment-processor-default-url (System/getenv "PROCESSOR_DEFAULT_URL"))
(def ^:private payment-processor-fallback-url (System/getenv "PROCESSOR_FALLBACK_URL"))

(defn get-hello-world!
  "Returns a hello world message"
  []
  {:message   "Hello, World!"
   :status    "success"
   :timestamp (System/currentTimeMillis)})

(defn check-processor-health!
  "Checks processor health - respects 5-second rate limit"
  [processor-url processor-type]
  (when (redis/should-check-health? processor-type)
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
  []
  (check-processor-health! payment-processor-default-url :default)
  (check-processor-health! payment-processor-fallback-url :fallback))

(defn ^:private evaluate-circuit-breaker!
  "Evaluates and updates circuit breaker state based on processors health"
  [default-health fallback-health]
  (let [should-activate (redis/should-circuit-breaker-be-active? default-health fallback-health)
        is-active (redis/is-circuit-breaker-active?)]
    (cond
      (and should-activate (not is-active))
      (redis/set-circuit-breaker-state! true)
      (and (not should-activate) is-active)
      (redis/reset-circuit-breaker!))))

(defn ^:private send-payment-to-processor!
  "Sends payment to a specific processor"
  [processor-url processor-type correlation-id amount requested-at]
  (let [url (str processor-url "/payments")
        payload {:correlationId correlation-id
                 :amount amount
                 :requestedAt requested-at}]
    (try
      (let [response @(http/post url
                                 {:headers {"Content-Type" "application/json"}
                                  :body (m/encode m/instance "application/json" payload)
                                  :timeout 350})] 
        (case (:status response)
          200 (do
                (db/execute!
                 "INSERT INTO payments (correlation_id, amount, requested_at, processor) VALUES (?::uuid, ?, ?::timestamp, ?)"
                 correlation-id amount requested-at (name processor-type))
                {:status 200 :processor processor-type})
          422 {:status 422 :error "Payment already exists"}
          (do
            (check-both-processors!)
            {:status 500 :error (str "error: " (:body response))})))
      (catch Exception e
        (println "Unexpected error in payment processing:" (.getMessage e))
        (check-both-processors!)
        {:status 500 :error (str "error: " (.getMessage e))}))))

(defn ^:private process-payment-with-smart-routing!
  "Processes payment with smart routing based on health status"
  [correlation-id amount requested-at]
  (let [default-health (redis/get-processor-health :default)
        fallback-health (redis/get-processor-health :fallback)]

    (evaluate-circuit-breaker! default-health fallback-health)

    (cond
      (:active (redis/get-circuit-breaker-state))
      (do
        (println "Circuit breaker allowing test request")
        (let [best-choice (logic/get-best-processor default-health
                                                    fallback-health
                                                    payment-processor-default-url
                                                    payment-processor-fallback-url)
              test-result (send-payment-to-processor! (:url best-choice)
                                                      (:processor best-choice)
                                                      correlation-id
                                                      amount
                                                      requested-at)]
          (if (= (:status test-result) 200)
            (do
              (println "Test request successful - resetting circuit breaker")
              (redis/reset-circuit-breaker!)
              test-result)
            (do
              (println "Test request failed - keeping circuit breaker active")
              {:status 503 :error "Service temporarily unavailable - test request failed"}))))

      :else
      (let [best-choice (logic/get-best-processor default-health
                                                  fallback-health
                                                  payment-processor-default-url
                                                  payment-processor-fallback-url)
            primary-result (send-payment-to-processor! (:url best-choice)
                                                       (:processor best-choice)
                                                       correlation-id
                                                       amount
                                                       requested-at)]
        (condp = (:status primary-result)
          200 primary-result
          422 primary-result
          (let [fallback-choice (if (= (:processor best-choice) :default)
                                  {:processor :fallback :url payment-processor-fallback-url}
                                  {:processor :default :url payment-processor-default-url})]
            (send-payment-to-processor! (:url fallback-choice)
                                        (:processor fallback-choice)
                                        correlation-id
                                        amount
                                        requested-at)))))))

(defn ^:private process-payment-async!
  "Processes a payment asynchronously with smart routing"
  [correlation-id amount requested-at result-chan]
  (async/go
    (try
      (let [result (process-payment-with-smart-routing! correlation-id amount requested-at)]
        (async/>! result-chan result))
      (catch Exception e
        (println "Processing error:" (.getMessage e))
        (async/>! result-chan {:status 500 :error (str "Processing error: " (.getMessage e))})))))

(defn process-payment!
  "Creates a new payment - handles business logic processing"
  [correlation-id amount]
  (let [requested-at (str (Instant/now))
        result-chan (async/chan 1)]
    (process-payment-async! correlation-id amount requested-at result-chan)
    (async/alt!!
      result-chan ([result] result)
      (async/timeout 8000) {:status 408 :error "Processing timeout"})))

(defn ^:private execute-summary-query!
  "Executes the summary query with proper error handling"
  [query params]
  (try
    (if (empty? params)
      (db/execute! query)
      (apply db/execute! query params))
    (catch Exception e
      (println "Database error in summary query:" (.getMessage e))
      [])))

(defn get-payments-summary
  "Gets payments summary with optional date filters"
  [from to]
  (let [{:keys [query params]} (logic/build-summary-query from to)
        results (execute-summary-query! query params)]
    (logic/format-summary-results results)))
