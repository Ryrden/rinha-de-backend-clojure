(ns rinha.services
  (:require [clojure.core.async :as async]
            [org.httpkit.client :as http]
            [rinha.db :as db]
            [rinha.redis :as redis]
            [rinha.logic :as logic]
            [rinha.strategy :as strategy]
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

(defn ^:private send-payment-to-processor!
  "Sends payment to a specific processor - only does HTTP POST"
  [processor-url correlation-id amount requested-at]
  (let [url (str processor-url "/payments")
        payload {:correlationId correlation-id
                 :amount amount
                 :requestedAt requested-at}]
    (try
      (let [response @(http/post url
                                 {:headers {"Content-Type" "application/json"}
                                  :body (m/encode m/instance "application/json" payload)
                                  :timeout 200})]
        {:status (:status response)
         :body (:body response)})
      (catch Exception e
        (println "HTTP request failed:" (.getMessage e))
        {:status nil :error (.getMessage e)}))))

(defn ^:private save-payment-to-db!
  "Saves payment to database"
  [correlation-id amount requested-at processor]
  (try
    (db/execute!
     "INSERT INTO payments (correlation_id, amount, requested_at, processor) VALUES (?::uuid, ?, ?::timestamp, ?)"
     correlation-id amount requested-at (name processor))
    true
    (catch Exception e
      (println "Database save failed:" (.getMessage e))
      false)))

(defn ^:private process-payment-with-routing!
  "Processes payment with smart routing based on business rules"
  [correlation-id amount requested-at]
  (let [default-health (redis/get-processor-health :default)
        fallback-health (redis/get-processor-health :fallback)
        best-choice (logic/get-best-processor default-health
                                              fallback-health
                                              payment-processor-default-url
                                              payment-processor-fallback-url)
        
        ;; Try primary processor
        primary-response (send-payment-to-processor! (:url best-choice)
                                                     correlation-id
                                                     amount
                                                     requested-at)
        {:keys [status error processor]} (logic/parse-payment-response primary-response (:processor best-choice))]
    
    (cond
      ;; Success or already exists - save to DB if success
      (or (= status 200) (= status 422))
      (do
        (when (= status 200)
          (save-payment-to-db! correlation-id amount requested-at processor))
        {:status status :processor processor :error error})
      
      ;; Status 500 - Processor failed, check both processors and choose by minResponseTime
      (logic/should-check-both-processors-after-500? status)
      (do
        (strategy/check-both-processors! payment-processor-default-url payment-processor-fallback-url)
        (let [updated-default-health (redis/get-processor-health :default)
              updated-fallback-health (redis/get-processor-health :fallback)
              best-choice-after-check (logic/choose-processor-by-min-response-time 
                                       updated-default-health 
                                       updated-fallback-health
                                       payment-processor-default-url
                                       payment-processor-fallback-url)
              retry-response (send-payment-to-processor! (:url best-choice-after-check)
                                                         correlation-id
                                                         amount
                                                         requested-at)
              {:keys [status error processor]} (logic/parse-payment-response retry-response (:processor best-choice-after-check))]
          (when (= status 200)
            (save-payment-to-db! correlation-id amount requested-at processor))
          {:status status :processor processor :error error}))
      
      ;; Status nil - Timeout, try fallback
      (logic/should-try-fallback-after-timeout? status)
      (let [fallback-choice (logic/get-fallback-processor (:processor best-choice)
                                                          payment-processor-default-url
                                                          payment-processor-fallback-url)
            fallback-response (send-payment-to-processor! (:url fallback-choice)
                                                          correlation-id
                                                          amount
                                                          requested-at)
            {:keys [status error processor]} (logic/parse-payment-response fallback-response (:processor fallback-choice))]
        (cond
          ;; Fallback succeeded
          (or (= status 200) (= status 422))
          (do
            (when (= status 200)
              (save-payment-to-db! correlation-id amount requested-at processor))
            {:status status :processor processor :error error})
          
          ;; Fallback also timed out - check both processors and choose by minResponseTime 
          :else
          (do
            (strategy/check-both-processors! payment-processor-default-url payment-processor-fallback-url)
            (let [updated-default-health (redis/get-processor-health :default)
                  updated-fallback-health (redis/get-processor-health :fallback)
                  best-choice-after-check (logic/choose-processor-by-min-response-time 
                                           updated-default-health 
                                           updated-fallback-health
                                           payment-processor-default-url
                                           payment-processor-fallback-url)
                  retry-response (send-payment-to-processor! (:url best-choice-after-check)
                                                             correlation-id
                                                             amount
                                                             requested-at)
                  {:keys [status error processor]} (logic/parse-payment-response retry-response (:processor best-choice-after-check))]
              (when (= status 200)
                (save-payment-to-db! correlation-id amount requested-at processor))
              {:status status :processor processor :error error}))))
      
      ;; Other errors
      :else
      {:status status :processor processor :error error})))

(defn ^:private process-payment-async!
  "Processes a payment asynchronously"
  [correlation-id amount requested-at result-chan]
  (async/go
    (try
      (let [result (process-payment-with-routing! correlation-id amount requested-at)]
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
