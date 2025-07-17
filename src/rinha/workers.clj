(ns rinha.workers
  (:require [clojure.core.async :as async]
            [org.httpkit.client :as http]
            [rinha.db :as db]
            [rinha.queue :as queue]
            [muuntaja.core :as m])
  (:import [java.time Instant]))

(def ^:private payment-processor-default-url (System/getenv "PROCESSOR_DEFAULT_URL"))
(def ^:private payment-processor-fallback-url (System/getenv "PROCESSOR_FALLBACK_URL"))
(def ^:private worker-timeout 5) ; seconds to wait for messages

(defn ^:private save-payment-to-db!
  "Saves payment to database"
  [correlation-id amount requested-at processor]
  (try
    (db/execute!
     "INSERT INTO payments (correlation_id, amount, requested_at, processor) VALUES (?::uuid, ?, ?::timestamp, ?)"
     correlation-id amount requested-at (name processor))
    (println "Payment saved to database:" correlation-id amount requested-at (name processor))
    (catch Exception e
      (println "Database save failed:" (.getMessage e)))))

(defn ^:private send-payment-to-processor!
  "Sends payment to a specific processor"
  [processor-url processor correlation-id amount timeout]
  (let [url (str processor-url "/payments")
        payload {:correlationId correlation-id
                 :amount amount}]
    (try
      (let [{:keys [status]} @(http/post url
                                         {:headers {"Content-Type" "application/json"}
                                          :body (m/encode m/instance "application/json" payload)
                                          :timeout timeout})]
        (condp = status
          200 (do
                (println "Payment processed by" processor "with status" status)
                (save-payment-to-db! correlation-id amount (str (Instant/now)) processor)
                {:status 200 :message "Payment processed"})
          422 {:status 422 :message "Payment already exists"}
          500 {:status 500 :message "Processor failed"}
          nil {:status nil :message "Request timeout"}
          {:status status :message (str "HTTP error: " status)}))
      (catch Exception e
        (println "HTTP request failed:" (.getMessage e))
        {:status 500 :message (.getMessage e)}))))

(defn ^:private process-payment-message!
  "Processes a single payment message from the queue"
  [message]
  (let [{:keys [correlation-id amount]} message
        {:keys [status message]} (send-payment-to-processor! payment-processor-default-url
                                                             :default
                                                             correlation-id
                                                             amount
                                                             150)]
    (when (or (= status 500) (nil? status))
      (send-payment-to-processor! payment-processor-fallback-url
                                  :fallback
                                  correlation-id
                                  amount
                                  150))
    {:status status :message message}))

(defn ^:private worker-loop!
  "Main worker loop that processes messages from the queue"
  [worker-id stop-chan]
  (println "Worker" worker-id "started")
  (async/go
    (loop []
      (let [dequeue-chan (async/thread (queue/dequeue-payment! worker-timeout))]
        (async/alt!
          stop-chan
          (do
            (println "Worker" worker-id "stopping")
            :stopped)

          dequeue-chan
          ([message]
           (when message
             (async/thread
               (let [result (process-payment-message! message)]
                 (println "Worker" worker-id "processed payment:" result))))
           (recur)))))))

(defn start-worker!
  "Starts a payment processing worker"
  [worker-id]
  (let [stop-chan (async/chan)
        worker-go-chan (worker-loop! worker-id stop-chan)]
    {:worker-id worker-id
     :stop-chan stop-chan
     :worker-go-chan worker-go-chan
     :started-at (System/currentTimeMillis)}))

(defn start-workers!
  "Starts multiple payment processing workers"
  [num-workers]
  (println "Starting" num-workers "payment processing workers")
  (doall
   (for [i (range num-workers)]
     (start-worker! (str "payment-worker-" i)))))