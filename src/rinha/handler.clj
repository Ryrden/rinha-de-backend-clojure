(ns rinha.handler
  (:require [rinha.services :as services]
            [rinha.logic :as logic]))

(defn hello-world
  "Returns a hello world message"
  [_request]
  {:status 200
   :body (services/get-hello-world!)})

(defn create-payment
  "Handles payment creation requests"
  [{:keys [body-params]}]
  (let [{:keys [correlationId amount]} body-params
        process-payment (delay (services/process-payment! correlationId amount))
        result (if (logic/valid-payment-data? {:correlationId correlationId :amount amount})
                 @process-payment
                 {:status 400 :error "Invalid payment data"})]
    (cond
      (= (:status result) 400)
      {:status 400
       :body {:error "Invalid payment data"
              :message "correlationId must be a valid UUID and amount must be a positive number"}}
      
      (= (:status result) 200)
      {:status 202
       :body {:message "Payment processed successfully"
              :processor (name (:processor result))}}
      
      :else
      {:status (:status result)
       :body {:error (:error result)}})))

(defn payments-summary
  "Handles payments summary requests"
  [{:keys [query-params]}]
  (let [from (get query-params "from")
        to (get query-params "to")]
    {:status 200
     :body (services/get-payments-summary from to)}))
