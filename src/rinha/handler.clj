(ns rinha.handler
  (:require [rinha.services :as services]))

(defn hello-world
  "Returns a hello world message"
  [_request]
  {:status 200
   :body (services/get-hello-world!)})

(defn create-payment
  "Handles payment creation requests"
  [{:keys [body-params]}]
  (let [{:keys [correlationId amount]} body-params
        result (services/process-payment! correlationId amount)]
    (cond
      (and (not (:success result)) (= (:error result) "Invalid payment data"))
      {:status 400
       :body {:error "Invalid payment data"
              :message "correlationId must be a valid UUID and amount must be a positive number"}}

      (:success result)
      {:status 202
       :body {:message "Payment processed successfully"
              :processor (name (:processor result))}}

      :else
      {:status 500
       :body {:error (:error result)}})))

(defn payments-summary
  "Handles payments summary requests"
  [{:keys [query-params]}]
  (let [from (get query-params "from")
        to (get query-params "to")]
    {:status 200
     :body (services/get-payments-summary from to)}))
