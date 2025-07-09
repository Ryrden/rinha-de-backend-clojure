(ns rinha.db
  (:require [next.jdbc :as jdbc]
            [next.jdbc.connection :as connection])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defn create-hikari-config
  "Creates HikariCP configuration"
  [{:keys [jdbc-url username password
           minimum-idle maximum-pool-size
           connection-timeout idle-timeout
           max-lifetime]}]
  (doto (HikariConfig.)
    (.setJdbcUrl jdbc-url)
    (.setUsername username)
    (.setPassword password)
    (.setMinimumIdle (or minimum-idle 2))
    (.setMaximumPoolSize (or maximum-pool-size 10))
    (.setConnectionTimeout (or connection-timeout 30000))
    (.setIdleTimeout (or idle-timeout 600000))
    (.setMaxLifetime (or max-lifetime 1800000))
    (.setAutoCommit true)))

(defn create-datasource
  "Creates HikariDataSource from configuration"
  [config]
  (HikariDataSource. (create-hikari-config config)))

(defn get-db-config
  "Gets database configuration from environment variables"
  []
  {:jdbc-url (or (System/getenv "DATABASE_URL") 
                 "jdbc:postgresql://localhost:5432/rinha_db")
   :username (or (System/getenv "DATABASE_USER") "rinha_user")
   :password (or (System/getenv "DATABASE_PASSWORD") "rinha_pass")
   :minimum-idle 2
   :maximum-pool-size 10
   :connection-timeout 30000
   :idle-timeout 600000
   :max-lifetime 1800000})

(defonce datasource 
  (delay (create-datasource (get-db-config))))

(defn get-connection
  "Gets database connection from pool"
  []
  @datasource)

(defn execute!
  "Executes a query with parameters"
  [query & params]
  (jdbc/execute! (get-connection) (vec (cons query params))))

(defn execute-one!
  "Executes a query expecting single result"
  [query & params]
  (jdbc/execute-one! (get-connection) (vec (cons query params))))

(defn close-datasource!
  "Closes the datasource connection pool"
  []
  (when (realized? datasource)
    (.close @datasource))) 