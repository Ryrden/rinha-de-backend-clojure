(ns rinha.db
  (:require [next.jdbc :as jdbc])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defn create-hikari-config
  "Creates HikariCP configuration with optimized settings
  
  References for optimization settings used:
  - PostgreSQL JDBC optimizations from: https://medium.com/@ahmettemelkundupoglu/optimizing-database-connections-with-hikaricp-in-spring-boot-3-and-java-21-80fab58cc1c7
  - Connection pool sizing guidance from: https://medium.com/@nikita.ojamae_67727/exploring-the-hikari-connection-pool-configuration-aec0eb64d26b"
  [{:keys [jdbc-url username password
           minimum-idle maximum-pool-size
           connection-timeout idle-timeout
           max-lifetime leak-detection-threshold
           pool-name]}]
  (doto (HikariConfig.)
    (.setJdbcUrl jdbc-url)
    (.setUsername username)
    (.setPassword password)
    (.setPoolName (or pool-name "rinha-pool"))
    (.setMinimumIdle (or minimum-idle 2))
    (.setMaximumPoolSize (or maximum-pool-size 10))
    (.setConnectionTimeout (or connection-timeout 30000))
    (.setIdleTimeout (or idle-timeout 600000))
    (.setMaxLifetime (or max-lifetime 1800000))
    (.setLeakDetectionThreshold (or leak-detection-threshold 60000))
    (.setConnectionTestQuery "SELECT 1")
    (.setAutoCommit true)
    (.addDataSourceProperty "cachePrepStmts" "true")
    (.addDataSourceProperty "prepStmtCacheSize" "250")
    (.addDataSourceProperty "prepStmtCacheSqlLimit" "2048")
    (.addDataSourceProperty "useServerPrepStmts" "true")
    (.addDataSourceProperty "useLocalSessionState" "true")
    (.addDataSourceProperty "rewriteBatchedStatements" "true")
    (.addDataSourceProperty "cacheResultSetMetadata" "true")
    (.addDataSourceProperty "cacheServerConfiguration" "true")
    (.addDataSourceProperty "elideSetAutoCommits" "true")
    (.addDataSourceProperty "maintainTimeStats" "false")))

(defn create-datasource
  "Creates HikariDataSource from configuration"
  [config]
  (HikariDataSource. (create-hikari-config config)))

(defn get-db-config
  "Gets database configuration from environment variables with optimized defaults"
  []
  {:jdbc-url (System/getenv "DATABASE_URL")
   :username (System/getenv "DATABASE_USER")
   :password (System/getenv "DATABASE_PASSWORD")
   :pool-name "rinha-hikari-pool"
   :minimum-idle 2
   :maximum-pool-size 10
   :connection-timeout 30000
   :idle-timeout 600000
   :max-lifetime 1800000
   :leak-detection-threshold 60000})

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