(defproject rinha-clojure "0.1.0-SNAPSHOT"
  :description "Rinha de Backend 2025 - Clojure" 
  
  :dependencies [[org.clojure/clojure "1.12.1"]
                 [org.clojure/core.async "1.6.681"]
                 [metosin/reitit "0.9.1"]
                 [http-kit/http-kit "2.8.0"]
                 [metosin/muuntaja "0.6.8"]
                 [com.taoensso/carmine "3.3.2"]]
  
  :main rinha.core
  :aot [rinha.core]
  
  :target-path "target/%s"
  
  :profiles {:uberjar {:aot :all}}) 