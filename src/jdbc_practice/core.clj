(ns jdbc-practice.core
  (:require [clojure.java.jdbc :as jdbc])
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

(def db-spec {:dbtype "postgresql"
            :dbname "jdbc_practice"
            :host "localhost"
            :user "postgres"
            :password "password"})

(def db-uri
  {:connection-uri (str "postgresql://postgres:password@localhost:5432/jdbc_practice")})

;A "Hello World" Query
(jdbc/query db-spec ["SELECT 3 * 5 AS result"])


;Creating tables
 (def fruit-table-ddl
  (jdbc/create-table-ddl :fruit
                         [[:name "varchar(32)"]
                          [:appearance "varchar(32)"]
                          [:cost :int]
                          [:grade :real]]))
(jdbc/db-do-commands db-spec
                     [fruit-table-ddl
                      "CREATE INDEX name_ix ON fruit(name);"])

;Querying the database
(jdbc/insert! db-spec :fruit {:name "apple" :appearance "red" :cost 100 :grade 1})
(jdbc/insert! db-spec :fruit {:name "grape" :appearance "purple" :cost 120 :grade 2.3})
(jdbc/query db-spec ["SELECT * FROM fruit"])
(jdbc/query db-spec ["SELECT * FROM fruit WHERE name=?" "apple"])
(jdbc/update! db-spec :fruit {:appearance "green"} ["name=?" "apple"])
(jdbc/delete! db-spec :fruit ["name=?" "grape"])
(jdbc/insert-multi! db-spec :fruit 
                    [{:name "orange" :appearance "round" :cost 90 :grade 4}
                     {:name "cherry" :appearance "small" :cost 50 :grade 0}])
(jdbc/insert-multi! db-spec :fruit 
                    [:name :appearance :cost :grade]
                    [["mellon" "mesh" 1000 5]
                     ["banana" "yellow" 30 1]])
;Dropping our tables
(def drop-fruit-table-ddl (jdbc/drop-table-ddl :fruit))

(jdbc/db-do-commands db-spec 
                     ["DROP INDEX name_ix;"
                      drop-fruit-table-ddl])

