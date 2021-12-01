(ns jdbc-practice.core
  (:require [clojure.java.jdbc :as jdbc])
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))


;;
;; overview
;;
(def db-spec {:dbtype "postgresql"
            :dbname "jdbc_practice"
            :host "localhost"
            :user "postgres"
            :password "password"})

(def db-uri
  {:connection-uri (str "postgresql://postgres:password@localhost:5432/jdbc_practice")})

;; A "Hello World" Query
(jdbc/query db-spec ["SELECT 3 * 5 AS result"])


;; Creating tables
(def fruit-table-ddl
  (jdbc/create-table-ddl :fruit
                         [[:name "varchar(32)"]
                          [:appearance "varchar(32)"]
                          [:cost :int]
                          [:grade :real]]))
(jdbc/db-do-commands db-spec
                     [fruit-table-ddl
                      "CREATE INDEX name_ix ON fruit(name);"])

;; Querying the database
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
;; Dropping our tables
(def drop-fruit-table-ddl (jdbc/drop-table-ddl :fruit))

;作成とは逆の順序でテーブルとインデックスを破棄
(jdbc/db-do-commands db-spec 
                     ["DROP INDEX name_ix;"
                      drop-fruit-table-ddl])

;;
;; Manipulating data with SQL
;;
(jdbc/query db-spec ["SELECT * FROM fruit"])
(jdbc/query db-spec ["SELECT * FROM fruit WHERE cost < ?" 50])
(jdbc/query db-spec ["SELECT * FROM fruit WHERE cost < ?" 100] {:as-arrays? true})

;; Processing a result set lazily
;; query and :result-set-fn
(jdbc/query db-spec ["SELECT * FROM fruit WHERE cost < ?" 100]
            {:result-set-fn (fn [rs]
                              (reduce (fn [total row-map]
                                        (+ total (:cost row-map)))
                                      0 rs))})
(jdbc/query db-spec ["SELECT sum(cost) FROM fruit WHERE cost < ?" 100]
            {:result-set-fn first})
(jdbc/query db-spec ["SELECT sum(cost) FROM fruit WHERE cost < ?" 100])

;; reducible-query
; You cannot use any of the following options that query 
; accepts: as-arrays?, :explain, :explain-fn, :result-set-fn, or :row-fn.
; you can specify :raw? true and 
; no conversion from Java's ResultSet to Clojure's sequence of hash maps will be performed.
(reduce (fn [total {:keys [cost]}] (+ total cost))
        0
        (jdbc/reducible-query db-spec
                              ["SELECT * FROM fruit WHERE cost < ?" 100]
                              {:raw? true}))
(transduce (map :cost)
           +
           (jdbc/reducible-query db-spec
                                 ["SELECT * FROM fruit WHERE cost < ?" 100]
                                 {:raw? true}))
;; Processing each row lazily
(jdbc/query db-spec ["SELECT * FROM fruit WHERE cost < ?" 100]
            {:row-fn :name})
(jdbc/query db-spec ["SELECT * FROM fruit WHERE cost < ?" 100]
            {:row-fn :cost
             :result-set-fn (partial reduce +)})
(jdbc/query db-spec ["SELECT SUM(cost) AS total FROM fruit WHERE cost < ?" 100]
            {:row-fn :total
             :result-set-fn first})

(defn add-tax [row] (assoc row :tax (* 0.08 (:cost row))))
(jdbc/query db-spec ["SELECT * FROM fruit"]
            {:row-fn add-tax})

(into [] (map :name) (jdbc/reducible-query db-spec ["SELECT name FROM fruit WHERE cost < ?" 100]))
(transduce (map :cost) + (jdbc/reducible-query db-spec ["SELECT * FROM fruit WHERE cost < ?" 100]))
(into [] (map add-tax) (jdbc/reducible-query db-spec ["SELECT * FROM fruit"]))
