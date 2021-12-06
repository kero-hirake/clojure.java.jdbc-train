(ns jdbc-practice.core
  (:require [clojure.java.jdbc :as jdbc])
  (:gen-class))

;;
;; overview
;;
; 接続用マップ
(def db-spec {:dbtype "postgresql"
            :dbname "jdbc_practice"
            :host "localhost"
            :user "postgres"
            :password "password"})
; 接続文字列
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
; db-do-commandsを使用して、単一のトランザクションでテーブルとインデックスを作成
(jdbc/db-do-commands db-spec
                     [fruit-table-ddl
                      "CREATE INDEX name_ix ON fruit(name);"])

;; Querying the database
; テーブル名は、文字列またはキーワードとして指定可能
; create
(jdbc/insert! db-spec :fruit {:name "apple" :appearance "red" :cost 100 :grade 1})
(jdbc/insert! db-spec :fruit {:name "grape" :appearance "purple" :cost 120 :grade 2.3})
; 列名のベクトルを取得し、その後に列値のベクトルを使用して、それぞれの列に挿入することも可能

; read
(jdbc/query db-spec ["SELECT * FROM fruit"])
(jdbc/query db-spec ["SELECT * FROM fruit WHERE name=?" "apple"])

; update
(jdbc/update! db-spec :fruit {:appearance "green"} ["name=?" "apple"])
; delete
(jdbc/delete! db-spec :fruit ["name=?" "grape"])


;; Dropping our tables
(def drop-fruit-table-ddl (jdbc/drop-table-ddl :fruit))
;作成とは逆の順序でテーブルとインデックスを破棄
(jdbc/db-do-commands db-spec 
                     ["DROP INDEX name_ix;"
                      drop-fruit-table-ddl])

;;
;; Manipulating data with SQL
;;
; Reading rows
; 通常は、列名と値のマップが返ってくる
(jdbc/query db-spec ["SELECT * FROM fruit"])
(jdbc/query db-spec ["SELECT * FROM fruit WHERE cost < ?" 50])
;=> ({:id 1 :name "Apple" :appearance "red" :cost 59 :grade 87} ...

; :as-arrays?オプションを使うと、1行目に列名のベクター、2行目以降に値のベクター
(jdbc/query db-spec ["SELECT * FROM fruit WHERE cost < ?" 100] {:as-arrays? true})
;; ([:id :name :appearance :cost :grade]
;;  [2 "Banana" "yellow" 29 92.2] ....


;; Processing a result set lazily
; queryは全データが返ってくるので、非常に大きな結果を処理するのは難しい場合がある
; しかし、java.jdbcは、接続が開いているときに遅延処理する機能がある
;result-set-fnオプションを介して関数を渡すか、
;リリース0.7.0以降、reducible-queryを介して関数を渡す。

;; query and :result-set-fn
; 関数は、結果セットがまだ処理されている間に接続が閉じられないように、
; 結果の実現を強制する必要があります。
(jdbc/query db-spec ["SELECT * FROM fruit WHERE cost < ?" 100]
            {:result-set-fn (fn [rs]
                              (reduce (fn [total row-map]
                                        (+ total (:cost row-map)))
                                      0 rs))})
;単純な場合は、クエリで計算しても良い
(jdbc/query db-spec ["SELECT sum(cost) FROM fruit WHERE cost < ?" 100]
            {:result-set-fn first})

;; reducible-query
; 0.7.0以降に推奨される書き方だが、いくつかの制限があり
; queryはas-arrays?, :explain, :explain-fn, :result-set-fn, :row-fn が使えない
; 一方、raw? true にするとJavaのResultSetからClojureのハッシュマップに変換されず、処理を高速にできる
; この場合、keys, vals, seq は使えない

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
;:row-fnオプションを使用して各行を処理できる
(jdbc/query db-spec ["SELECT * FROM fruit WHERE cost < ?" 100]
            {:row-fn :name})
;=> ("Apple" "Banana" ...)
; :result-set-fnと組み合わせて、結果セットの処理を簡素化できます
(jdbc/query db-spec ["SELECT * FROM fruit WHERE cost < ?" 100]
            {:row-fn :cost
             :result-set-fn (partial reduce +)})
; ↑↓ 同じ結果
(jdbc/query db-spec ["SELECT SUM(cost) AS total FROM fruit WHERE cost < ?" 100]
            {:row-fn :total
             :result-set-fn first})

; 行を操作して計算列を追加する例
(defn add-tax [row] (assoc row :tax (* 0.08 (:cost row))))
(jdbc/query db-spec ["SELECT * FROM fruit"]
            {:row-fn add-tax})

(into [] (map :name) (jdbc/reducible-query db-spec ["SELECT name FROM fruit WHERE cost < ?" 100]))
(transduce (map :cost) + (jdbc/reducible-query db-spec ["SELECT * FROM fruit WHERE cost < ?" 100]))
(into [] (map add-tax) (jdbc/reducible-query db-spec ["SELECT * FROM fruit"]))


