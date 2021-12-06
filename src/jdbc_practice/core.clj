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


;;Inserting data
;複雑なinsertが必要な場合は、execute！を使用できます。
;また、DBがサポートするなら、生成されたキーを取得するためのオプション：return-keysを使用可能

;Inserting a row
(jdbc/insert! db-spec :fruit {:name "Pear" :appearance "green" :cost 99})

;Inserting multiple rows
;複数の行を挿入するには、マップのシーケンスとして、;またはベクトルのシーケンスとして2つの方法があります。
;前者の場合、複数の挿入が実行され、生成されたキーのマップが挿入ごとに（シーケンスとして）返されます。
;後者の場合、単一のバッチ挿入が実行され、一連の行挿入カウントが返されます（通常は一連の挿入）。
;後者は、多数の行を挿入する場合に大幅に高速になる可能性があります。
(jdbc/insert-multi! db-spec :fruit
                    [{:name "Pomegranate" :appearance "fresh" :cost 585}
                     {:name "Kiwifruit" :grade 93}])
(jdbc/insert-multi! db-spec :fruit
                    [{:name "orange" :appearance "round" :cost 90 :grade 4}
                     {:name "cherry" :appearance "small" :cost 50 :grade 0}])
; 挿入する列を指定し、その後に各行を列値のベクトルとして指定
(jdbc/insert-multi! db-spec :fruit
                    [:name :appearance :cost :grade]
                    [["mellon" "mesh" 1000 5]
                     ["banana" "yellow" 30 1]])
;完全な行を挿入する場合は、列名ベクトルを省略可能だが、指定した方が安全
(jdbc/insert-multi! db-spec :fruit
                    nil ; column names not supplied
                    [[1 "Apple" "red" 59 87]
                     [2 "Banana" "yellow" 29 92.2]
                     [3 "Peach" "fuzzy" 139 90.0]
                     [4 "Orange" "juicy" 89 88.6]])

;Updating rows
(jdbc/update! db-spec :fruit
              {:cost 49}
              ["grade < ?" 75])
(jdbc/execute! db-spec
               ["UPDATE fruit SET cost = (2 * grade) WHERE grade > ?" 50.0])

;Deleting rows
(jdbc/delete! db-spec :fruit ["grade < ?" 25.0])
(jdbc/execute! db-spec ["DELETE FROM fruite WHERE grade < ?" 25.0])

;Using transactions
(jdbc/with-db-transaction [t-con db-spec]
                          (jdbc/update! t-con :fruit
                                        {:cost 49}
                                        ["grade < ?" 50])
                          (jdbc/execute! t-con
                                         ["UPDATE fruit SET cost = (2 * grade) where grade > ?"50.0 ]))
;:isolationオプションで、トランザクション分離レベルを指定可能
;指定できる値は、:none、:read-committed、:read-uncommitted、:repeatable-read、:serializable
(jdbc/with-db-transaction [t-con db-spec {:isolation :serializable}]
  ...)
(jdbc/db-set-rollback-only! t-con)   ; コミットではなくロールバック
(jdbc/db-unset-rollback-only! t-con) ;成功した場合、このトランザクションはコミット
(jdbc/db-is-rollback-only t-con)     ; トランザクションがロールバックに設定されている場合はtrueを返す



