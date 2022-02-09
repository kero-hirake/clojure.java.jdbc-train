(ns jdbc-practice.core
  (:require [clojure.java.jdbc :as jdbc])
  (:import (com.mchange.v2.c3p0 ComboPooledDataSource)))

;----------------------------------------
;; overview
;----------------------------------------
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

;----------------------------------------
;; Manipulating data with SQL
;----------------------------------------
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

;; Updating rows
(jdbc/update! db-spec :fruit
              {:cost 49}
              ["grade < ?" 75])
(jdbc/execute! db-spec
               ["UPDATE fruit SET cost = (2 * grade) WHERE grade > ?" 50.0])

;; Deleting rows
(jdbc/delete! db-spec :fruit ["grade < ?" 25.0])
(jdbc/execute! db-spec ["DELETE FROM fruite WHERE grade < ?" 25.0])

;; Using transactions
(jdbc/with-db-transaction [t-con db-spec]
  (jdbc/update! t-con :fruit
                {:cost 49}
                ["grade < ?" 50])
  (jdbc/execute! t-con
                 ["UPDATE fruit SET cost = (2 * grade) where grade > ?" 50.0]))
;:isolationオプションで、トランザクション分離レベルを指定可能
;指定できる値は、:none、:read-committed、:read-uncommitted、:repeatable-read、:serializable
(jdbc/with-db-transaction [t-con db-spec {:isolation :serializable}]
  ...)
(jdbc/db-set-rollback-only! t-con)   ; コミットではなくロールバック
(jdbc/db-unset-rollback-only! t-con) ;成功した場合、このトランザクションはコミット
(jdbc/db-is-rollback-only t-con)     ; トランザクションがロールバックに設定されている場合はtrueを返す

;; Updating or Inserting rows conditionally
;java.jdbcには、「既存の行があれば更新、無ければ新しい行を挿入」という組み込み関数はありません。
(defn update-or-insert! [db table row where-clause]
  (jdbc/with-db-transaction [t-con db]
    (let [result (jdbc/update! t-con table row where-clause)]
      (if (zero? (first result))
        (jdbc/insert! t-con table row)
        result))))

(update-or-insert! db-spec :fruit
                   {:name "Cactus" :appearance "Spiky" :cost 2000}
                   ["name = ?" "Cactus"])

(update-or-insert! db-spec :fruit
                   {:name "Cactus" :appearance "Spiky" :cost 2500}
                   ["name = ?" "Cactus"])

(jdbc/query db-spec ["select * from fruit where name=?" "Pear"])
(jdbc/delete! db-spec :fruit ["name=?" "Apple"])

;; Exception Handling and Transaction Rollback
(jdbc/with-db-transaction [t-con db-spec]
  (jdbc/insert-multi! t-con :fruit
                      [:name :appearance]
                      [["Grape" "yummy"]
                       ["Pear" "bruised"]])
  ;insertは完了しても、例外でロールバックする
  (throw (Exception. "sql/test exception")))

(jdbc/with-db-transaction [t-con db-spec]
  (prn "is-rollback-only" (jdbc/db-is-rollback-only t-con))
  (jdbc/db-set-rollback-only! t-con)
  (jdbc/insert-multi! t-con :fruit
                      [:name :appearance]
                      [["Orange" "yummy"]
                       ["Pear" "bruised"]])
  (prn "is-rollback-only" (jdbc/db-is-rollback-only t-con))
  (jdbc/query t-con ["SELECT * FROM fruit"]
              {:row-fn println}))
(prn)
(jdbc/query db-spec ["SELECT * FROM fruit"]
            {:row-fn println})


;; Clojure identifiers and SQL entities
;java.jdbcは、結果セット内のSQLエンティティ名をClojureのキーワードに小文字に変換し、
;テーブル名と列名（マップ内）を指定する文字列とキーワードをデフォルトでそのままSQLエンティティに変換します。
;オプションマップを指定することで、この動作をオーバーライドできます。
; :identifiersは、ResultSet列名をキーワード（または文字列）に変換するためのものです。
;  デフォルトはclojure.string / lower-caseです。
; :keywordize？識別子をキーワード（デフォルト）に変換するかどうかを制御します。
; :qualifierは、オプションで、キーワードを修飾する名前空間を指定します（:keywordize？がfalseでない場合）。
; :entitiesは、Clojureのキーワード/文字列をSQLエンティティ名に変換するためのものです。
;  デフォルトはIdentityです（キーワードまたは文字列でnameを呼び出した後）。

;クエリ結果でjava.jdbcがSQLエンティティ名を小文字に変換しないようにする場合は、：identifiersidentityを指定できます。
(jdbc/query db-spec ["SELECT * FROM fruit"]
            {:identifiers identity})
;列名にアンダースコアが含まれているデータベースを使用している場合は、
;Clojureキーワードでそれらをダッシュ​​に変換する関数を指定することをお勧めします。
(jdbc/query db-spec ["SELECT * FROM fruit"]
            {:identifiers #(.replace  % \_ \-)})

; いくつかのデータベースの場合、エンティティを何らかの方法でクォートする必要があります。
; entitiesオプションとquoted関数を使う
(jdbc/insert! db-spec :fruit
              {:name "Apple" :appearance "Round" :cost 99}
              {:entities (jdbc/quoted \`)})
;INSERT INTO `fruit` ( `name`, `appearance`, `cost` )
;    VALUES ( ?, ?, ? )

(jdbc/insert! db-spec :fruit
              {:name "Apple" :appearance "Round" :cost 99}
              {:entities (jdbc/quoted [\[ \]])})
;INSERT INTO [fruit] ([name], [appearance], [cost])
;VALUES (?, ?, ?)

;----------------------------------------
;; How to reuse database connecitons
;----------------------------------------

; using with-db-connection
(jdbc/with-db-connection [db-con db-spec]
  (let [rows (jdbc/query db-con ["SELECT * FROM fruit WHERE cost = ?" 186])]
    (jdbc/insert! db-con :fruit (dissoc (first rows) :name))))
;↑query と insert! がそれぞれ独自のトランザクションで実行される

; using with-db-transaction
(jdbc/with-db-transaction [t-con db-spec]
  (let [rows (jdbc/query t-con ["SELECT * FROM fruit WHERE cost = ?" 186])]
    (jdbc/insert! t-con :fruit (dissoc (first rows) :name))))
;↑ ネストした場合は、外部のトランザクションがそのまま使用される

;; Using Connection Pooling
; java.jdbcは接続プールを直接提供しない
(defn pool [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec))
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               (.setMaxIdleTimeExcessConnections (* 30 60))
               (.setMaxIdleTime (* 3 60 60)))]
    {:datasource cpds}))

(def pooled-db (delay (pool db-spec)))
(defn db-connection [] @pooled-db)

;----------------------------------------
;; Using DDL and Metadata
;----------------------------------------
;コマンドは、トランザクションにラップされた単一のバッチステートメントとして実行されます。
;(jdbc/db-do-commands db-spec [sql]-command1 ....)
;トランザクションを回避したい場合↓
;(jdbc/db-do-commands db-spec false [sql]-command1 ....)

;; Creating tables
;[IF NOT EXISTS]を追加したいときは、conditional? をtureに
(jdbc/create-table-ddl :fruit
                       [[:lame "varchar(32)" :primaty :key]
                        [:appearance "varchar(32)"]
                        [:cost :int]
                        [:grade :real]
                        {:table-spec "ENGINE=InnoDB"
                         :entries clojure.string/upper-case}])
;=>CREATE TABLE FRUIT
;  (NAME varchar (32) primary key
;   APPEARANCE varchar (32)
;   COST int
;   GRADE real) ENGINE=InnoDB

;;Dropping Tabels
;↑と同じように condition? あり
(jdbc/drop-table-ddl :fruit)
(jdbc/drop-table-ddl :fruit {:entities clojure.string/upper-case}) ;drop table FRUIT

;; Accessing metadata
(clojure.pprint/pprint
 (jdbc/with-db-metadata [md db-spec]
   (jdbc/metadata-result (.getTables md nil nil nil (into-array ["TABLE" "VIEW"])))))
#_({:remarks nil
    :table_type "TABLE"
    :ref_generation ""
    :table_schem "public"
    :type_cat ""
    :table_name "fruit"
    :type_schem ""
    :self_referencing_col_name ""
    :type_name ""
    :table_cat nil})




