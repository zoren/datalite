(ns db
  (:require
   [honeysql.core :as honey]
   [next.jdbc :as jdbc]
   [next.jdbc.sql :as sql]
   [next.jdbc.result-set :as result-set]
   ))

(def fact-columns [:entity :attribute :value :transaction_id])

(def char->column (into {} (map (fn [col] [(-> col name first) col]) fact-columns)))

(defn index-columns [index] (map char->column (name index)))

(def create-facts-table "
CREATE TABLE IF NOT EXISTS facts (
  entity,
  attribute,
  value,
  transaction_id INTEGER NOT NULL)")

(defn index-map [index order]
  (let [columns (index-columns index)]
    {:from [:facts]
     :order-by (map (fn [col] [col order]) columns)
     :select fact-columns}))

(defn get-index-order [ds index values order]
  (let [columns (index-columns index)]
    (sql/find-by-keys
     ds
     :facts
     (into {} (map vector columns values))
     {:order-by (mapv (fn [col] [col order]) columns)
      :columns (if (= (count columns) (count values))
                 [:entity]
                 (drop (count values) columns))
      :builder-fn result-set/as-arrays})))

(defn insert-facts [ds facts]
  (jdbc/with-transaction [tx ds]
    (let [[next-transaction-id]
          (jdbc/execute-one!
           tx
           ["SELECT IFNULL(max(transaction_id), 0) + 1 FROM facts"]
           {:builder-fn result-set/as-arrays})]
      (sql/insert-multi!
       tx :facts
       fact-columns
       (map
        #(into % [next-transaction-id])
        facts)))))

(comment
  (def conn (jdbc/get-connection "jdbc:sqlite::memory:"))
  (jdbc/execute! conn [create-facts-table])

  (jdbc/execute-one!
   conn
   ["SELECT IFNULL(max(transaction_id), 0) + 1 FROM facts"]
   {:builder-fn result-set/as-arrays})
  (jdbc/execute-one!
   conn
   (honey/format {:from [:facts]
                  :select [(honey/call :now) ["g" [:+ 3 4]]]})
   )

  (insert-facts conn [])

  (get-index-order conn :eavt [1] :asc)
  (get-index-order conn :avet ["s" "str"] :asc)
  (get-index-order conn :avet ["s" "str" 1] :asc)

  (insert-facts
   conn
   [[1 "x" 5]
    [1 "y" 6]])
  (insert-facts
   conn
   [[1 "s" "str"]])

  (rest (jdbc/execute! conn ["select * from facts"] {:builder-fn result-set/as-arrays}))

  )
