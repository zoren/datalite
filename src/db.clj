(ns db
  (:require
   [honeysql.core :as honey]
   [next.jdbc :as jdbc]
   [next.jdbc.sql :as sql]
   [next.jdbc.result-set :as result-set]
   ))

(def fact-columns [:entity :attribute :value :transaction_id])

(defn connect [connection]
  (jdbc/execute!
   connection
   ["
CREATE TABLE IF NOT EXISTS facts (
  entity,
  attribute,
  value,
  transaction_id INTEGER NOT NULL)"])
  connection)

(defn- next-transaction-id [db-spec]
  (first
   (jdbc/execute-one!
    db-spec
    ["SELECT IFNULL(MAX(transaction_id), 0) + 1 FROM facts"]
    {:builder-fn result-set/as-arrays})))

(defn make-db [connection]
  {:connection connection :transaction-id (next-transaction-id connection)})

(defn insert-facts [ds facts]
  (jdbc/with-transaction [tx ds]
    (let [transaction-id (next-transaction-id ds)]
      (sql/insert-multi!
       tx :facts
       fact-columns
       (map
        #(into % [transaction-id])
        facts)))))

(def char->column (into {} (map (fn [col] [(-> col name first) col]) fact-columns)))

(defn- index-map [{:keys [index order]} {:keys [transaction-id values]}]
  (let [columns (map char->column (name index))]
    {:from [:facts]
     :order-by (map (fn [col] [col order]) columns)
     :where (into [:and [:< :transaction_id transaction-id]]
                  (map (fn [col val] [:= col val]) columns values))
     :select (if (= (count columns) (count values))
               [:entity]
               (drop (count values) columns))}))

(defn filter-index [{:keys [transaction-id connection]} index values]
  (rest
   (jdbc/execute!
    connection
    (honey/format
     (index-map {:index index :order :asc} {:transaction-id transaction-id :values values}))
    {:builder-fn result-set/as-unqualified-arrays})))
