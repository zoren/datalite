(ns db
  (:require
   [honeysql.core :as honey]
   [next.jdbc :as jdbc]
   [next.jdbc.sql :as sql]
   [next.jdbc.result-set :as result-set]
   ))

(defn connect [jdbc-connection]
  (jdbc/execute-one!
   jdbc-connection
   ["
CREATE TABLE IF NOT EXISTS facts (
  entity,
  attribute,
  value,
  added_transaction_id INTEGER NOT NULL,
  retracted_transaction_id INTEGER,
  fact_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT  -- ensure monotonically increasing fact_id used as row id
)
"])
  jdbc-connection)

(defn max-transaction-ids [jdbc-connection]
  (jdbc/execute-one!
   jdbc-connection
   ["select max(added_transaction_id) as max_added_transaction_id,
            max(retracted_transaction_id) as max_retracted_transaction_id from facts"]
   {:builder-fn result-set/as-kebab-maps}))

(defn max-transaction-id [jdbc-connection]
  (let [{:keys [max-added-transaction-id max-retracted-transaction-id]}
        (max-transaction-ids jdbc-connection)]
    (apply max (filter identity [max-added-transaction-id max-retracted-transaction-id 0]))))

(defn make-db [jdbc-connection]
  {:jdbc-connection jdbc-connection :transaction-id (max-transaction-id jdbc-connection)})

(defn transact
  [jdbc-connection facts-to-add fact-ids-to-retract]
  (jdbc/with-transaction [jdbc-transaction jdbc-connection]
    (let [transaction-id (inc (max-transaction-id jdbc-transaction))]
      (sql/insert-multi!
       jdbc-transaction :facts
       [:entity :attribute :value :added_transaction_id]
       (map #(into % [transaction-id]) facts-to-add))
      (when-not (empty? fact-ids-to-retract)
        (jdbc/execute-one!
         jdbc-transaction
         (honey/format
          {:update :facts
           :set {:retracted_transaction_id transaction-id}
           :where [:and [:= :retracted_transaction_id nil] [:in :fact_id fact-ids-to-retract]]})))
      {:transaction-id transaction-id})))

(def char->column
  {\e :entity
   \a :attribute
   \v :value
   \t :added_transaction_id})

(defn- index-map [{:keys [index order]} {:keys [transaction-id values]}]
  (let [columns (map char->column (name index))]
    {:from [:facts]
     :order-by (map (fn [col] [col order]) columns)
     :where (into [:and [:<= :added_transaction_id transaction-id]
                   [:or [:= :retracted_transaction_id nil] [:< transaction-id :retracted_transaction_id]]]
                  (map (fn [col val] [:= col val]) columns values))
     :select (conj (into [] (drop (count values) columns)) :fact_id)}))

(defn filter-index [{:keys [jdbc-connection transaction-id]} index values]
  (rest
   (jdbc/execute!
    jdbc-connection
    (honey/format
     (index-map {:index index :order :asc} {:transaction-id transaction-id :values values}))
    {:builder-fn result-set/as-unqualified-arrays})))

(defn get-raw-facts-table [connection]
  (sql/find-by-keys connection :facts :all {:builder-fn result-set/as-unqualified-maps}))
