(ns db-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [db :refer [connect make-db transact filter-index get-raw-facts-table max-transaction-id]]
   [next.jdbc :refer [get-connection]]
   [clojure.pprint]
   ))

(defn make-sqlite-db [name] (get-connection (str "jdbc:sqlite:" name)))

(defn make-mem-sqlite-db [] (make-sqlite-db ":memory:"))

(deftest db-test
  (let [connection (connect (make-mem-sqlite-db))]
    (let [db (make-db connection)]
      (is (= 0 (count (filter-index db :eavt []))))

      (transact connection [[1 "s" "str"]] [])

      (is (= 0 (count (filter-index db :eavt [])))))

    (testing "isolation"
      (let [new-db (make-db connection)]
        (is (= 1 (count (filter-index new-db :eavt []))))))

    (transact connection [[1 "x" 5]
                          [1 "y" 6]] [])

    (let [new-db (make-db connection)]
      (is (= 1 (count (filter-index new-db :eavt [1 "x"])))))

    (transact connection [] [2])

    (let [new-db (make-db connection)]
      (is (= 0 (count (filter-index new-db :eavt [1 "x"])))))))

(deftest retract-test
  (let [connection (connect (make-mem-sqlite-db))]
    (transact connection [[1 "s" "str"]] [])
    (testing "can retract same fact again"
      (transact connection [] [1])
      (transact connection [] [1]))

    (is (= 0 (count (filter-index (make-db connection) :eavt []))))
    (testing "can add fact back in"
      (transact connection [[1 "s" "str"]] [])
      (is (= 1 (count (filter-index (make-db connection) :eavt []))))))
  (testing "if we guess id's we cannot delete facts as we're adding them"
    (let [connection (connect (make-mem-sqlite-db))]
      (transact connection [[1 "s" "str"]] [1])
      (is (= 1 (count (filter-index (make-db connection) :eavt [])))))))

(deftest set-test
  (let [connection (connect (make-mem-sqlite-db))]
    (transact connection [[1 "set" 1] [1 "set" 2] [1 "set" 3]] [])

    (is (= 3 (count (filter-index (make-db connection) :eavt []))))

    (transact connection [[1 "set" 4]] [])

    (is (= 4 (count (filter-index (make-db connection) :eavt []))))

    (transact connection [] [2 4])

    (is (= #{1 3} (into #{} (map first (filter-index (make-db connection) :eavt [1 "set"])))))))

(defn histogram [values] (reduce (fn [acc v] (update acc v (fnil inc 0))) {} values))

(deftest multiset-test
  (let [connection (connect (make-mem-sqlite-db))]
    (transact connection [[1 "set" 1] [1 "set" 1] [1 "set" 34]] [])
    (is (= 2 (count (filter-index (make-db connection) :eavt [1 "set" 1]))))
    (is (= 1 (count (filter-index (make-db connection) :eavt [1 "set" 34]))))
    (transact connection [[1 "set" 1] [1 "set" 34] [1 "set" 34] [1 "set" 5]] [])
    (is (= {1 3, 5 1, 34 3} (histogram (map first (filter-index (make-db connection) :eavt [1 "set"])))))))

(defn print-fact-table [connection]
  (clojure.pprint/print-table (get-raw-facts-table connection)))

(comment
  (def connection (connect (make-mem-sqlite-db)))
  (max-transaction-id connection)
  (print-fact-table connection)
  ;;(def connection (connect (jdbc/get-connection "jdbc:sqlite:test.db")))
  (transact connection [[1 "set" 1] [1 "set" 2] [1 "set" 3]] [])
  (transact connection [[10 "s" "str"]] [])
  (def db (make-db connection))
  (transact connection [[10 "x" 54]
                        [10 "y" 36]] [])
  (transact connection [] [1 3])
  (transact connection [] [2])

  (filter-index (make-db connection) :eavt [])
  (filter-index (make-db connection) :eavt [1 "set"])
  (filter-index db :eavt [])
  (filter-index {:jdbc-connection connection
                 :transaction-id 1} :eavt [])
  )
