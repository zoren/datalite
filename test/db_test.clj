(ns db-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [db :refer [connect make-db insert-facts filter-index]]
   [next.jdbc :as jdbc]
   ))

(defn make-mem-sqlite-db [] (jdbc/get-connection "jdbc:sqlite::memory:"))

(deftest db-test
  (let [connection (connect (make-mem-sqlite-db))
        db (make-db connection)]
    (is (= 0 (count (filter-index db :eavt []))))
    (insert-facts connection [[1 "s" "str"]])
    (is (= 0 (count (filter-index db :eavt []))))
    (testing "isolation"
      (let [new-db (make-db connection)]
        (is (= 1 (count (filter-index new-db :eavt []))))))
    (insert-facts connection [[1 "x" 5]
                              [1 "y" 6]])
    (is (= [[5 2]] (filter-index (make-db connection) :eavt [1 "x"])))))
