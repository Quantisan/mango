(ns mango.test.io-tests
  (:use [mango.io]
        [clojure.test]
        [somnium.congomongo]
        [incanter core mongodb])
  (:require [clj-time.core :as time]
            [clj-time.coerce :as time.coerce])) 

(def ds (dataset [:Date :a] [[(time/date-time 1986 10 14 4 3 27 456) 1.555]
                             [(time/date-time 1986 10 15 2 8 53 318) 2.130]
                             [(time/date-time 1986 10 16 13 4 3 563) 1.850]]))
(def date-string "02/01/04 19:01:27")

(def test-file "data/testing.csv")
(def test-db "coretests-db")
(def test-coll :testcoll)

(def ^{:private true} db (make-connection test-db))

(defn fill-db! 
" Populate database with test data."  
  [] 
  (->> ds
    (date->long)
    (insert-dataset test-coll)))

(defn teardown!
" Drops test database."
  [] 
  (drop-database! test-db))
 
(defmacro with-test-mongo [db & body]
  `(with-mongo db
     (do ~@body
       (teardown!))))

(deftest utils-fn
  (is (= '(529718400000 529804800000)
         (when-query-map {:from (time/date-time 1986 10 15)
                          :to (time/date-time 1986 10 16)}))))

(deftest push-pull-data
  (with-test-mongo db
    (is (push-ts test-coll ds))
    (is (= 2.13
           ($ :a (fetch-dataset test-coll :where {:Date {:$gte 529718400000 :$lt 529804800000}})))
        "query on epoch time")))

(deftest csv-mongo
  (with-test-mongo db
    (is (csv->mongo test-coll read-oanda-csv test-file))
    (is (= 1.2964 
           (first ($ :Bid (fetch-ts test-coll)))))))

(deftest fetch-time-series ; TODO expand with querying
  (with-test-mongo db
    (do (fill-db!)
      (is (= ds
             ($ (col-names ds) (fetch-ts test-coll))))
      (is (= 1.555
             (first ($ :a (fetch-ts test-coll)))))
      (is (= 2.13
             ($ :a (fetch-ts test-coll :when {:from (time/date-time 1986 10 15)
                                              :to (time/date-time 1986 10 16)})))))))

(deftest read-csv
  (is (= (time/date-time 2004 01 02 19 01 27) 
         (parse-time "dd/MM/YY HH:mm:ss" date-string))
      "parse-time")
  (is (= 1.2964 
         (first ($ :Bid (read-ts-csv "dd/MM/YY HH:mm:ss" 
                                     [:Date :Bid :Ask] 
                                     test-file))))
      "read-ts-csv: checking one element")
  (is (= 1.2964 
         (first ($ :Bid (read-oanda-csv test-file))))
      "read-oanda-csv: checking one element"))