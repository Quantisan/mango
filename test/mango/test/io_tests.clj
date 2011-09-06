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

(def test-db "coretests-db")
(def test-coll :testcoll)

(def ^{:private true} db (make-connection test-db))

(defn setup! 
" Open connection and populate database."  
  [] 
  (with-mongo db
    (->> ds
      (date->long)
      (insert-dataset test-coll))))

(defn teardown!
" Drops test database."
  [] 
  (with-mongo db
    (drop-database! test-db)))
 
(defmacro with-test-mongo [& body]
  `(do
     (setup!)
     ~@body
     (teardown!)))

(deftest mongo-io-dataset
  (with-test-mongo       
    (is (= '(2.13 1.85)
           (with-mongo db
             ($ :a (fetch-dataset test-coll :where {:Date {:$gt 529646607456}}))))
        "query on epoch time")))

(deftest fetch-time-series
  (with-test-mongo
    (is (= ds
           ($ (col-names ds) (fetch-ts db test-coll))))))

(deftest read-csv
  (is (= (time/date-time 2004 01 02 19 01 27) 
         (parse-time "dd/MM/YY HH:mm:ss" date-string))
      "parse-time")
  (is (= 1.2964 
         (first ($ :Bid (read-ts-csv "dd/MM/YY HH:mm:ss" 
                                     [:Date :Bid :Ask] 
                                     "data/testing.csv"))))
      "read-ts-csv: checking one element")
  (is (= 1.2964 
         (first ($ :Bid (read-oanda-csv "data/testing.csv"))))
      "read-oanda-csv: checking one element"))