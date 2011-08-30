(ns mango.test.core-tests
  (:use [mango.core]
        [clojure.test]
        [somnium.congomongo]
        [incanter core mongodb])
  (:require [clj-time.core :as time]
            [clj-time.coerce :as time.coerce])) 

(def ds (dataset [:Date :a] [[(time/date-time 1986 10 14 4 3 27 456) 1.555]
                             [(time/date-time 1986 10 15 2 8 53 318) 2.130]
                             [(time/date-time 1986 10 16 13 4 3 563) 1.850]]))

(def test-db "coretests-db")
(def test-col :testcol)

(defn setup! 
" Open connection and populate database."  
  [] 
  (do (mongo! :db test-db)
    (->> ds
      (transform-column :Date time.coerce/to-long)
      (insert-dataset test-col))))

(defn teardown! [] (drop-database! test-db))
 
(defmacro with-test-mongo [& body]
  `(do
     (setup!)
     ~@body
     (teardown!)))

;(deftest date-col-conversion
  

(deftest mongo-io-dataset
  (with-test-mongo       
    (is (= '(2.13 1.85)
           ($ :a (fetch-dataset test-col :where {:Date {:$gt 529646607456}})))
        "query on epoch time")))

(deftest fetch-time-series
  (with-test-mongo
    (is (= ds
           (fetch-ts test-col)))))

(comment   ; can't serialize jode time
(deftest clj-time-mongodb-interop
  (with-test-mongo
    (is (insert! test-col {:Date (time/date-time 1999 10 01)}))))
)