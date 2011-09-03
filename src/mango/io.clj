(ns mango.io
  (:require [clj-time.core :as time]
            [clj-time.coerce :as time.coerce]
            [clj-time.format :as time.format])  
  (:use [incanter core 
         (io :only (read-dataset)) 
         (mongodb :only (fetch-dataset))]))

(defn long-to-date
  [dataset]
  (transform-col :Date time.coerce/from-long dataset))

(defn date-to-long
  [dataset]
  (transform-col :Date time.coerce/to-long dataset))

(defn fetch-ts
  [db-col]
  (long-to-date (fetch-dataset db-col)))

(def oanda-fmt (time.format/formatter "dd/MM/YY HH:mm:ss"))

(defn read-oanda-csv
  [file]
  (let [fmt       (time.format/formatter "dd/MM/YY HH:mm:ss")
        dataset   (-> (read-dataset file :header false)
                      (col-names [:Date :Bid :Ask]))]
    (transform-col :Date #(time.format/parse fmt %) dataset)))
                                