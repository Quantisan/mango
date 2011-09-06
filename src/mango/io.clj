(ns mango.io
  (:require [clj-time.core :as time]
            [clj-time.coerce :as time.coerce]
            [clj-time.format :as time.format])  
  (:use [somnium.congomongo :only (add-index! make-connection with-mongo)]
        [incanter core 
         (io :only (read-dataset)) 
         (mongodb :only (fetch-dataset insert-dataset))]))

(def ^{:private true} db (make-connection "oanda"))

(defn long->date
  [dataset]
  (transform-col :Date time.coerce/from-long dataset))

(defn date->long
  [dataset]
  (transform-col :Date time.coerce/to-long dataset))

(defn fetch-ts
  [db db-coll]
  (with-mongo db
    (long->date (fetch-dataset db-coll))))

(defn push-ts
  [db db-coll dataset]
  (with-mongo db
    (do 
      (add-index! db-coll [:Date] :unique true)
      (->> dataset
        (date->long)
        (insert-dataset db-coll)))))

(def oanda-fmt (time.format/formatter "dd/MM/YY HH:mm:ss"))

(defn parse-time
" Wrapper for clj-time.format/parse."
  [format string]
  (let [fmt       (time.format/formatter format)]
    (time.format/parse fmt string)))

(defn read-ts-csv
  [date-format column-names file]
  (let [data   (-> (read-dataset file :header false)
                      (col-names column-names))]
    (transform-col :Date #(parse-time date-format %) data)))

(defn read-oanda-csv
" Reads a Oanda historical time series data file."
  [file]
  (read-ts-csv "dd/MM/YY HH:mm:ss" [:Date :Bid :Ask] file))

(defn csv->mongo
  [db db-coll csv-fn file]
  (let [data   (csv-fn file)
        data   (date->long data)]
    (push-ts db db-coll data)))