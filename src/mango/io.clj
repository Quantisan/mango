(ns mango.io
  (:require [clj-time.core :as time]
            [clj-time.coerce :as time.coerce]
            [clj-time.format :as time.format])  
  (:use [somnium.congomongo :only (add-index!)]
        [incanter core 
         (io :only (read-dataset)) 
         (mongodb :only (fetch-dataset insert-dataset))]))

;(def ^{:private true} db (make-connection "oanda"))

(defn long->date
  [dataset]
  (transform-col :Date time.coerce/from-long dataset))

(defn date->long
  [dataset]
  (transform-col :Date time.coerce/to-long dataset))

(defn fetch-ts
" Fetch time-series data from MongoDB. Wrap inside with-mongo."
  [db-coll]
    (long->date (fetch-dataset db-coll)))

(defn push-ts
" Push time-series data to MongoDB. Wrap inside with-mongo."
  [db-coll dataset]
  (do 
    (add-index! db-coll [:Date] :unique true)
    (->> dataset
      (date->long)
      (insert-dataset db-coll))))

(defn parse-time
" Wrapper for clj-time.format/parse."
  [format string]
  (let [fmt       (time.format/formatter format)]
    (time.format/parse fmt string)))

(defn read-ts-csv
" Reads a generic time-series csv file.
  
  Parameters:
  date-format   A date/time parsing format string, e.g. \"ddMMYY\"
  column-names  a list of column names
  file          path and filename
"
  [date-format column-names file]
  (let [data   (-> (read-dataset file :header false)
                      (col-names column-names))]
    (transform-col :Date #(parse-time date-format %) data)))

(defn read-oanda-csv
" Reads a Oanda historical time series data file."
  [file]
  (let [oanda-format "dd/MM/YY HH:mm:ss"]
    (read-ts-csv oanda-format [:Date :Bid :Ask] file)))

(defn csv->mongo
" Reads a time-series csv file and dumps into mongo."  
  [db-coll csv-fn file]
  (let [data   (csv-fn file)]
    (push-ts db-coll data)))