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




                                