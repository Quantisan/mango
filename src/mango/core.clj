(ns mango.core
  (:require [clj-time.core :as time]
            [clj-time.coerce :as time.coerce])  
  (:use [incanter core mongodb]
        [somnium.congomongo]))

(defn transform-column   ; pushed into incanter.core
" Apply function f to the specified column of data and replace the column
  with new values."
  [column f data] 
  (let [new-col-names (sort-by (partial = column) (col-names data))
        new-dataset (conj-cols
                      (sel data :except-cols column)
                      ($map f column data))]
    ($ (col-names data) (col-names new-dataset new-col-names))))

(defn long-to-date
  [dataset]
  (transform-column :Date time.coerce/from-long dataset))

(defn date-to-long
  [dataset]
  (transform-column :Date time.coerce/to-long dataset))

(defn fetch-ts
  [db-col]
  (long-to-date (fetch-dataset db-col)))