(ns mango.core
  (:require [clj-time.core :as time]
            [clj-time.coerce :as time.coerce])  
  (:use [incanter core mongodb]
        [somnium.congomongo]))

;; credit to: Xavier Shay
(defn transform-column
" Apply function f to the specified column of data and replace the column
  with new values."
  [column f data] 
  (let [new-col-names (sort-by (partial = column) (col-names data))
        new-dataset (conj-cols
                      (sel data :except-cols column)
                      ($map f column data))]
    ($ (col-names data) (col-names new-dataset new-col-names))))