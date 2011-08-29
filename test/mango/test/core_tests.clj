(ns mango.test.core-tests
  (:use [mango.core])
  (:use [clojure.test]))

(def d (dataset [:Date :a] [[(time/date-time 1986 10 14 4 3 27 456) 1.555]
                            [(time/date-time 1986 10 15 2 8 53 318) 2.130]]))

(deftest replace-me ;; FIXME: write
  (is false "No tests have been written."))
