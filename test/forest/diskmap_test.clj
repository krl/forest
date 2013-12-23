(ns forest.diskmap-test
  (:use     [forest.debug]
            [forest.root]
            [forest.diskmap])
  (:require [clojure.test :refer :all]))

(deftest simple
  (let [test  (get-test-root (diskmap))
        added (associate test 
                         :a 1
                         :c 2)]
    (is (= (get-key added :a) 1))
    (is (= (get-key added :c) 2))
    (is (nil? (get-key added :b)))))
