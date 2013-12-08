(ns forest.diskseq-test
  (:use     [forest.debug]
            [forest.root]
            [forest.diskseq])
  (:require [clojure.test :refer :all]))

(deftest counting-test
  (let [test-number-of-elements 64
        randomly-populated-bucket-size-1
        (binding [forest.redblack/*bucket-size* 1]
          (reduce (fn [coll new] 
                    (conjoin coll new))
                  (get-test-root (diskseq identity))
                  (shuffle (range test-number-of-elements))))
        randomly-populated-bucket-size-4
        (binding [forest.redblack/*bucket-size* 4]
          (reduce (fn [coll new]
                    (conjoin coll new))
                  (get-test-root (diskseq identity))
                  (shuffle (range test-number-of-elements))))]
    (is (= (number-of-elements randomly-populated-bucket-size-1)
           (number-of-elements randomly-populated-bucket-size-4)
           test-number-of-elements))))

(deftest balance-test
  (binding [forest.redblack/*bucket-size* 1]
    (let [test-number-of-elements 64
          sequentially-populated
          (reduce (fn [coll new]
                    (conjoin coll new))
                  (get-test-root (diskseq identity))
                  (range test-number-of-elements))]
      (is (= (-> sequentially-populated
                 :value :value :cut))
          (/ test-number-of-elements 2)))))

(deftest range-test
  (binding [forest.redblack/*bucket-size* 1]
    (let [sequence-length 16
          memory-sequence (range sequence-length)
          disk-sequence
          (reduce (fn [coll new]
                    (conjoin coll new))
                  (get-test-root (diskseq identity))
                  memory-sequence)]
      (is (= (range-of disk-sequence nil nil)
             memory-sequence))      
      (is (= (take (/ sequence-length 2) memory-sequence)
             (range-of disk-sequence nil (/ sequence-length 2))))
      (is (= (drop (/ sequence-length 2) memory-sequence)
             (range-of disk-sequence (/ sequence-length 2) nil)))
      (is (= (take (/ sequence-length 2)
                   (drop (/ sequence-length 4) memory-sequence)))
          (range-of disk-sequence 
                    (/ sequence-length 4)
                    (/ sequence-length 4/3))))))

(comment (run-tests))