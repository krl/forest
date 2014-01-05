(ns forest.diskseq-test
  (:use     [forest.debug]
            [forest.root]
            [forest.store]
            [forest.diskseq])
  (:require [clojure.test :refer :all]))

(deftest addition
  (let [empty-root (get-test-root (diskseq identity))
        number     128
        added      (apply conjoin empty-root (range number))
        reverse    (apply conjoin empty-root (reverse (range number)))
        shuffle    (apply conjoin empty-root (shuffle (range number)))]
    ;; sorted insert balance
    (is (= (-> added :value :value :children second :min)
           (/ number 2)))
    (is (= (-> reverse :value :value :children second :min)
           (/ number 2)))
    (is (< (* number 1/3)
           (-> shuffle :value :value :children second :min)
              (* number 2/3)))))

(deftest deletion
  (let [empty-root  (get-test-root (diskseq identity))
        number      128
        first-half  (range number)
        second-half (filter odd? (range number))

        added1      (apply conjoin empty-root first-half)
        removed1a   (apply disjoin added1 second-half)
        removed1b   (apply disjoin added1 (reverse second-half))
        removed1c   (apply disjoin added1 (shuffle second-half))

        added2      (apply conjoin empty-root (reverse first-half))
        removed2a   (apply disjoin added2 second-half)
        removed2b   (apply disjoin added2 (reverse second-half))
        removed2c   (apply disjoin added2 (shuffle second-half))]

    (is (= (-> removed1a :value :value :children second :min)
           (/ number 2)))
    (is (= (-> removed1b :value :value :children second :min)
           (/ number 2)))
    (is (= (-> removed1c :value :value :children second :min)
           (/ number 2)))

    (is (= (-> removed2a :value :value :children second :min)
           (/ number 2)))
    (is (= (-> removed2b :value :value :children second :min)
           (/ number 2)))
    (is (= (-> removed2c :value :value :children second :min)
           (/ number 2)))))