(ns forest.redblack-test
  (:use     [forest.debug]
            [forest.root]
            [forest.transaction])
  (:require [clojure.test :refer :all]
            [forest.redblack :refer :all]
            [platt.core :as platt]))

(deftest sort-sequence
  (transact
    (let [elements        (map #(hash-map :value % :rand (rand)) (range 10))
          sortable        (map #(make-sortable :value %) elements)
          toplevel        (get-test-root)
          with-elements   (apply conjoin toplevel sortable)]
      (is (= (get-seq with-elements nil nil nil))
          elements)
      (is (= (get-seq with-elements 5 nil nil)
             (drop 5 elements)))
      (is (= (get-seq with-elements nil 5 nil)
             (take 5 elements)))
      (is (= (get-seq with-elements 2 4 nil)
             (take 2 (drop 2 elements)))))))

(deftest bucket-overflow
  (binding [forest.redblack/*bucket-size* 4]
    (transact
      (let [toplevel      (get-test-root)
            sequence      (map #(hash-map :value %) (range 7))
            with-elements (reduce (fn [in value]
                                    (conjoin in (make-sortable :value value)))
                             toplevel
                             sequence)]
        (is (= (get-seq with-elements nil nil nil)
               sequence))))))

(deftest conjoin-disjoin
  (binding [forest.redblack/*bucket-size* 4]
    (transact
      (let [toplevel       (get-test-root)
            first-sequence (map #(hash-map :value %) (range 10))
            sec-sequence   (map #(hash-map :value %) (range 10 20))
            first-seq      (reduce (fn [in value]
                                     (conjoin in (make-sortable :value value)))
                                   toplevel
                                   first-sequence)
            sec-seq        (reduce (fn [in value]
                                     (conjoin in (make-sortable :value value)))
                                   first-seq
                                   sec-sequence)
            disj-seq       (reduce (fn [in value]
                                     (disjoin in (make-sortable :value value)))
                                   sec-seq
                                   first-sequence)]
        (is (= (get-seq disj-seq nil nil nil) sec-sequence))))))

(deftest balance-test
  (binding [forest.redblack/*bucket-size* 4]
    (transact
      (let [toplevel       (get-test-root)
            first-sequence (map #(hash-map :value %) (range 100))
            first-seq      (reduce (fn [in value]
                                     (conjoin in (make-sortable :value value)))
                                   toplevel
                                   first-sequence)]
        (lookup *current-transaction* (:reference first-seq))))))

(comment (run-tests))