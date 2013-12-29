(ns forest.diskseq-test
  (:use     [forest.debug]
            [forest.root]
            [forest.store]
            [forest.diskseq])
  (:require [clojure.test :refer :all]))

(defn test-print-tree [tree store]
  (cond (= (type tree) forest.two3.Leaf)
        (vec (map :sort-value (:bucket tree)))
        
        (= (type tree) forest.two3.Node)
        (vec (map #(test-print-tree (lookup store (:vhash %)) store)
                  (:children tree)))
        
        :else
        tree))

(deftest addition
  (let [empty-root (get-test-root (diskseq identity))
        added      (apply conjoin empty-root (range 64))
        reverse    (apply conjoin empty-root (range 63 -1 -1))
        shuffle    (apply conjoin empty-root (shuffle (range 64)))]
    ;; sorted insert balance
    (is (= (-> added :value :value :children second :min)
           (-> reverse :value :value :children second :min)
           32))
    (is (= (< (* 64 1/3)
              (-> shuffle :value :value :children second :min)
              (* 64 2/3))))))

(comment (deftest deletion
  (let [empty-root  (get-test-root (diskseq identity))
        first-half  (range 64)
        second-half (filter odd? (range 64))
        added       (apply conjoin empty-root first-half)
        removed1    (apply disjoin added second-half)
        removed2    (apply disjoin added (reverse second-half))
        removed3    (apply disjoin added (shuffle second-half))])
       
    ;; sorted insert balance
    (is (= (-> added :value :value :children second :min)
           (-> reverse :value :value :children second :min)
           32))
    (is (= (< (* 64 1/3)
              (-> shuffle :value :value :children second :min)
              (* 64 2/3))))))

(comment (run-tests))