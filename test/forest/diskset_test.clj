(ns forest.diskset-test
  (:use     [forest.debug]
            [forest.root]
            [forest.diskset])
  (:require [clojure.test :refer :all]))

(deftest simple
  (let [test  (get-test-root (diskset))
        added (conjoin test :a :c)]
    (is (= (member? added :a) :a))
    (is (= (member? added :c) :c))
    (is (nil? (member? added :b)))))

(deftest adding
  (let [test    (get-test-root (diskset))
        members (range 100)
        added   (apply conjoin test members)
        added2  (apply conjoin test (shuffle members))]
    (is (= members
           (map (partial member? added) members)
           (map (partial member? added2) members)))
    (is (= added added2))))

(deftest add-remove-count
  (binding [forest.hamt/*bucket-size* 32
            forest.hamt/*bit-chunk-size* 4]
    (let [test    (get-test-root (diskset))

          multi   200

          set1    (range multi)
          set2    (range multi (* multi 2))

          comp     (apply conjoin test (shuffle set2))

          ;; testseq  (reduce (fn [coll new]
          ;;                    (println (number-of-elements (last coll)))
          ;;                    (conj coll (conjoin (last coll) new)))
          ;;                  [test]
          ;;                  (concat set1 set2))
          ;; testseq2 (reduce (fn [coll new]
          ;;                    (println (number-of-elements (last coll)))
          ;;                    (conj coll (disjoin (last coll) new)))
          ;;                  [(last testseq)]
          ;;                  set1)

          added    (apply conjoin test  (shuffle set1))
          added2   (apply conjoin added (shuffle set2))

          removed  (apply disjoin added2 (shuffle set1))]
      ;; (doseq [state testseq2]
      ;;   (reset! test-atom state)
      ;;   (Thread/sleep 1000))))

      (is (= (number-of-elements added) multi))
      (is (= (number-of-elements added2) (* multi 2)))
      (is (= (number-of-elements removed) multi)))))

(comment (run-tests))


