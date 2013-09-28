(ns forest.hashtree-test
  (:use     [forest.debug]
            [forest.root]
            [forest.transaction])
  (:require [clojure.test :refer :all]
            [forest.hashtree :refer :all]
            [platt.core :as platt]))

(deftest simple-associate
  (transact
    (let [toplevel   (get-test-root)
          associated
          (associate (dbg toplevel) :a 1)]
      (is (= (get-key associated :a)) 1))))

(deftest keep-reference
  (transact
    (let [toplevel   (get-test-root)
          first-ref  (associate toplevel :a 1)
          second-ref (associate first-ref :a 2)]
      (is (= (get-key first-ref  :a) 1))
      (is (= (get-key second-ref :a) 2)))))

(deftest dissociate-in-diskmap
  (binding [forest.hashtree/*bucket-size* 2]
    (transact
      (let [toplevel        (get-test-root)
            with-some-stuff (associate toplevel 
                                       :a 1 :b 2 :c 3)
            with-more-stuff (associate with-some-stuff 
                                       :d 4 :e 5 :f 6)
            removed-again   (dissociate with-more-stuff
                                        :d :e :f)]
      (is (= with-some-stuff removed-again))))))

(deftest assoc-dissoc
  (transact
    (binding [forest.hashtree/*bucket-size* 8]
      (let [number-range (range 100)
            empty-map    (get-test-root)
            assoc-map    (reduce (fn [diskmap nr]
                                   (associate diskmap nr nr))
                                 empty-map
                                 number-range)
            dissoc-map   (reduce (fn [diskmap nr]
                                   (dissociate diskmap nr))
                                 assoc-map
                                 number-range)]
        (is (= dissoc-map empty-map))))))

(deftest bucket-overflow
  (transact
    (let [number-range (range (inc forest.hashtree/*bucket-size*))
          overflowmap  (reduce (fn [diskmap nr]
                                 (associate diskmap nr nr))
                               (get-test-root)
                               number-range)]
    (is (every? #(= (get-key overflowmap %) %)
                number-range)))))

(deftest lotsa-key-values
  (transact
    (let [number-range (range 1000)
          overflowmap  (reduce (fn [diskmap nr]
                                 (associate diskmap nr nr))
                               (get-test-root)
                               number-range)]
      (is (every? #(= (get-key overflowmap %) %)
                  number-range)))))

(deftest nested-maps
  (transact
    (let [toplevel  (get-test-root)
          populated (associate-in toplevel [:test :a]
                      :x 3)]
      (is (=
           (-> populated
               (get-key :test)
               (get-key :a)
               (get-key :x))
           3)))))

(comment (run-tests))