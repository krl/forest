(ns forest.core-test
  (:use     [forest.debug])
  (:require [clojure.test :refer :all]
            [forest.hashtree :refer :all]
            [platt.core :as platt]))

(defn random-path []
  (str "/tmp/forest-test-" (rand)))

(deftest simple-associate
  (let [toplevel   (diskmap (random-path))
        associated
        (transact
          (associate toplevel :a 1))]
    (is (= (get-key associated :a)) 1)))

(deftest keep-reference
  (let [toplevel   (diskmap (random-path))
        first-ref  (transact
                     (associate toplevel :a 1))
        second-ref (transact
                     (associate first-ref :a 2))]
    (is (= (get-key first-ref  :a) 1))
    (is (= (get-key second-ref :a) 2))))

(deftest dissociate-in-diskmap
  (binding [forest.hashtree/*bucket-size* 2]
    (let [toplevel        (diskmap (random-path))
          with-some-stuff (transact (associate toplevel 
                                               :a 1 :b 2 :c 3))
          with-more-stuff (transact (associate with-some-stuff
                                               :d 4 :e 5 :f 6))
          removed-again   (transact (dissociate with-more-stuff
                                                :d :e :f))]
      (is (= with-some-stuff removed-again)))))

(deftest assoc-dissoc
  (binding [forest.hashtree/*bucket-size* 8]
    (let [number-range (range 100)
          empty-map    (diskmap (random-path))
          assoc-map    (transact
                         (reduce (fn [diskmap nr]
                                   (associate diskmap nr nr))
                                 empty-map
                                 number-range))
          dissoc-map   (transact
                         (reduce (fn [diskmap nr]
                                   (dissociate diskmap nr))
                                 assoc-map
                                 number-range))]
      (print-variables dissoc-map empty-map)
      (is (= dissoc-map empty-map)))))

(deftest bucket-overflow
  (let [number-range (range (inc forest.hashtree/*bucket-size*))
        overflowmap  (transact
                       (reduce (fn [diskmap nr]
                                 (associate diskmap nr nr))
                               (diskmap (random-path))
                               number-range))]
    (is (every? #(= (get-key overflowmap %) %)
                number-range))))

(deftest lotsa-key-values
  (let [number-range (range 1000)
        overflowmap  (transact
                       (reduce (fn [diskmap nr]
                                 (associate diskmap nr nr))
                               (diskmap (random-path))
                               number-range))]
    (is (every? #(= (get-key overflowmap %) %)
                number-range))))

(comment (run-tests))