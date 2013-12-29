(ns forest.root-test
  (:use [forest.root]
        [forest.diskseq]
        [forest.debug])
  (:require [clojure.test :refer :all]))

;; (deftest write-to-disk-test
;;   (binding [forest.two3/*bucket-size* 1]
;;     (let [path         (str "/tmp/forest-test-" (rand))
;;           testroot     (root path (diskseq identity))
;;           ref1         (conjoin testroot 1234)
;;           ref2         (conjoin ref1 1312)
;;           ref2-written (write ref2)]
;;       ;; same result of written and open.
;;       (is (= (range-of ref2 nil nil)
;;              (range-of ref2-written nil nil)))
;;       ;; old value still accesible
;;       (is (= (range-of ref1 nil nil) (list 1234)))
;;       (is (= (range-of ref2 nil nil) (list 1234 1312)))
;;       ;; setup a new reference to the same datastructure        
;;       (let [testroot2 (root path :ignored)]
;;         (is (= (range-of testroot2 nil nil)
;;                (range-of ref2 nil nil)))))))
