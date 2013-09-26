(ns forest.redblack
  (:use [forest.vhash]
        [forest.db]))

(def ^:dynamic *bucket-size* 128)

(defrecord ToplevelSeq  [db reference sort-fn])
(defrecord RedBlackNode [color cut left right number-of-elements])
(defrecord RedBlackLeaf [bucket])

(defprotocel SeqLike
  (conj*     [this value])
  (disj      [this value]))