(ns forest.diskseq
  (:require [forest.two3 :as two3])
  (:use [forest.debug]
        [forest.root]
        [forest.store]))

(defrecord DiskSeq [sort-fn value reverse?])

(defmacro diskseq [sort-fn]
  `((fn [sort-fn#]
      (DiskSeq. sort-fn# two3/EMPTY false))
    '~sort-fn))

(extend-type DiskSeq
  CollectionLike
  (conjoin* [this store element]
    (assoc this
      :value
      (two3/add-at-root (:value this)
                      store
                      (two3/->Entry ((eval (:sort-fn this)) element)
                                  element))))
  (disjoin* [this store element]
    (assoc this
      :value
      (two3/delete-at-root (:value this)
                           store
                           (two3/->Entry ((eval (:sort-fn this)) element)
                                         element))))

  Counted
  (number-of-elements [this]
    (number-of-elements (:value this)))

  Rangeable
  (range-of* [this store from to]
    (range-of* (:value this) store from to))

  Stored
  (store! [this store]
    (to-disk! store this)
    (store! (:value this) store)))
    
