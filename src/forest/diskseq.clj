(ns forest.diskseq
  (:use [forest.debug]
        [forest.root]
        [forest.store]
        [forest.redblack]))

(defrecord DiskSeq [sort-fn value reverse?])

(defmacro diskseq [sort-fn]
  `((fn [sort-fn#]
      (DiskSeq. sort-fn# forest.redblack/empty-tree false))
    '~sort-fn))

(extend-type DiskSeq
  CollectionLike
  (conjoin* [this store element]
    (assoc this
      :value
      (conjoin* (:value this)
                store
                (make-sortable (eval (:sort-fn this))
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
    
