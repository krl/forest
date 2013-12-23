(ns forest.diskset
  (:use [forest.debug]
        [forest.root]
        [forest.vhash]
        [forest.store])
  (:require [forest.hamt :as hamt]))

(defrecord DiskSet [value])

(defn diskset []
  (->DiskSet hamt/EMPTY))

(extend-type DiskSet
  CollectionLike
  (conjoin* [this store element]
    (assoc this
      :value
      (hamt/insert-element (:value this)
                           store
                           0
                           (forest.hamt/->Entry (vhash element) element))))
  
  (member* [this store element]
    (hamt/get-value (:value this)
                    store
                    0
                    (vhash element)))
  
  (disjoin* [this store element]
    (assoc this
      :value
      (hamt/remove-element (:value this)
                           store
                           0
                           (vhash element))))

  Counted
  (number-of-elements [this]
    (number-of-elements (:value this)))

  Stored
  (store! [this store]
    (to-disk! store this)
    (store! (:value this) store)))
