(ns forest.diskmap
  (:use [forest.debug]
        [forest.root]
        [forest.vhash]
        [forest.store])
  (:require [forest.hamt :as hamt]))

(defrecord DiskMap [value])

(defn diskmap []
  (->DiskMap hamt/EMPTY))

(extend-type DiskMap
  MapLike

  (associate* [this store key value]
    (assoc this
      :value
      (hamt/insert-element (:value this)
                           store
                           0
                           (forest.hamt/->Entry (vhash key) value))))

  (dissociate* [this store key]
    (assoc this
      :value
      (hamt/remove-element (:value this)
                           store
                           0
                           (vhash key))))

  (get-key* [this store key]
    (hamt/get-value (:value this)
                    store
                    0
                    (vhash key)))

  Counted
  (number-of-elements [this]
    (number-of-elements (:value this)))

  Stored
  (store! [this store]
    (to-disk! store this)
    (store! (:value this) store)))
