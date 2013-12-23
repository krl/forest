(ns forest.hamt
  (:use [clojure.math.numeric-tower :only [expt]]
        [forest.root]
        [forest.store]
        [forest.debug]
        [forest.vhash]))

(def ^:dynamic *bucket-size* 32)
(def ^:dynamic *bit-chunk-size* 4)

(defrecord Leaf [bucket])
(defrecord Node [children number-of-elements])
(defrecord Entry [hash value])

(def EMPTY (->Leaf ()))

(defprotocol HAMT
  (insert-element [this store level entry])
  (remove-element [this store level hash])
  (get-value      [this store level hash]))

(defn remove-from-bucket [bucket hash]
  (remove #(= (:hash %) hash)
          bucket))

(defn add-to-bucket [bucket entry]
  (sort-by #(:hash (:hash %))
           (conj
            (remove-from-bucket bucket (:hash entry))
            entry)))

(defn hash-part [hash part]
  (bit-and (dec (expt 2 *bit-chunk-size*))
           (bit-shift-right
            (:hash hash)
            (* *bit-chunk-size* part))))

(defn split-bucket [bucket level]
  (reduce (fn [coll new]
            (let [part (hash-part (:hash new) level)]
              (assoc coll part
                     (conj (or (get coll part)
                               #{})
                           new))))
          {}
          bucket))

(defn split-leaf [store bucket level]
  (reduce (fn [new-node [part bucket]]
            (let [leaf (->Leaf bucket)]
              (to-heap! store leaf)
              (assoc-in new-node [:children part] (vhash leaf))))
          (->Node {} (inc *bucket-size*))
          (split-bucket bucket level)))

(defn merge-leaves [node store]
  (Leaf. (mapcat (fn [[_ ref]]
                   (:bucket (lookup store ref)))
                 (:children node))))

(extend-type Leaf
  HAMT
  (get-value [this _ _ hash]
    (some #(and (= (:hash %) hash)
                (:value %))
          (:bucket this)))

  (insert-element [this store level entry]
    (let [new-bucket (add-to-bucket (:bucket this)
                                    entry)]
      (if (> (count new-bucket) *bucket-size*)
        (split-leaf store new-bucket level)
        (assoc this :bucket new-bucket))))

  (remove-element [this store level hash]
    (assoc this 
      :bucket (remove-from-bucket (:bucket this) hash)))

  Counted
  (number-of-elements [this]
    (count (:bucket this)))

  Stored
  (store! [this store]
    (to-disk! store this)))

(extend-type Node
  HAMT
  (get-value [this store level hash]
    (let [part    (hash-part hash level)
          pointer (get (:children this) part)]
      (when pointer
        (get-value (lookup store pointer) 
                   store
                   (inc level)
                   hash))))

  (insert-element [this store level entry]
    (let [part      (hash-part (:hash entry) level)
          pointer   (get (:children this) part)
          old-child (if pointer (lookup store pointer) EMPTY)
          new-child (insert-element old-child store (inc level) entry)]
      (to-heap! store new-child)
      (-> this
          (assoc-in [:children part] (vhash new-child))
          (assoc :number-of-elements 
            (if (> (number-of-elements new-child)
                   (number-of-elements old-child))
              (inc (:number-of-elements this))
              (:number-of-elements this))))))

  (remove-element [this store level hash]
    (let [part     (hash-part hash level)
          pointer  (get (:children this) part)]
      (if pointer
        (let [old-child       (lookup store pointer)
              new-child       (remove-element old-child store (inc level) hash)
              new-nr-elements (if (> (number-of-elements old-child)
                                     (number-of-elements new-child))
                                (dec (:number-of-elements this))
                                (:number-of-elements this))]
          ;(print-variables new-nr-elements)
          (let [new-node (-> this
                             (assoc-in [:children part] (vhash new-child))
                             (assoc :number-of-elements new-nr-elements))]
            (to-heap! store new-child)
            (if (= new-nr-elements *bucket-size*)
              (merge-leaves new-node store)
              new-node)))
        ;; no pointer, nothing done, return self
        this)))

  Counted
  (number-of-elements [this]
    (:number-of-elements this))

  Stored
  (store! [this store]
    (to-disk! store this)
    (doseq [[_ child] (:children this)]
      (to-disk! store child))))
