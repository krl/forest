(ns forest.root
  (:use [forest.debug]
        [forest.store]))

;; protocols

(defprotocol CollectionLike
  (conjoin* [this store element])
  (disjoin* [this store element]))

(defprotocol Rangeable
  (range-of* [this store from to]))

(defprotocol Counted
  (number-of-elements [this]))

(defprotocol MapLike
  (get-key*    [this store key])
  (dissociate* [this store key])
  (associate*  [this store key val]))

(defrecord Root [store value])

(extend-type Root
  CollectionLike
  (conjoin* [this _ element]
    (assoc this
      :value (conjoin* (:value this)
                       (:store this)
                       element)))
  (disjoin* [this _ element]
    (assoc this
      :value (disjoin* (:value this)
                       (:store this)
                       element)))
  
  Rangeable
  (range-of* [this _ from to]
    (range-of* (:value this)
               (:store this)
               from to))

  Counted
  (number-of-elements [this]
    (number-of-elements (:value this))))

(defn conjoin [root & elements]
  (reduce (fn [top element]
            (conjoin* top :_ element))
          root
          elements))

(defn range-of [root from to]
  (range-of* root :_ from to))

(defn root [path initial-state]
  (let [store (store-from-path path)
        ref  (or (get-root store)
                 initial-state)]
    (Root. store ref)))

(defn write [root-handle]
  (store!          (:value root-handle) (:store root-handle))
  (set-root!       (:store root-handle) (:value root-handle))
  (assoc root-handle :store (flush-heap! (:store root-handle))))

;; test helper

(defn get-test-root [initial-state]
  (root (str "/tmp/forest-test-" (rand)) initial-state))