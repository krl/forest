(ns forest.root
  (:use [forest.debug]
        [forest.vhash] ;; temp
        [forest.store]))

;; protocols

(defprotocol CollectionLike
  (conjoin* [this store element])
  (disjoin* [this store element])
  (member*  [this store element]))

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
  
  (member* [this _ element]
    (member* (:value this)
             (:store this)
             element))
  
  MapLike
  (associate* [this _ key value]
    (assoc this
      :value (associate* (:value this)
                         (:store this)
                         key
                         value)))

  (dissociate* [this _ key]
    (assoc this
      :value (dissociate* (:value this)
                         (:store this)
                         key)))

  (get-key* [this _ key]
    (get-key* (:value this)
              (:store this)
              key))

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

(defn disjoin [root & elements]
  (reduce (fn [top element]
            (disjoin* top :_ element))
          root
          elements))

(defn associate [root & kvs]
  (assert (even? (count kvs)) "associate takes an even amount of key-values")
  (reduce (fn [top [key value]]
            (associate* top :_ key value))
          root
          (partition 2 kvs)))

(defn get-key [root key]
  (get-key* root :_ key))

(defn dissociate [root & keys]
  (reduce (fn [top key]
            (dissociate* top :_ key))
          root
          keys))

(defn member? [root element]
  (member* root :_ element))

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