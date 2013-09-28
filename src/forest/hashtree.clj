(ns forest.hashtree
  (:use [forest.debug]
        [clojure.math.numeric-tower :only [floor]]
        [forest.vhash]
        [forest.db]
        [forest.transaction]))

(def ^:dynamic *bucket-size* 128)

(defrecord DiskMapNode [cut left right number-of-elements])
(defrecord DiskMapLeaf [cut bucket])

(defprotocol MapLike
  (get-key            [this key])
  (dissociate*        [this key])
  (associate*         [this key val])
  (number-of-elements [this]))

(declare split-leaf
         merge-leaves
         empty-diskmap
         number-of-elements
         opposite
         hash-ratio)

;; type extensions

(extend-type DiskMapLeaf
  MapLike
  (get-key [this key]
    (get (:bucket this) key))
  (associate* [this key value]
    (let [new-bucket (assoc (:bucket this) key value)
          new-node
          (if (> (count new-bucket)
                 *bucket-size*)
            (split-leaf (:cut this) new-bucket)
            (assoc this :bucket new-bucket))]
      (store! new-node)
      new-node))
  (dissociate* [this key]
    (let [new-bucket (dissoc (:bucket this) key)
          new-node   (assoc this :bucket new-bucket)]
      (store! new-node)
      new-node))
  (number-of-elements [this]
    (count (:bucket this))))

(extend-type DiskMapNode
  MapLike
  (get-key [this key]
    (let [direction (if (< (hash-ratio (vhash key)) 
                           (:cut this)) 
                      :left :right)]
      (get-key (lookup (direction this)) key)))
  (associate* [this key value]
    (let [direction (if (< (hash-ratio (vhash key))
                           (:cut this))
                      :left :right)
          old-leaf  (lookup (direction this))
          new-leaf  (associate* old-leaf key value)
          new-node  (assoc this 
                      direction           (vhash new-leaf)
                      :number-of-elements (if (= (number-of-elements old-leaf)
                                                 (number-of-elements new-leaf))
                                            (:number-of-elements this)
                                            (inc (:number-of-elements this))))]
      (store! new-node new-leaf)
      new-node))
  (dissociate* [this key]
    (let [direction    (if (< (hash-ratio (vhash key))
                              (:cut this)) :left :right)
          old-leaf     (lookup (direction this))
          new-leaf     (dissociate* old-leaf key)

          new-node
          (cond 
            ;; nothing was removed
            (= (number-of-elements old-leaf)
               (number-of-elements new-leaf))
            this                
            ;; the previous size of the node was just above bucket size
            (= (:number-of-elements this) (inc *bucket-size*))
            (merge-leaves (:cut this)
                          new-leaf
                          (lookup ((opposite direction) this)))

            :else
            (assoc this 
              direction (vhash new-leaf)
              :number-of-elements (dec (:number-of-elements this))))]
      (store! new-node)
      new-node))
  (number-of-elements [this]
    (:number-of-elements this)))

(defn- opposite [dir]
  (if (= dir :left) :right :left))

(defn empty-diskmap
  "Returns an empty diskmap leaf"
  []
  (DiskMapLeaf.
   1/2
   (sorted-map-by #(> (hash %1) (hash %2)))))

 (defn- split-map
  "Splits a map in two based on key hash."
  [to-split cut]
  (let [pred (fn [[key _]]
               (< (hash-ratio (vhash key)) cut))]
    [(into {} (filter pred to-split))
     (into {} (remove pred to-split))]))

 (defn- hash-ratio
  "Calculates a value between 0 and 1 based on vhash"
  [vhash]
  (/ (float (floor (/ vhash    10000000000)))
     (float (floor (/ MAX_HASH 10000000000)))))

 (defn- split-ratio
  "Calculates two new split ratios from argument."
  [ratio]
  (let [offset (/ 1 (int (* (denominator ratio) 2)))]
    (list (- ratio offset) (+ ratio offset))))

 (defn- split-leaf
  "Takes a leaf and returns a node pointing to two new leaves.
The leaves are stored in the transaction"
  [cut bucket]
  (let [[left-bucket right-bucket] (split-map bucket cut)
        [left-cut right-cut]       (split-ratio cut)

        left-leaf                  (DiskMapLeaf. left-cut left-bucket)
        right-leaf                 (DiskMapLeaf. right-cut right-bucket)
        new-node                   (DiskMapNode.
                                    cut
                                    (vhash left-leaf)
                                    (vhash right-leaf)
                                    (inc *bucket-size*))]    
    (store! left-leaf right-leaf)
    new-node))

 (defn- merge-leaves
  "Takes two leaves and merges them together. Returns the new leaf."
  [cut left right]
  (DiskMapLeaf.
   cut 
   (merge (:bucket left)
          (:bucket right))))