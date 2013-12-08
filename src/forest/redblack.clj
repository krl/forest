(ns forest.redblack
  (:use [forest.debug]
        [forest.vhash]
        [forest.store]
        [forest.root]))

(def ^:dynamic *bucket-size* 128)

(defrecord RedBlackNode [color cut number-of-elements left right])
(defrecord RedBlackLeaf [bucket])
(defrecord Sortable [sort-value value])

(defprotocol RedBlack
  "Methods that are specific for red black trees."
  (balance [this store])
  (conjoin-nonroot [this store sortable]))

(def empty-tree (RedBlackLeaf. []))

(defn make-sortable [sort-fn value]
  (Sortable. (sort-fn value) value))

(defn- sort-elements [elements]
  (sort-by #(vector (:sort-value %) (vhash %)) elements))

(defn split-bucket [bucket]
  (let [len   (/ (count bucket) 2)
        left  (take len bucket)
        right (drop len bucket)]
    [left right]))

(defn- merge-leaves
  "Takes two leaves and merges them together. Returns the new leaf."
  [branch cut left right]
  (RedBlackLeaf.
   (sort-elements (concat (:bucket left) (:bucket right)))))
  
;; tree helper functions

(defn- red? [node]
  (= (:color node) :red))

(defn- black?
  [node]
  ;; This works for leaves too, since they don't have a :color attribute
  (not (red? node)))

(defn- split-leaf [store leaf]
  (let [bucket                     (:bucket leaf)
        [left-bucket right-bucket] (split-bucket bucket)
        left-node                  (RedBlackLeaf. left-bucket)
        right-node                 (RedBlackLeaf. right-bucket)]
    (to-heap! store left-node right-node)
    (RedBlackNode. :red
                   (-> right-bucket first :sort-value)
                   (count bucket)
                   (vhash left-node)
                   (vhash right-node))))

(defn- sortable-in-range [start end sortable]
  (and (or (nil? start)
           (>= (:sort-value sortable) start))
       (or (nil? end)
           (< (:sort-value sortable) end))))

(defn add-to-bucket [bucket sortable]
  (if (some (partial = sortable) bucket)
    bucket
    (sort-elements (conj bucket sortable))))

(defn remove-from-bucket [bucket sortable]
  (remove (partial = sortable) bucket))

(defn rotate [store x y z a b c d]
  (let [child-elements 
        #(reduce + (map number-of-elements
                        (map (partial lookup store)
                             %1)))

        left-node  (RedBlackNode.
                    :black
                    x
                    (child-elements [a b])
                    a 
                    b)
        right-node (RedBlackNode.
                    :black
                    z 
                    (child-elements [c d])
                    c 
                    d)]
    (to-heap! store left-node right-node)
    (RedBlackNode. :red
                   y
                   (+ (number-of-elements left-node)
                      (number-of-elements right-node))
                   (vhash left-node)
                   (vhash right-node))))

(extend-type RedBlackLeaf
  RedBlack
  (balance [this _] this)
  (conjoin-nonroot [this store sortable]
    (conjoin* this store sortable))
  
  Stored
  (store! [this store]
    (to-disk! store this))

  CollectionLike
  (conjoin* [this store sortable]
    (let [new-bucket (add-to-bucket (:bucket this) sortable)]
      (if (> (count new-bucket)
             *bucket-size*)        
        (split-leaf store (assoc this :bucket new-bucket))
        (assoc this :bucket new-bucket))))
  (disjoin* [this store sortable]
    (assoc this 
      :bucket (remove-from-bucket (:bucket this) sortable)))

  Rangeable
  (range-of* [this _ from to]
    (map :value
         (filter (partial sortable-in-range from to)
                 (:bucket this))))

  Counted
  (number-of-elements [this]
    (count (:bucket this))))

(extend-type RedBlackNode
  RedBlack
  (balance [this store]
    ;; FIXME, do as few lookups as possible
    (let [left-child        (lookup store (:left this))
          left-left-child   (lookup store (:left left-child))
          left-right-child  (lookup store (:right left-child))
          right-child       (lookup store (:right this))
          right-left-child  (lookup store (:left right-child))
          right-right-child (lookup store (:right right-child))]
      (cond (and
             (red? left-child)
             (red? left-left-child))
            (rotate store 
                    (:cut left-left-child)
                    (:cut left-child)
                    (:cut this)

                    (:left  left-left-child)
                    (:right left-left-child)
                    (:right left-child)
                    (:right this))

            (and
             (red? left-child)
             (red? left-right-child))
            (rotate store
                    (:cut left-child)
                    (:cut left-right-child)
                    (:cut this)

                    (:left  left-child)
                    (:left  left-right-child)
                    (:right left-right-child)
                    (:right this))

            (and (red? right-child)
                 (red? right-left-child))            
            (rotate store
                    (:cut this)
                    (:cut right-left-child)
                    (:cut right-child)

                    (:left this)
                    (:left right-left-child)
                    (:right right-left-child)
                    (:right right-child))

            (and (red? right-child)
                 (red? right-right-child))
            (rotate store
                    (:cut this)
                    (:cut right-child)
                    (:cut right-right-child)
                    
                    (:left this)
                    (:left right-child)
                    (:left right-right-child)
                    (:right right-right-child))
            :else (assoc this
                    :number-of-elements
                    (+ (number-of-elements left-child)
                       (number-of-elements right-child))))))
  (conjoin-nonroot [this store sortable]
    (let [direction (if (< (:sort-value sortable)
                           (:cut this))
                      :left :right)
          new-node  (balance (conjoin-nonroot (lookup store (direction this))
                                              store
                                              sortable)
                             store)]
      (to-heap! store new-node)
      (assoc this direction (vhash new-node))))  

  Stored
  (store! [this store]
    (to-disk! store this)
    (store! (lookup-in-heap store (:left this)) store)
    (store! (lookup-in-heap store (:right this)) store))

  CollectionLike
  (conjoin* [this store sortable]
    ;; at root, always black
    (assoc (balance (conjoin-nonroot this store sortable) store)
      :color :black))

  Rangeable
  (range-of* [this store from to]
    (lazy-cat (when (or (nil? from) 
                        (< from (:cut this)))
                (range-of* (lookup store (:left this))
                           store from to))
              (when (or (nil? to)
                        (>= to (:cut this)))
                (range-of* (lookup store (:right this))
                           store from to))))

  Counted
  (number-of-elements [this]
    (:number-of-elements this)))
