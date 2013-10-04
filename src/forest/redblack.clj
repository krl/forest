(ns forest.redblack
  (:use [forest.debug]
        [forest.vhash]
        [forest.root]
        [forest.transaction]
        [forest.db]))

(def ^:dynamic *bucket-size* 128)

(defrecord RedBlackNode [color cut left right number-of-elements])
(defrecord RedBlackLeaf [bucket])

(defrecord Sortable [sort-value value])

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
  [cut left right]
  (RedBlackLeaf.
   (sort-elements (concat (:bucket left) (:bucket right)))))
  

(defn- split-leaf [transaction bucket]
  (let [[left-bucket right-bucket] (split-bucket bucket)
        cut                        (-> right-bucket first :sort-value)
        left-leaf                  (RedBlackLeaf. left-bucket)
        right-leaf                 (RedBlackLeaf. right-bucket)
        new-node                   (RedBlackNode. 
                                    :red
                                    cut
                                    (vhash left-leaf)
                                    (vhash right-leaf)
                                    (count bucket))]
    (store! transaction left-leaf right-leaf)
    new-node))

(defn- sortable-in-range [start end sortable]
  (and (or (nil? start)
           (>= (:sort-value sortable) start))
       (or (nil? end)
           (< (:sort-value sortable) end))))

(defn- empty-seq []
  (RedBlackLeaf. []))

(defn add-to-bucket [bucket sortable]
  (if (some (partial = sortable) bucket)
    bucket
    (sort-elements (conj bucket sortable))))

(defn remove-from-bucket [bucket sortable]
  (remove (partial = sortable) bucket))

(extend-type RedBlackLeaf
  SeqLike
  (conjoin* [this transaction sortable]
    (assert (= (type sortable) forest.redblack.Sortable)
            "only elements of type Sortable can be conjoined to a seq")
    (let [new-bucket (add-to-bucket (:bucket this) sortable)
          new-node   (if (> (count new-bucket)
                            *bucket-size*)
                       (split-leaf transaction new-bucket)
                       (assoc this :bucket new-bucket))]
      (store! transaction new-node)
      new-node))
  (disjoin* [this transaction sortable]
    (assert (= (type sortable) forest.redblack.Sortable)
            "only elements of type Sortable can be disjoined from a seq")
    (let [new-bucket (remove-from-bucket (:bucket this) sortable)
          new-node   (assoc this :bucket new-bucket)]
      (store! transaction new-node)
      new-node))
  (get-seq* [this _ start end reverse?]
    (map :value
         (filter (partial sortable-in-range start end)
                 (:bucket this))))
  (number-of-elements [this]
    (count (:bucket this))))

(extend-type RedBlackNode
  SeqLike
  (get-seq* [this transaction start end reverse?]
    (cond (and end (< (:cut this) end))
          (get-seq* (lookup transaction (:left this))
                    transaction
                    start end reverse?)
          (and start (>= (:cut this) start))
          (get-seq* (lookup transaction (:right this))
                    transaction
                    start end reverse?)
          :else
          (lazy-cat (get-seq* (lookup transaction (:left this))
                              transaction
                              start end reverse?)
                    (get-seq* (lookup transaction (:right this))
                              transaction
                              start end reverse?))))
  (conjoin* [this transaction element]
    (let [direction (if (< (:sort-value element)
                           (:cut this))
                      :left :right)
          old-leaf  (lookup transaction (direction this))
          new-leaf  (conjoin* old-leaf transaction element)
          new-node  (assoc this 
                      direction           (vhash new-leaf)
                      :number-of-elements (if (= (number-of-elements old-leaf)
                                                 (number-of-elements new-leaf))
                                            (:number-of-elements this)
                                            (inc (:number-of-elements this))))]
      (store! transaction 
              new-node new-leaf)
      new-node))
  (disjoin* [this transaction element]
    (let [direction (if (< (:sort-value element)
                           (:cut this))
                      :left :right)
          old-leaf  (lookup transaction (direction this))
          new-leaf  (disjoin* old-leaf transaction element)
          new-node  
          (cond (= (number-of-elements old-leaf)
                   (number-of-elements new-leaf))
                this
                ;; the previous size of the node was just above bucket size
                (= (:number-of-elements this) (inc *bucket-size*))
                (merge-leaves (:cut this)
                              new-leaf
                              (lookup transaction ((opposite direction) this)))
                :else
                (assoc this 
                  direction           (vhash new-leaf)
                  :number-of-elements (dec (:number-of-elements this))))]
      (store! transaction new-node new-leaf)
      new-node))
  (number-of-elements [this]
    (:number-of-elements this)))

(extend-type forest.root.Root
  SeqLike
  (conjoin* [this transaction element]
    (let [transaction (assoc transaction
                        :db (:db this))
          ref         (vhash
                       (conjoin* (when (:reference this)
                                   (lookup transaction 
                                           (:reference this)))
                                 transaction
                                 element))]
      (set-root-ref! transaction ref)
      (assoc this
        :reference ref)))
  (disjoin* [this transaction element]
    (let [transaction (assoc transaction
                        :db (:db this))
          ref         (vhash
                       (disjoin* (when (:reference this)
                                   (lookup transaction 
                                           (:reference this)))
                                 transaction
                                 element))]
      (set-root-ref! transaction ref)
      (assoc this
        :reference ref)))
  (get-seq* [this transaction start end reverse?]
    (let [transaction (assoc transaction
                        :db (:db this))]
      (get-seq* (when (:reference this)
                  (lookup transaction
                          (:reference this)))
                transaction
                start end reverse?))))

(extend-type nil
  SeqLike
  (conjoin* [this transaction value]
    (conjoin* (empty-seq) transaction value))
  ;; all the nils
  (get-seq* [this transaction direction start end reverse?])
  (disjoin* [this transaction value]))
