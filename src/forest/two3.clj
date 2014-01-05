(ns forest.two3
  (:use [forest.root]
        [forest.store]
        [forest.debug]
        [forest.vhash]))

(def ^:dynamic *bucket-size* 3)
(def ^:dynamic *min-children* 2)
(def ^:dynamic *max-children* 3)

(defrecord Leaf [bucket])
(defrecord Node [children])
(defrecord Entry [sort-value value])
(defrecord ChildRef [vhash min])

(def EMPTY (->Leaf '()))

(defprotocol Two3Tree
  (add-at-root    [this store entry])
  (add            [this store entry])
  (delete-at-root [this store entry])
  (delete         [this store entry])
  (balance        [this store])
  (to-reference   [this store])
  (get-seq        [this start end])
  (num-children   [this])
  (min-value      [this])
  (split-node     [this])
  (merge-nodes    [this other]))

(defn delete-from-bucket [bucket entry]
  (remove #(= entry %) bucket))

(defn add-to-bucket [bucket entry]
  (sort-by #(vector (:sort-value %) (:hash %))
           (conj
            (delete-from-bucket bucket entry)
            entry)))

(defn split-root-maybe [node store]
  (if (> (num-children node)
         *max-children*)
    (->Node (map #(to-reference % store)
                 (split-node node)))
    node))

(defn remove-root-maybe [node store]
  (if (< (num-children node)
         *min-children*)
    (lookup store (-> node :children first :vhash))
    node))

(extend-type Leaf
  Two3Tree

  (split-node [this]
    (list (->Leaf (take *min-children* (:bucket this)))
          (->Leaf (nthnext (:bucket this) *min-children*))))

  (merge-nodes [this other]
    (assert (= (type this) (type other)))
    (->Leaf (sort-by min-value 
                     (concat (:bucket this) (:bucket other)))))

  (num-children [this]
    (count (:bucket this)))

  (min-value [this]
    (:sort-value (first (:bucket this))))

  (add-at-root [this store entry]
    (split-root-maybe (add this store entry)
                      store))

  (delete-at-root [this store entry]
    (delete this store entry))

  (add [this store entry]
    (assoc this :bucket (add-to-bucket (:bucket this) entry)))

  (delete [this store entry]
    (assoc this :bucket (delete-from-bucket (:bucket this) entry)))    

  (to-reference [this store]
    (to-heap! store this)
    (->ChildRef (vhash this)
                (-> this :bucket first :sort-value)))

  Counted
  (number-of-elements [this]
    (count (:bucket this))))

(defn select-subtree [childrefs min-val]
  (or
   (some #(and (>= min-val (min-value %)) %)
         (reverse childrefs))
   (first childrefs)))

(defn merge-into-children [underflow-child childrefs store]
  (let [merge-into      (select-subtree childrefs (min-value underflow-child))
        merge-into-node (lookup store (:vhash merge-into))
        untouched       (remove (partial = merge-into) childrefs)
        merged          (merge-nodes merge-into-node underflow-child)
        result          (if (> (num-children merged) *max-children*)
                          (split-node merged)
                          (list merged))
        refers (map #(to-reference % store) result)
        with-untouched (concat untouched refers)]
    (sort-by min-value
             with-untouched)))

(extend-type Node
  Two3Tree

  (split-node [this]
    (list (->Node (take *min-children* (:children this)))
          (->Node (nthnext (:children this) *min-children*))))

  (merge-nodes    [this other]
    (assert (= (type this) (type other)))
    (->Node (sort-by min-value 
                     (concat (:children this) (:children other)))))

  (num-children [this]
    (count (:children this)))

  (min-value [this]
    (min-value (first (:children this))))

  (add-at-root [this store entry]
    (split-root-maybe (add this store entry)
                      store))
  
  (add [this store entry]
    (let [childref         (select-subtree (:children this) 
                                           (:sort-value entry))
          new-child        (add (lookup store (:vhash childref))
                                store entry)
          untouched        (remove (partial = childref) (:children this))
          ;; if there's an overflow, split-node the node in two
          new-children     (if (> (num-children new-child) *max-children*)
                             (split-node new-child)
                             (list new-child))
          new-references   (map #(to-reference % store) new-children)
          sorted-childrefs (sort-by min-value (concat untouched new-references))]
      (assoc this 
        :children sorted-childrefs)))

  (delete-at-root [this store entry]
    (remove-root-maybe
     (delete this store entry)
     store))

  (delete [this store entry]
    (let [childref     (select-subtree (:children this) 
                                       (:sort-value entry))
          new-child        (delete (lookup store (:vhash childref))
                                   store entry)
          untouched        (remove (partial = childref) (:children this))
          new-childrefs    (if (< (num-children new-child) *min-children*)
                             (merge-into-children new-child untouched store)
                             (conj untouched (to-reference new-child store)))
          sorted-childrefs (sort-by min-value new-childrefs)]
      (assoc this
        :children sorted-childrefs)))

  (to-reference [this store]
    (to-heap! store this)
    (->ChildRef (vhash this)
                (-> this :children first :min)))

  Counted
  (number-of-elements [this]
    (reduce + (map :num-children (:children this)))))

(extend-type ChildRef
  Two3Tree
  (min-value [this]
    (:min this)))

(extend-type Entry
  Two3Tree
  (min-value [this]
    (:sort-value this)))
