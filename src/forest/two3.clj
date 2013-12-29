(ns forest.two3
  (:use [forest.root]
        [forest.store]
        [forest.debug]
        [forest.vhash]))

(def ^:dynamic *bucket-size* 3)
(def ^:dynamic *max-children* 3)

(defrecord Leaf [bucket])
(defrecord Node [children])
(defrecord Entry [sort-value value])
(defrecord ChildRef [vhash min])

(def EMPTY (->Leaf '()))

(defprotocol Two3Tree
  (add-at-root  [this store entry])
  (add          [this store entry])
  (balance      [this store])
  (to-reference [this store])
  (delete       [this store entry])
  (get-seq      [this start end]))

(defn new-root-maybe [result store]
  (if (> (count result) 1)
    (->Node (map #(to-reference % store) result))
    (first result)))

(defn delete-from-bucket [bucket entry]
  (remove #(= entry %) bucket))

(defn add-to-bucket [bucket entry]
  (sort-by #(vector (:sort-value %) (:hash %))
           (conj
            (delete-from-bucket bucket entry)
            entry)))

(extend-type Leaf
  Two3Tree

  (add-at-root [this store entry]
    (new-root-maybe 
     (add this store entry)
     store))

  (delete-at-root [this store entry]
    (new-root-maybe 
     (add this store entry)
     store))

  (add [this store entry]
    ;;(println "add in leaf")
    (balance
     (assoc this :bucket (add-to-bucket (:bucket this) entry))
     store))

  (delete [this store entry]
    ;;(println "delete in leaf")
    (balance
     (assoc this :bucket (delete-from-bucket (:bucket this) entry))
     store))

  (balance [this store]
    ;;(println "balance leaf")
    (if (> (count (:bucket this)) *bucket-size*)
      (list
       (->Leaf (take (/ *bucket-size* 2) (:bucket this)))
       (->Leaf (nthnext (:bucket this) (/ *max-children* 2))))
      (list this)))

  (to-reference [this store]
    (to-heap! store this)
    (->ChildRef (vhash this)
                (-> this :bucket first :sort-value)))

  Counted
  (number-of-elements [this]
    (count (:bucket this))))

(extend-type Node
  Two3Tree

  (add-at-root [this store entry]
    (new-root-maybe 
     (add this store entry)
     store))
  
  (add [this store entry]
    ;;(println "add in node")
    (balance
     (let [childref     (or (some #(and (<= (:min %) (:sort-value entry)) %)
                                  ;; look at the largest values first
                                  (reverse (:children this)))
                            ;; else just pick the first
                            (first (:children this)))
           new-children (add (lookup store (:vhash childref))
                                store
                                entry)
           untouched    (remove (partial = childref) (:children this))
           added        (concat untouched (map #(to-reference % store)
                                               new-children))]
       (assoc this :children
              (sort-by :min added)))      
     store))

  ;; (delete [this store entry]
  ;;   (balance
  ;;    (let [childref     (or (some #(and (<= (:min %) (:sort-value entry)) %)
  ;;                                 ;; look at the largest values first
  ;;                                 (reverse (:children this)))
  ;;                           ;; else just pick the first
  ;;                           (first (:children this)))
  ;;          new-children (delete
  ;;    store))

  (balance [this store]
    ;;(println "balance node")
    (if (> (count (:children this)) *max-children*)
      (list 
       (->Node (take (/ *max-children* 2) (:children this)))
       (->Node (nthnext (:children this) (/ *max-children* 2))))
      (list this)))

  (to-reference [this store]
    (to-heap! store this)
    (->ChildRef (vhash this)
                (-> this :children first :min)))

  Counted
  (number-of-elements [this]
    (reduce + (map :num-children (:children this)))))
