(ns forest.hashtree
  (:use [forest.debug]
        [clojure.math.numeric-tower :only [floor]]
        [forest.vhash]
        [forest.db]))

(def ^:dynamic *current-transaction* nil)
(def ^:dynamic *bucket-size* 128)

(defrecord ToplevelMap [db reference])
(defrecord DiskMapNode [cut left right number-of-elements])
(defrecord DiskMapLeaf [cut bucket])

(defprotocol MapLike
  (get-key            [this key])
  (dissociate*        [this key])
  (associate*         [this key val])
  (number-of-elements [this]))

(declare transact
         split-leaf
         merge-leaves
         empty-diskmap
         lookup
         store!
         set-transaction-root! 
         number-of-elements
         opposite
         set-root-ref!
         get-root-ref
         hash-ratio)

;; helper functions

(defn associate [in & kvs]
  (assert (even? (count kvs)) 
          "associate requires an even number of key value pairs")
  (reduce (fn [in [key val]]
            (associate* in key val))
          in
          (partition 2 kvs)))

(defn dissociate [in & keys]
  (reduce (fn [in key]
            (dissociate* in key))
          in keys))

;; type extensions

(extend-type ToplevelMap
  MapLike
  (associate* [this key val]
    (with-db (:db this)
      (let [ref (vhash
                 (associate*
                  (if (:reference this)
                    (lookup (:reference this))
                    (empty-diskmap))
                  key val))]
        (set-root-ref! (:db this) ref)
        (assoc this
          :reference ref))))
  (dissociate* [this key]
    (with-db (:db this)
      (let [ref (vhash
                 (dissociate*
                  (if (:reference this)
                    (lookup (:reference this))
                    (empty-diskmap))
                  key))]
        (set-root-ref! (:db this) ref)
        (assoc this
          :reference ref))))
  (get-key [this key]
    (with-db (:db this)
      (when (:reference this)
        (get-key (lookup (:reference this)) key)))))

(extend-type DiskMapLeaf
  MapLike
  (get-key [this key]
    (get (:bucket this) key))
  (associate* [this key value]
    ;; (println "associate in leaf")
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
      ;;(print-variables key direction)
      (get-key (lookup (direction this)) key)))
  (associate* [this key value]
    ;; (println "associate in node")
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
    (println "dissoc in node")
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

(defn opposite [dir]
  (if (= dir :left) :right :left))

(defn store!
  "Stores nodes in the transaction transient."
  [& nodes]
  (doseq [node nodes]
    (swap! (:transient *current-transaction*)
           assoc!
           (vhash node) node)))

(defn lookup [vhash]
  (or (and *current-transaction*
           (get @(:transient *current-transaction*) vhash))
      (fetch vhash)
      (throw (Exception. (str "lookup of vhash " vhash " failed.")))))

(defn set-root-ref! [db ref]
  (swap! (:roots *current-transaction*) assoc db ref))

(defn get-root-ref [db]
  (or (and *current-transaction*
           (get @(:roots *current-transaction*) db))
      (fetch :root)))

(defn empty-diskmap []
  (DiskMapLeaf.
   1/2
   (sorted-map-by #(> (hash %1) (hash %2)))))

(def ^:dynamic *write-count*)

(defn write-recursively [db node]
  ;; (println "writing to db:")
  ;; (print-variables (vhash node) node)
  (put (vhash node) node)
  (swap! *write-count* inc)
  (when (:left node)
    (write-recursively db (lookup (:left node)))
    (write-recursively db (lookup (:right node)))))

(defn write-transaction
  "Writes all nodes in transient referenced from transaction roots to disk
returns the root node reference."
  []
  ;; (print-variables *current-transaction*)
  (println "heap size: " (count @(:transient *current-transaction*)))
  (doseq [[db root-ref] @(:roots *current-transaction*)]
    (let [root-node (get @(:transient *current-transaction*)
                         root-ref)]
      ;; (print-variables db root-ref root-node)
      (with-db db
        (put :root root-ref)
        (binding [*write-count* (atom 0)]
          (write-recursively db root-node)
          (println "wrote " @*write-count* " nodes"))))))

(defmacro transact [& body]
  `(binding [*current-transaction*
             {:roots      (atom {})
              :transient  (atom (transient {}))}]
     (let [result# (do ~@body)]
       (write-transaction)
       result#)))

(defn split-map
  "Splits a map in two based on key hash."
  [to-split cut]
  (let [pred (fn [[key _]]
               (< (hash-ratio (vhash key)) cut))]
    [(into {} (filter pred to-split))
     (into {} (remove pred to-split))]))

(defn hash-ratio [nr]
  (/ (float (floor (/ nr       10000000000)))
     (float (floor (/ MAX_HASH 10000000000)))))

(defn split-ratio [ratio]
  (let [offset (/ 1 (int (* (denominator ratio) 2)))]
    (list (- ratio offset) (+ ratio offset))))

(defn split-leaf [cut bucket]
  ;; (println "splitting")
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

(defn merge-leaves [cut left right]
  ;; (println "merging")
  ;; (print-variables cut left right)
  (DiskMapLeaf.
   cut 
   (merge (:bucket left)
          (:bucket right))))

;; constructor

(defonce EMPTY_MAP (empty-diskmap))

(def get-db-from-path 
  (memoize open-database))

(defn diskmap [path]
  (let [db (get-db-from-path path)]
    (with-db db
      (put (vhash EMPTY_MAP) EMPTY_MAP) ; store reference by default
      (ToplevelMap. 
       db
       (or 
        (fetch :root)
        EMPTY_MAP)))))

