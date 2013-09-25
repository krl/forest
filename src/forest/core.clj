(ns forest.core
  (:use     [forest.debug]
            [clojure.walk :only [postwalk]]
            [clojure.math.numeric-tower :only [floor]])
  (:require [digest :as d]
            [platt.core :as platt]))

(def MAX_HASH (BigInteger. (apply str (repeat 40 "f")) 16))
(def ^:dynamic *current-transaction* nil)

(def ^:dynamic *bucket-size* 128)
(def EMPTY_MAP nil)

(defn vhash [value]
  (BigInteger.
   (d/sha-1 (pr-str (hash value)))
   16))

(defrecord DiskMapNode [cut left right number-of-elements])
(defrecord DiskMapLeaf [cut bucket])

(defrecord ToplevelMap [db reference])

(defprotocol MapLike
  (get-key            [this key])
  (associate          [this key value])
  (dissociate         [this key])
  (number-of-elements [this]))

;; type extensions

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
         get-root-ref)

(extend-type ToplevelMap
  MapLike
  (associate [this key val]
    (platt/with-db (:db this)
      (let [ref (vhash
                 (associate
                  (if (:reference this)
                    (lookup (:reference this))
                    (empty-diskmap))
                  key val))]
        (set-root-ref! (:db this) ref)
        (assoc this
          :reference ref))))
  (get-key [this key]
    (platt/with-db (:db this)
      (when (:reference this)
        (get-key (lookup (:reference this)) key)))))

(extend-type DiskMapLeaf
  MapLike
  (get-key [this key]
    (get (:bucket this) key))
  (associate [this key value]
    ;; (println "associate in leaf")
    (let [new-bucket (assoc (:bucket this) key value)
          new-node
          (if (> (count new-bucket)
                 *bucket-size*)
            (split-leaf (:cut this) new-bucket)
            (assoc this :bucket new-bucket))]
      (store! new-node)
      new-node))
  (dissociate [this key]    
    (assoc this :bucket 
           (dissoc (:bucket this) key)))
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
  (associate [this key value]
    ;; (println "associate in node")
    (let [direction (if (< (hash-ratio (vhash key))
                           (:cut this))
                      :left :right)
          old-leaf  (lookup (direction this))
          new-leaf  (associate old-leaf key value)
          new-node  (assoc this 
                      direction           (vhash new-leaf)
                      :number-of-elements (if (= (number-of-elements old-leaf)
                                                 (number-of-elements new-leaf))
                                            (:number-of-elements this)
                                            (inc (:number-of-elements this))))]
      (store! new-node new-leaf)
      new-node))
  (dissociate [this key]
    (let [direction    (if (< (hash-ratio (vhash key))
                              (:cut this)) :left :right)
          old-leaf     (lookup (direction this))
          new-leaf     (dissociate old-leaf key)

          new-node
          (cond (= (number-of-elements old-leaf)
                   (number-of-elements new-leaf))
                this
                
                (= (:number-of-elements this) (inc *bucket-size*))
                (merge-leaves (:cut this)
                              new-leaf
                              (lookup ((opposite direction) this)))

                :else
                (assoc this 
                  direction new-leaf
                  :number-of-elements (dec (:number-of-elements this))))]
      (store! new-node new-leaf)
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
      (platt/fetch vhash)
      (throw (Exception. (str "lookup of vhash " vhash " failed.")))))

(defn set-root-ref! [db ref]
  (swap! (:roots *current-transaction*) assoc db ref))

(defn get-root-ref [db]
  (or (and *current-transaction*
           (get @(:roots *current-transaction*) db))
      (platt/fetch :root)))

(defn empty-diskmap []
  (DiskMapLeaf.
   1/2
   (sorted-map-by #(> (hash %1) (hash %2)))))

(def ^:dynamic *write-count*)

(defn write-recursively [db node]
  ;; (println "writing to db:")
  ;; (print-variables (vhash node) node)
  (platt/put (vhash node) node)
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
      (platt/with-db db
        (platt/put :root root-ref)
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
  (let [;; keyhashes (map (fn [[key _]] (vhash key)) to-split)
        ;; _         (dbg (readable-hash (apply min keyhashes)))
        ;; _         (dbg (readable-hash (apply max keyhashes)))
        pred (fn [[key _]]
               ;; (println "----")
               ;; (println (readable-hash (vhash key)))
               ;; (println (readable-hash cut))
               (< (hash-ratio (vhash key)) cut))]
    [(into {} (filter pred to-split))
     (into {} (remove pred to-split))]))

(defn hash-ratio [nr]
  (/ 
   (float (floor    
           (/ nr
              10000000000)))
   (float (floor
           (/ MAX_HASH 
              10000000000)))))

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

    ;; (println "diff1" (readable-hash (- left-cut cut)))
    ;; (println "diff2" (readable-hash (- right-cut cut)))

    ;; (println "splitting" 
    ;;          (readable-hash cut)
    ;;          "into" 
    ;;          (readable-hash left-cut)
    ;;          "and" 
    ;;          (readable-hash right-cut))
    
    (if-not (< left-cut cut right-cut)
      (print-variables left-cut cut right-cut))    

    (store! new-node left-leaf right-leaf)
    new-node))

(defn merge-leaves [cut left right]
  ;; (println "merging")
  (DiskMapLeaf. 
   cut (merge (:bucket left)
              (:bucket right))))

(def get-db-from-path 
  (memoize platt/open-database))

(defn diskmap [path]
  (let [db (get-db-from-path path)]
    (platt/with-db db
      (forest.core.ToplevelMap. db (platt/fetch :root)))))