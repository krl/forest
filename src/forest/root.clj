(ns forest.root
  (:use [forest.transaction]
        [forest.debug]
        [forest.vhash]
        [forest.db]))

;; protocols

(defprotocol SeqLike
  (conjoin* [this sortable])
  (disjoin* [this sortable])
  (get-seq* [this stack direction start end]))

(defprotocol MapLike
  (get-key*           [this key])
  (dissociate*        [this key])
  (associate*         [this key val])
  (number-of-elements [this]))

(defrecord Root [db reference])

;; helper functions

(defn get-key [in key]
  (get-key* in key))  

(defn associate [in & kvs]
  (assert (in-transaction?) "associate must be called inside a transaction")
  (assert (even? (count kvs)) 
          "associate requires an even number of key value pairs")
  (reduce (fn [in [key val]]
            (associate* in key val))
          in
          (partition 2 kvs)))

(defn conjoin [in & elements]
  (reduce (fn [top sortable]
            (conjoin* top sortable))
          in
          elements))

(defn dissociate [in & keys]
  (assert (in-transaction?) "dissociate must be called inside a transaction")
  (reduce (fn [in key]
            (dissociate* in key))
          in keys))

(defn associate-in [in path & kvs]
  (assert (in-transaction?) "associate-in must be called inside a transaction")
  (assert (even? (count kvs))
          "associate-in requires an even number of key value pairs")
  (associate in (first path)
             (let [inner-map (get-key* in (first path))]
               (if (= (count path) 1)
                 (apply associate inner-map kvs)
                 (apply associate-in inner-map (rest path) kvs)))))

(defn conjoin-in [in path & elements]
  (assert (in-transaction?) "conjoin-in must be called inside a transaction")
  (associate in (first path)
             (let [inner-value (get-key* in (first path))]
               (if (= (count path) 1)
                 (apply conjoin inner-value elements)
                 (apply conjoin-in inner-value (rest path) elements)))))

(defmacro wrap-db [this & body]
  `(with-db (:db ~this) ~@body))

(def get-db-from-path
  (memoize open-database))

(defn root [path]
  (assert (in-transaction?) "root must be called from within a transaction")
  (let [db       (get-db-from-path path)
        root-ref (with-db db (fetch :root))]
    (Root. db root-ref)))

;; test helper

(defn get-test-root []
  (root (str "/tmp/forest-test-" (rand))))
