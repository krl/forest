(ns forest.root
  (:use [forest.hashtree]
        [forest.transaction]
        [forest.debug]
        [forest.vhash]
        [forest.db]))

(defrecord Root [db reference])

;; helper functions

(defn associate [in & kvs]
  (assert (in-transaction?) "associate must be called inside a transaction")
  (assert (even? (count kvs)) 
          "associate requires an even number of key value pairs")
  (reduce (fn [in [key val]]
            (associate* in key val))
          in
          (partition 2 kvs)))

(defn dissociate [in & keys]
  (assert (in-transaction?) "dissociate must be called inside a transaction")
  (reduce (fn [in key]
            (dissociate* in key))
          in keys))

(defn associate-in [in path & kvs]
  (print-variables in path kvs)
  (assert (in-transaction?) "associate-in must be called inside a transaction")
  (assert (even? (count kvs))
          "associate-in requires an even number of key value pairs")
  (associate in (first path)
             (let [inner-map (or (get-key in (first path))
                                 (empty-diskmap))]
               (if (= (count path) 1)
                 (apply associate inner-map kvs)
                 (apply associate-in inner-map (rest path) kvs)))))

(defn conj-in [in path & elements]
  
  
;; example

(extend-type Root
  MapLike
  (associate* [this key val]
    (with-db (:db this)
      (let [ref (vhash
                 (associate*
                  (lookup (:reference this))
                  key val))]
        (set-root-ref! (:db this) ref)
        (assoc this
          :reference ref))))
  (dissociate* [this key]
    (with-db (:db this)
      (let [ref (vhash
                 (dissociate*
                  (lookup (:reference this))                  
                  key))]
        (set-root-ref! (:db this) ref)
        (assoc this
          :reference ref))))
  (get-key [this key]
    (with-db (:db this)
      (get-key (lookup (:reference this)) key))))

(def get-db-from-path
  (memoize open-database))

;; test helper

(defn get-test-root []
  (root (str "/tmp/forest-test-" (rand))))

(def EMPTY_MAP (empty-diskmap))

(defn root [path]
  (assert (in-transaction?) "root must be called from within a transaction")
  (let [db       (get-db-from-path path)
        root-ref (or (with-db db (fetch :root))
                     (do
                       (store! EMPTY_MAP)
                       (set-root-ref! db (vhash EMPTY_MAP))
                       (vhash EMPTY_MAP)))]    
    (Root. db root-ref)))