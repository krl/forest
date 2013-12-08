(ns forest.store  
  (:require [platt.core :as p])
  (:use [forest.vhash]
        [forest.debug]))

(def get-db-from-path
  (memoize p/open-database))

(defrecord Store [db heap])

(defprotocol StoreProtocol
  (to-heap*       [this nodes])
  (to-disk!       [this node])
  (flush-heap!    [this])
  (lookup         [this key])
  (lookup-in-heap [this key])
  (set-root!      [this node]))

(defprotocol Stored
  (store! [this store]))

(extend-type Store
  StoreProtocol
  (to-heap* [this nodes]
    (swap! (:heap this)
           #(reduce (fn [coll node]
                      (assoc coll (vhash node) node))
                    %
                    nodes)))
  (to-disk! [this node]
    (p/with-db (:db this)
      (p/put (vhash node) node)))
  (set-root! [this node]
    (p/with-db (:db this)
      (p/put :root node)))
  (flush-heap! [this]
    (assoc this :heap (atom {}))
    this)
  (lookup [this key]    
    (let [result (or (lookup-in-heap this key)
                     (p/with-db (:db this)
                       (p/fetch key)))]
      ;; (println (str "looking up " key " => " result))
      result))
  (lookup-in-heap [this key]
    (get @(:heap this) key)))

(defn to-heap! [this & nodes]
  (to-heap* this nodes))

(defn store-from-path [path]
  (Store. (get-db-from-path path)
          (atom {})))

(defn get-root [store]
  (p/with-db (:db store)
    (p/fetch :root)))  