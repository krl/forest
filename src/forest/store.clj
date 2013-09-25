(ns forest.store)

(defprotocol Store
  (fetch  [this key])
  (store  [this key value]))

(defrecord MemoryStore [store]
  Store
  (fetch [this key]
    (get @store key))
  (store [this key value]
    (alter store assoc key value)))
    
(defn memory-store []
  (MemoryStore. (ref {})))