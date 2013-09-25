(ns forest.vhash
  (:require [digest :as d]))

(def MAX_HASH (BigInteger. (apply str (repeat 40 "f")) 16))

(defn vhash [value]
  (BigInteger.
   (d/sha-1 (pr-str (hash value)))
   16))