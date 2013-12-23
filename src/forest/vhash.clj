(ns forest.vhash
  (:use [forest.debug])
  (:require [digest :as d]))

(defrecord HashRef [hash])

(defn vhash [value]
  (HashRef.
   (long
    (mod 
     (BigInteger.
      (d/sha-1 (str (pr-str value)))
      16)
     Long/MAX_VALUE))))

(defn vhash? [value]
  (= (type value) forest.vhash.HashRef))