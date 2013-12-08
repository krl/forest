(ns forest.vhash
  (:use [forest.debug])
  (:require [digest :as d]))

(def MAX_HASH (BigInteger. (apply str (repeat 40 "f")) 16))

(defrecord HashRef [hash])

(defn vhash [value]
  (HashRef.
   (BigInteger.
    (d/sha-1 (str (pr-str value)))
    16)))

(defprotocol Dereference
  (dereference* [this db]))

(defn vhash? [value]
  (= (type value) forest.vhash.HashRef))

(defn dereference [thing db]
  (if (vhash? thing)
    (dereference* thing db)
    thing))