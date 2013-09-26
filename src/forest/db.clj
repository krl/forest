(ns forest.db
  (:require [platt.core :as p])
  (:use [potemkin :only [import-vars]]))

(import-vars
  [platt.core
   open-database
   fetch
   put])

(defmacro with-db [& body]
  `(p/with-db ~@body))