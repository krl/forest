(ns forest.db
  (:require [platt.core :as p])
  (:use [potemkin :only [import-vars]]))

;; Just import and create some references to the key value store

(import-vars
  [platt.core
   open-database
   fetch
   put])

(defmacro with-db [db & body]
  `(p/with-db ~db ~@body))