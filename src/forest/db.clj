(ns forest.db
  ;; (:require [platt.core :as p])
  (:use [potemkin :only [import-vars]]))

(import-vars
  [platt.core
   with-db
   open-database
   fetch
   put])