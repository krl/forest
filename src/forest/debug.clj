(ns forest.debug
  (:use     [clojure.walk :only [postwalk-replace]])
  (:require [clojure.pprint :as p]))

(def pprint p/pprint)

(defmacro dbg [form]
  `(let [result# ~form]
     (print '~form "\n=>")
     (pprint result#)
     result#))

(def ^:dynamic *recursion-depth* 0)
(def ^:dynamic *max-recursion-depth* 6)

(defn print-indented [msg x]
  (print (str (apply str (repeat *recursion-depth* "| "))))
  (print msg " ")
  (prn x))

(defmacro print-variables [& variables]
  `(doseq [[symbol# value#] (map #(list %1 %2)
                               '~variables 
                               (list ~@variables))]
     (print (str symbol# ": "))
     (pprint value#)))

(defmacro defndbg [name docstring arguments & body]
  (let [body-sans-recur  (postwalk-replace {'recur name} body)
        arguments-sans-& (remove (partial = '&) arguments)]
    `(defn ~name ~docstring ~arguments
       (binding [*recursion-depth* (inc *recursion-depth*)]
         (if (> *recursion-depth* *max-recursion-depth*)
           (print-indented "max recursion depth hit for " '~name)
           ;; else
           (do
             (print-indented "calling " '~name)
             (doall (map #(when-not (:dbghide (meta %1)) 
                            (print-indented (str " - " %1 ": ") %2))
                         '~arguments-sans-&
                         ~arguments-sans-&
                         ))
             (let [result# (do ~@body-sans-recur)]
               (print-indented (str "result of " '~name ": ") result#)
               result#)))))))



