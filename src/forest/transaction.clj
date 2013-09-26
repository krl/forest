(ns forest.transaction
  (:use [forest.db]
        [forest.debug]
        [forest.vhash]))

(def ^:dynamic *current-transaction* nil)

(defn set-root-ref! [db ref]
  (swap! (:roots *current-transaction*) assoc db ref))

(defn store!
  "Stores nodes in the transaction transient."
  [& nodes]
  (doseq [node nodes]
    (swap! (:transient *current-transaction*)
           assoc
           (vhash node) node)))

(defn lookup [vhash]
  ;; (println "lookup")
  ;; (print-variables vhash)
  (or (and *current-transaction*
           (get @(:transient *current-transaction*) vhash))
      (fetch vhash)
      (throw (Exception.
              (str "lookup of vhash " vhash " failed.")))))

(def ^:dynamic *write-count*)

(defn- write-recursively [node]
  ;; (println "writing to db:")
  ;; (print-variables (vhash node) node)
  (put (vhash node) node)
  (swap! *write-count* inc)
  (when (:left node)
    (write-recursively (lookup (:left node)))
    (write-recursively (lookup (:right node)))))

(defn write-transaction
  "Writes all nodes in transient referenced from transaction roots to disk
returns the root node reference."
  []
  ;; (println "write-transaction")
  ;; (print-variables *current-transaction*)
  (println "heap size: " (count @(:transient *current-transaction*)))
  (doseq [[db root-ref] @(:roots *current-transaction*)]
    (let [root-node (get @(:transient *current-transaction*)
                         root-ref)]
      ;; (print-variables db root-ref root-node)
      (with-db db
        (put :root root-ref)
        (binding [*write-count* (atom 0)]
          (write-recursively root-node)
          (println "wrote " @*write-count* " nodes"))))))

(defmacro transact [& body]
  `(binding [*current-transaction*
             {:roots      (atom {})
              :transient  (atom {})}]
     (let [result# (do ~@body)]
       (write-transaction)
       result#)))