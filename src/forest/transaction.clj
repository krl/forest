(ns forest.transaction
  (:use [forest.db]
        [forest.debug]
        [forest.vhash]))

(def ^:dynamic *current-transaction* nil)

(defn in-transaction? []
  *current-transaction*)

(defn set-root-ref!
  "Sets the root reference for this database, only nodes refered from this will be written to disk."
  [db ref]
  (swap! (:roots *current-transaction*) assoc db ref))

(defn store!
  "Stores nodes in the transaction heap."
  [& nodes]
  (doseq [node nodes]
    (swap! (:heap *current-transaction*)
           assoc
           (vhash node) node)))

(defn lookup
  "Looks up a hash reference, either in the transaction heap or from disk."
  [vhash]
  (or (and *current-transaction*
           (get @(:heap *current-transaction*) vhash))
      (fetch vhash)
      (throw (Exception.
              (str "lookup of vhash " vhash " failed.")))))

(defn- write-recursively 
  "Takes a node and writes it to the database. If the node has children, recurse on them."
  [node]
  (put (vhash node) node)
  (when (:left node)
    (write-recursively (lookup (:left node)))
    (write-recursively (lookup (:right node)))))

(defn write-transaction
  "Writes all nodes in transaction heap referenced from transaction roots to disk
returns the root node reference."
  []
  (doseq [[db root-ref] @(:roots *current-transaction*)]
    (let [root-node (get @(:heap *current-transaction*)
                         root-ref)]
      (with-db db
        (put :root root-ref)
        (write-recursively root-node)))))

(defmacro transact
  "Macro to set up a transaction, only the final value of the top level trees
will be written to disk."
  [& body]
  `(binding [*current-transaction*
             {:roots (atom {})
              :heap  (atom {})}]
     (let [result# (do ~@body)]
       (write-transaction)
       result#)))