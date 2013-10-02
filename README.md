# Forest

On-disk persistent functional datastructures for Clojure.

Uses a key-value store as backend, storing maps as hash-trees. Any historical state of the datastructures can be looked up in Olog(n) time, every association or dissociation returns a new top-level map, the old version can still be freely referenced. Basically just like the standard Clojure datastructures.

Check out the tests for examples on how it works.

# Example

From ns forest.hashtree-test

```clojure
(deftest keep-reference
  (transact
    (let [toplevel   (get-test-root)
          first-ref  (associate toplevel :a 1)
          second-ref (associate first-ref :a 2)]
      (is (= (get-key first-ref  :a) 1))
      (is (= (get-key second-ref :a) 2)))))
```

# Still working on

* Red-black trees for sorted sequences.
