# Forest

On-disk persistent functional datastructures for Clojure.

Uses a key-value store as backend, storing maps as hash-trees. Any historical state of the datastructures can be looked up in Olog(n) time, every association or dissociation returns a new top-level map, the old version can still be freely referenced. Just like Clojure datastructures.

Check out the tests for how it works.

# Example

From ns forest.hashtree-test

    (deftest keep-reference
      (let [toplevel   (diskmap (random-path))
            first-ref  (transact
                         (associate toplevel :a 1))
            second-ref (transact
                         (associate first-ref :a 2))]
        (is (= (get-key first-ref  :a) 1))
        (is (= (get-key second-ref :a) 2))))

# TODO

* Red-black trees for sorted sequences.
* Have the top-level map keep such a sequence of its prior states.
