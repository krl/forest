# Forest

On-disk persistent functional datastructures for Clojure.

Uses a key-value store as backend, storing sorted sequences as red-black trees. Any previous state of the datastructures can be looked up in Olog(n) time, every association or dissociation returns a new top-level map, the old version can still be freely referenced. Basically just like the standard Clojure datastructures.

Check out the tests for examples on how it works.

# Example

From ns forest.root-test

```clojure
(binding [forest.redblack/*bucket-size* 1]
    (let [path         (str "/tmp/forest-test-" (rand))
          testroot     (root path (diskseq identity))
          ref1         (conjoin testroot 1234)
          ref2         (conjoin ref1 1312)
          ref2-written (write ref2)]
      ;; same result of written and open.
      (is (= (range-of ref2 nil nil)
             (range-of ref2-written nil nil)))
      ;; old value still accesible
      (is (= (range-of ref1 nil nil) (list 1234)))
      (is (= (range-of ref2 nil nil) (list 1234 1312)))
      ;; setup a new reference to the same datastructure        
      (let [testroot2 (root path :ignored)]
        (is (= (range-of testroot2 nil nil)
               (range-of ref2 nil nil))))))
```

# Red Black Tree implementation

Implementation and balancing function based on Okasakis implementaion in 'Purely Functional Data Structures' with some additional side-effect magic storing the nodes on disk when written, and only in memory when not.

# TODO

* The trees (like Okasakis) are insert-only for now, will look at smk's implementaiton of removal.
* HAMTs for sets and maps.