# Forest

On-disk persistent functional datastructures for Clojure.

Uses a key-value store as backend, storing sorted sequences as red-black trees. Any previous state of the datastructures can be looked up in Olog(n) time, every association or dissociation returns a new top-level map, the old version can still be freely referenced. Basically just like the standard Clojure datastructures.

Check out the tests for examples on how it works.

Uses 2-3 Trees for sorted sequences, and Hash array mapped tries for sets and maps

Branching factor of 16 for now, but twiddlable. 

