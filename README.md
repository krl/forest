

Forest is a set of on-disk persistent datastructures.

The objects we work with are typed references with a hash-id.

(def x (DiskMap. {}))

(def new-map 
		 (transact x
		   (assoc :lol 3)
		   (assoc :lol 3)

The empty map is hash0, an assoc :lol 3 to this map produces the diskmap equal to
{:lol 3}. This is internally represented as a tree. In order to lookup keys in very large maps, only log(n) nodes will have to be touched.

one leaf of the DiskMap tree contains up to N keys, ordered by the hash of the key.
when the keys overflow

DiskMapLeaf

key-values
a bucket of max BUCKET_MAX_SIZE size.
([key1 val1] [key2 val2])

DiskMapNode
cut   : hash value that cuts the children in half
left  : reference to left child
right : reference to right child

