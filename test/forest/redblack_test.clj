(ns forest.redblack-test
  (:use     [forest.debug]
            [forest.root]
            [forest.transaction])
  (:require [clojure.test :refer :all]
            [forest.hashtree :refer :all]
            [platt.core :as platt]))