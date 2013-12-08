(ns forest.visualize
  (:use [quil.core]
        [forest.debug]
        [forest.store]
        [forest.root]))

(defn setup []
  (smooth)
  (frame-rate 2)
  (stroke 0)
  (background 255 255 255))

(defn draw-thing [store thing x y width]
  (cond (= (str (type thing)) 
           (str forest.redblack.RedBlackNode))
        (do
          (if (= (:color thing) :red)
            (fill 200 0 0)
            (fill 0 0 0))
          (ellipse x y 32 32)

          (text-align :center)
          (fill 255)
          (text (str (number-of-elements thing)) x y)

          (draw-thing store
                      (lookup store (:left thing))
                      (- x (/ width 2)) (+ y 48)
                      (/ width 2))
          (draw-thing store
                      (lookup store (:right thing))
                      (+ x (/ width 2)) (+ y 48)
                      (/ width 2)))

        (= (str (type thing))
           (str forest.redblack.RedBlackLeaf))
        (do
          (fill 0)
          (rect (- x 16) (- y 16) 32 32)
          (text-align :center)
          (fill 255)
          (text (str (number-of-elements thing))
                x y))
        :else
        (do
          (fill 240 0 0)
          (rect (- x 12) (- y 16) 24 32))))

(defn visualize-atom [root-atom]
  (let [width 600]
    (sketch
     :title "Oh so many grey circles"
     :setup setup
     :size [width 800]
     :draw (fn []           
             (let [root @root-atom]
               (background 255 255 255)  
               (draw-thing (:store root)
                           (:value (:value root))
                           (/ width 2) 30 (/ width 2)))))))
