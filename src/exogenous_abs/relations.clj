(ns exogenous-abs.relations
  (:require [clojure.set :as set]
            [exogenous-abs.atomic-sections :refer [fut-id]]
            [exogenous.relations :as rels]))

(defn causal-dependency? [{:keys [:atomic/creates :atomic/resolves]}
                          {:keys [:atomic/depends-on-create :atomic/depends-on-resolve]}]
  (seq (set/union (set/intersection creates depends-on-create)
                  (set/intersection resolves depends-on-resolve))))

(defn local-dependency? [{node1 :atomic/node [i _] :atomic/range :as s1}
                         {node2 :atomic/node [j _] :atomic/range :as s2}]
  (and (= node1 node2) (< i j)
       (or (= (fut-id s1) (fut-id s2))
           (= :init (:atomic/name s1)))))

(defn time-dependency? [{t1 :atomic/time} {t2 :atomic/time}]
  (< t1 t2))

(defn must-happen-before [sections]
  (-> (for [s1 sections
            s2 sections
            :when (or (causal-dependency? s1 s2)
                      (local-dependency? s1 s2)
                      (time-dependency? s1 s2))]
        [s1 s2])
      (rels/pairs->rel)))

(defn interference [sections]
  (-> (for [{node1 :atomic/node r1 :atomic/reads w1 :atomic/writes :as s1} sections
            {node2 :atomic/node r2 :atomic/reads w2 :atomic/writes :as s2} sections
            :when (and (= node1 node2)
                       (not= s1 s2)
                       (or (seq (set/intersection r1 w2))
                           (seq (set/intersection w1 r2))
                           (seq (set/intersection w1 w2))))]
        [s1 s2])
      (rels/pairs->rel)))
