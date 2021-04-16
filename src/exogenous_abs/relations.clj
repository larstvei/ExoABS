(ns exogenous-abs.relations
  (:require [clojure.set :as set]
            [exogenous.relations :as rels]))

(defn causal-dependency? [{:keys [:atomic/creates
                                  :atomic/resolves]}
                          {:keys [:atomic/depends-on-create
                                  :atomic/depends-on-resolve]}]
  (seq (set/union (set/intersection creates depends-on-create)
                  (set/intersection resolves depends-on-resolve))))

(defn local-dependency? [s1 s2]
  (and (= (:atomic/node s1)
          (:atomic/node s2))
       (< (first (:atomic/range s1))
          (first (:atomic/range s2)))
       (or (and (not= :schedule (:atomic/sync-type s1))
                (not= :schedule (:atomic/sync-type s2)))
           (= (:atomic/future-id s1)
              (:atomic/future-id s2))
           (= :init (:atomic/name s1)))))

(defn time-dependency? [s1 s2]
  (when-not (or (:atomic/phantom s1) (:atomic/phantom s2))
    (< (:atomic/time s1) (:atomic/time s2))))

(defn must-happen-before [sections]
  (-> (for [s1 sections
            s2 sections
            :when (or (causal-dependency? s1 s2)
                      (local-dependency? s1 s2)
                      (time-dependency? s1 s2))]
        [(:atomic/uniq-id s1) (:atomic/uniq-id s2)])
      (rels/pairs->rel)))

(defn add-disabled [mhb disabled]
  (-> (for [uniq disabled] [(update uniq 3 dec) uniq])
      (rels/pairs->rel)
      (rels/rel-union mhb)))

(defn interference [read-writes]
  (set (for [[node1 type1 fut1 seq1 :as u1] (keys read-writes)
             [node2 type2 fut2 seq2 :as u2] (keys read-writes)
             :let [{r1 :reads w1 :writes} (read-writes u1)
                   {r2 :reads w2 :writes} (read-writes u2)]
             :when (and (= type1 :schedule)
                        (= type2 :schedule)
                        (= node1 node2)
                        (not= u1 u2)
                        (or (seq (set/intersection r1 w2))
                            (seq (set/intersection w1 r2))
                            (seq (set/intersection w1 w2))))]
         [u1 u2])))

#_(defn interference [sections]
    (-> (for [{r1 :atomic/reads w1 :atomic/writes :as s1} sections
              {r2 :atomic/reads w2 :atomic/writes :as s2} sections
              :when (and (= (:atomic/sync-type s1) :schedule)
                         (= (:atomic/sync-type s2) :schedule)
                         (= (:atomic/node s1) (:atomic/node s2))
                         (not= s1 s2)
                         (or (seq (set/intersection r1 w2))
                             (seq (set/intersection w1 r2))
                             (seq (set/intersection w1 w2))))]
          [(:atomic/uniq-id s1) (:atomic/uniq-id s2)])
        (rels/pairs->rel)))
