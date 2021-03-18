(ns exogenous-abs.linearize
  (:require [clojure.set :as set]
            [exogenous-abs.relations :refer [must-happen-before]]))

(def main-cog
  "The main-cog of any ABS-trace is identified by the following value."
  [0])

(defn atomic-sections-by-node [sections]
  (let [by-sections (group-by :atomic/node sections)]
    (-> (fn [m n] (update m n (partial sort-by :atomic/range)))
        (reduce by-sections (keys by-sections)))))

(defn init-state [sections]
  (let [sections-by-node (atomic-sections-by-node sections)
        main-block (first (sections-by-node main-cog))]
    {:remaining sections
     :mhb (must-happen-before sections)
     :seen #{}
     :disabled #{}
     :enabled #{main-block}
     :selected :none-selected}))

(defn update-disabled [state {:keys [:atomic/enables :atomic/disables]}]
  (-> state
      (update :disabled set/difference enables)
      (update :disabled set/union disables)))

(defn update-enabled [{:keys [remaining mhb seen disabled] :as state}]
  (let [is-enabled? (fn [s] (empty? (set/difference (mhb s) seen)))
        candidates (set (filter is-enabled? remaining))]
    (assoc state :enabled (set/difference candidates disabled))))

(defn choose-section [{:keys [enabled]}]
  (apply min-key (comp first :atomic/range) enabled))

(defn step [state]
  (let [section (choose-section state)]
    (-> state
        (assoc :selected section)
        (update :seen conj section)
        (update :remaining disj section)
        (update-disabled section)
        (update-enabled))))

(defn linearize-sections [sections]
  (->> (iterate step (init-state sections))
       (take (inc (count sections)))))

(defn expand-section [trace {:keys [:atomic/node :atomic/range]}]
  (mapv #(assoc % :atomic/node node) (apply subvec (trace node) range)))

(defn expand-sections [trace linearized-sections]
  (vec (mapcat (partial expand-section trace) linearized-sections)))

(defn linearization->abs-trace [linearization]
  (-> (fn [t node local-trace]
        (assoc t node (mapv #(dissoc % :atomic/node) local-trace)))
      (reduce-kv {} (group-by :atomic/node linearization))))
