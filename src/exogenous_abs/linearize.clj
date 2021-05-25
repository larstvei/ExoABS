(ns exogenous-abs.linearize
  (:require [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [exogenous-abs.abs-traces :as abs-traces]
            [exogenous-abs.atomic-sections :as atomic]
            [exogenous-abs.relations :as relations]))

(def main-cog
  "The main-cog of any ABS-trace is identified by the following value."
  [-1])

(defn atomic-sections-by-node [sections]
  (let [by-sections (group-by :atomic/node sections)]
    (-> (fn [m n] (update m n (partial sort-by :atomic/range)))
        (reduce by-sections (keys by-sections)))))

(defn awaits? [{:keys [:atomic/enables
                       :atomic/disables
                       :atomic/future-id]}]
  (let [await-events (set/union enables disables)
        await-futs (map :abs/stable_id await-events)]
    ((set await-futs) future-id)))

(defn make-phandom-section [{:keys [:atomic/node
                                    :atomic/name
                                    :atomic/future-id
                                    :atomic/uniq-id
                                    :atomic/time]}]
  {:atomic/node node
   :atomic/sync-type :schedule
   :atomic/range [Integer/MAX_VALUE Integer/MAX_VALUE]
   :atomic/name name
   :atomic/time nil
   :atomic/future-id future-id
   :atomic/uniq-id (update uniq-id 3 inc)
   :atomic/creates #{}
   :atomic/resolves #{}
   :atomic/depends-on-create #{}
   :atomic/depends-on-resolve #{}
   :atomic/enables #{}
   :atomic/disables #{}
   :atomic/reads #{}
   :atomic/writes #{}
   :atomic/phantom true})

(defn add-phantom-sections [sections]
  (let [uniq-ids (set (map :atomic/uniq-id sections))
        candidates (filter awaits? sections)
        phantom-sections (->> (map make-phandom-section candidates)
                              (remove (comp uniq-ids :atomic/uniq-id)))]
    (set/union sections phantom-sections)))

(defn init-state [seed-trace sections]
  (let [sections (add-phantom-sections sections)
        sections-by-node (atomic-sections-by-node sections)
        main-block (first (sections-by-node main-cog))]
    {:remaining sections
     :mhb (relations/must-happen-before sections)
     :seen #{}
     :schedule-counters {}
     :disabled #{}
     :blocked-nodes #{}
     :enabled #{main-block}
     :selected :none-selected
     :read-writes {}
     :seed-trace seed-trace}))

(defn update-disabled [{:keys [schedule-counters disabled] :as state}
                       {:keys [:atomic/node :atomic/enables :atomic/disables]}]
  (let [efuts (set (map :abs/stable_id enables))
        removed (set (filter (fn [[_ _ fut _]] (efuts fut)) disabled))
        added (set (for [ev disables
                         :let [fut (:abs/stable_id ev)
                               i (schedule-counters fut)]]
                     [node :schedule fut i]))]
    (-> state
        (update :disabled set/difference removed)
        (update :disabled set/union added))))

(defn is-enabled? [{:keys [mhb seen disabled]}
                   {:keys [:atomic/uniq-id]}]
  (and (empty? (set/difference (mhb uniq-id) seen))
       (not (disabled uniq-id))))

(defn update-enabled [{:keys [remaining] :as state}]
  (->> (set (filter (partial is-enabled? state) remaining))
       (assoc state :enabled)))

(defn next-section [candidates]
  (when-not (empty? candidates)
    (apply min-key (comp first :atomic/range) candidates)))

(defn pick-first [enabled]
  (let [truly-enabled (remove :atomic/phantom enabled)]
    (next-section truly-enabled)))

(defn update-blocked-nodes [{:keys [remaining] :as state}]
  (let [remaining-by-node (group-by :atomic/node remaining)
        blocked (for [[node sections] remaining-by-node
                      :let [s (next-section sections)]
                      :when (= :future_read (:atomic/sync-type s))]
                  node)]
    (assoc state :blocked-nodes (set blocked))))

;;; TODO: Should refactor
(defn update-read-writes [{:keys [read-writes schedule-counters] :as state}
                          {:keys [:atomic/node
                                  :atomic/uniq-id
                                  :atomic/reads
                                  :atomic/writes
                                  :atomic/enables
                                  :atomic/disables]}]
  (let [events (into enables disables)
        new-read-writes (-> (fn [rw event]
                              (let [fut (:abs/stable_id event)
                                    s (schedule-counters fut)
                                    uniq [node :schedule fut s]]
                                (update-in rw [uniq :reads]
                                           (fnil set/union #{})
                                           (set (:abs/reads event)))))
                            (reduce read-writes events))]
    (-> (assoc state :read-writes new-read-writes)
        (update-in [:read-writes uniq-id :reads] (fnil set/union #{}) reads)
        (update-in [:read-writes uniq-id :writes] (fnil set/union #{}) writes))))

(defn choose-section [{:keys [enabled seed-trace]}]
  (if-let [uniq (first seed-trace)]
    (first (filter (comp (partial = uniq) :atomic/uniq-id) enabled))
    (pick-first enabled)))

(defn step [state]
  (let [section (choose-section state)]
    (-> state
        (assoc :selected section)
        (update :seed-trace rest)
        (update-in [:schedule-counters (:atomic/future-id section)] (fnil inc 0))
        (update :seen conj (:atomic/uniq-id section))
        (update :remaining disj section)
        (update-read-writes section)
        (update-blocked-nodes)
        (update-disabled section)
        (update-enabled))))

(defn linearize-sections [seed-trace sections]
  (->> (iterate step (init-state seed-trace sections))
       (take (inc (count sections)))))

(defn expand-section [trace {:keys [:atomic/node :atomic/range]}]
  (mapv #(assoc % :atomic/node node) (apply subvec (trace node) range)))

(defn expand-sections [trace linearized-sections]
  (vec (mapcat (partial expand-section trace) linearized-sections)))

(defn linearization->abs-trace [linearization]
  (-> (fn [t node local-trace]
        (assoc t node (mapv #(dissoc % :atomic/node) local-trace)))
      (reduce-kv {} (group-by :atomic/node linearization))))

(defn uniq-id->event [[node sync-type future-id _]]
  {:abs/type sync-type
   :abs/stable_id future-id})

(defn exo-trace->abs-trace [exo-trace]
  (-> (fn [trace [node & _ :as uniq]]
        (update trace node (fnil conj []) (uniq-id->event uniq)))
      (reduce {} exo-trace)))

(defn sections->exo
  ([sections] (sections->exo () sections))
  ([seed-trace sections]
   (let [steps (linearize-sections seed-trace sections)
         sequential-sections (mapv :selected (rest steps))
         enabled (map (comp set (partial map :atomic/uniq-id) :enabled) steps)
         disabled (map :disabled steps)
         enabled-disabled (mapv (fn [e d] {:enabled e :disabled d}) enabled disabled)
         interference (relations/interference (:read-writes (last steps)))]
     {:trace (mapv :atomic/uniq-id sequential-sections)
      :enabled-disabled enabled-disabled
      :mhb (:mhb (last steps))
      :interference interference})))
