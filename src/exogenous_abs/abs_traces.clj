(ns exogenous-abs.abs-traces
  (:require [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [exogenous-abs.abs-trace-spec :refer :all]
            [exogenous.relations :as rels]))

;;; Enable spec asserts.
(s/check-asserts true)

(def main-cog
  "The main-cog of any ABS-trace is identified by the following value."
  [0])

(def fut-id
  "A future is identified by the caller- and local id."
  (juxt :abs/caller_id :abs/local_id))

(defn synchronization-point? [{:keys [:abs/type]}]
  (#{:schedule :future_read :cpu :bw :memory} type))

(defn fut-ids-for-types [by-type types]
  (->> (select-keys by-type types)
       (mapcat val)
       (map fut-id)
       (into #{})))

(defn make-atomic-section [node local-trace [beg end]]
  (let [{:keys [:abs/type :abs/time :abs/name] :as sync-event} (local-trace beg)
        atomic-section (subvec local-trace beg end)
        future (fut-id sync-event)
        by-type (group-by :abs/type atomic-section)
        resolves (fut-ids-for-types by-type [:future_write :cpu :bw :memory])
        creates (fut-ids-for-types by-type [:invocation :new_object :resource])
        depends-on-create (if (= type :future_read) #{} #{future})
        depends-on-resolve (fut-ids-for-types by-type [:future_read :await_future])]
    {:atomic/node node
     :atomic/sync-type type
     :atomic/range [beg end]
     :atomic/name name
     :atomic/time time
     :atomic/future-id future
     :atomic/creates creates
     :atomic/resolves resolves
     :atomic/depends-on-create depends-on-create
     :atomic/depends-on-resolve depends-on-resolve
     :atomic/enables (fut-ids-for-types by-type [:await_enable])
     :atomic/disables (fut-ids-for-types by-type [:await_disable])
     :atomic/reads (reduce into #{} (map :abs/reads atomic-section))
     :atomic/writes (reduce into #{} (map :abs/writes atomic-section))}))

;;; TODO: Very refactor
(defn add-special-cases [sections]
  (let [has-run (filter (comp (partial = :run) second :atomic/future-id) sections)
        init-block (first sections)]
    (if-not (empty? has-run)
      (do (assert (= :init (:atomic/name init-block)))
          (conj (rest sections) (update init-block :atomic/creates conj [(:atomic/node init-block) :run])))
      sections)))

(defn make-local-atomic-sections [[node local-trace]]
  (let [n (count local-trace)
        at-sync (fn [i e] (when (synchronization-point? e) i))
        indices (keep-indexed at-sync local-trace)
        ranges (map vector indices (concat (rest indices) [n]))
        sections (map (partial make-atomic-section node local-trace) ranges)]
    (add-special-cases sections)))

(defn trace->atomic-sections [trace]
  (into #{} (mapcat make-local-atomic-sections trace)))

(defn atomic-sections-by-node [sections]
  (let [by-sections (group-by :atomic/node sections)]
    (-> (fn [m n] (update m n (partial sort-by :atomic/range)))
        (reduce by-sections (keys by-sections)))))

(defn causal-dependency? [{:keys [:atomic/creates :atomic/resolves]}
                          {:keys [:atomic/depends-on-create
                                  :atomic/depends-on-resolve]}]
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

(defn sequential-trace->trace [sequential-trace]
  (-> (fn [t node local-trace]
        (assoc t node (mapv #(dissoc % :atomic/node) local-trace)))
      (reduce-kv {} (group-by :atomic/node sequential-trace))))

;;; This is really due to a weakness of the traces produced by ABS. We add a
;;; caller to the run-method of active objects. The current cog is (seemingly)
;;; more easily obtained here than during runtime. This should be refactored if
;;; possible.
(defn add-caller-to-run [trace]
  (reduce-kv
   (fn [trace node local-trace]
     (reduce (fn [trace i]
               (let [{:keys [:abs/local_id]} (get-in trace [node i])]
                 (if (= :run local_id)
                   (assoc-in trace (conj [node i] :abs/caller_id) node)
                   trace)))
             trace (range (count local-trace))))
   trace trace))

(defn ditch-empty-traces
  "The runtime emits some local traces that can be empty (usually the main-block
  has a dummy-DC it never interacts with). We can safely remove those."
  [trace]
  (-> (fn [t c l] (if (empty? l) t (assoc t c l)))
      (reduce-kv {} trace)))

(defn json->clj
  "Take a string and return a clojure data structure. Make all keys into
  namespaced keywords (e.g. :abs/local_trace) and all string values
  non-namespaced keywords (e.g. :schedule)."
  [json]
  (json/read-str
   json
   :key-fn #(keyword "abs" %)
   :value-fn #(if (string? %2) (keyword %2) %2)))

(defn add-cog
  "Takes a :abs/trace and a :abs/json-entry and adds it to the trace."
  [trace {:keys [:abs/node :abs/local_trace]}]
  (assoc trace node local_trace))

(defn json->trace
  "Take json as input, and return a mapping from cog/DC identifiers to their
  local trace."
  [json]
  (->> (json->clj json)
       (s/assert :abs/json-trace)
       (reduce add-cog {})
       (s/assert :abs/trace)
       (ditch-empty-traces)
       (s/assert :abs/trace)
       (add-caller-to-run)
       (s/assert :abs/trace)))

(def t1 (json->trace (slurp "resources/abs-models/shared-buffer-naive/trace.json")))
(def t2 (json->trace (slurp "resources/abs-models/photo-video/trace.json")))
