(ns exogenous-abs.abs-traces
  (:require [clojure.spec.alpha :as s]
            [clojure.data.json :as json]
            [exogenous-abs.abs-trace-spec :refer :all]
            [exogenous.relations :as rels]))

;;; The wire-protocol for ABS traces is described by the spec in
;;; exogenous-abs.abs-trace-spec. There is one distinction between the traces
;;; that travel over the wire, and the ones we work with in the functions in
;;; this namespace, which is related to a limitation of JSON.
;;;
;;; A :abs/json-trace travels over the wire is a collection of maps, with an
;;; :abs/id and a :abs/local-trace, as described by :abs/trace. When working
;;; with the traces, it is more practical to let the trace be a map from an
;;; :abs/id to a :abs/local_trace. E.g. a trace
;;;
;;;    [{:abs/id c1 :abs/local_trace t1},
;;;     {:abs/id c2 :abs/local_trace t2},
;;;     ...
;;;     {:abs/id cn :abs/local_trace tn}]
;;;
;;;
;;; is rather represented as a map
;;;
;;;    {c1 t1, c2 t2, ..., cn tn}
;;;
;;; We will refer to maps like this as traces, even though they don't follow
;;; the specification of :abs/trace.

(def main-cog [0])

;;; Enable spec asserts.
(s/check-asserts true)

(defn trace->event-keys
  "Take a trace, and return keys that can be used to retrieve a event in the
  trace."
  [trace]
  (-> (fn [[cog local-trace]]
        (map-indexed (fn [i e] [cog i]) local-trace))
      (mapcat trace)))

(defn event-key->event
  "Take a trace and an event-key and return the event."
  [trace event-key]
  (get-in trace event-key))

(def fut-id
  "A future is identified by the caller- and local id."
  (juxt :abs/caller_id :abs/local_id))

(defn group-schedule-events-by-fut
  "Return a map from a future id to all event keys associated with that future."
  [trace event-keys]
  (->> event-keys
       (filter (comp #(= % :schedule) :abs/type #(event-key->event trace %)))
       (group-by (comp fut-id (partial event-key->event trace)))))

;;; This is really due to a weakness of the traces produced by ABS. We add a
;;; caller to the run-method of active objects. The current cog is (seemingly)
;;; more easily obtained here than during runtime. This should be refactored if
;;; possible.
(defn add-caller-to-run [trace [cog i]]
  (let [event (event-key->event trace [cog i])
        local-id (:abs/local_id event)]
    (if (= :run local-id)
      (assoc-in trace (conj [cog i] :abs/caller_id) cog)
      trace)))

;;; Scheduling events are not necessarily unique, because a method can be
;;; scheduled multiple times (i.e. suspend and reschedule).
(defn add-seq-to-local-trace [local-trace]
  (-> (fn [[t freqs] {:keys [:abs/type] :as event}]
        (if (= type :schedule)
          [(conj t (assoc event :abs/seq (or (freqs (fut-id event)) 0)))
           (update freqs (fut-id event) (fnil inc 0))]
          [(conj t event) freqs]))
      (reduce [[] {}] local-trace)
      first))

(defn add-seq-to-trace [trace]
  (reduce #(update %1 %2 add-seq-to-local-trace) trace (keys trace)))

(defn ditch-empty-traces
  "The runtime emits some local traces that can be empty (usually the main-block
  has a dummy-DC it never interacts with). We can safely remove those."
  [trace]
  (-> (fn [t c l] (if (empty? l) t (assoc t c l)))
      (reduce-kv {} trace)))

(defn augment-trace [trace]
  (->> (trace->event-keys trace)
       (reduce add-caller-to-run trace)
       (add-seq-to-trace)
       (ditch-empty-traces)))

;; (defn get-enabled [trace fut-map fut-status disabled]
;;   (->> (filter (comp (partial = :unresolved) second) fut-status)
;;        (map (comp fut-map first))
;;        (mapcat (partial map (partial event-key->event trace)))
;;        (group-by fut-id)
;;        (map (comp (partial apply min-key :abs/seq) second))
;;        (remove disabled)
;;        (set)))

;; (defn linearize-step [{:keys [trace cog-ptrs
;;                               fut-map fut-status
;;                               disabled enabled
;;                               sequential-trace]
;;                        :as current}]
;;   (let [candidates (filter #(enabled (event-key->event trace %)) cog-ptrs)
;;         [cog i] (first candidates)
;;         next-event (event-key->event trace [cog i])
;;         new-cog-ptrs (if (= i (dec (count (trace cog))))
;;                        (dissoc cog-ptrs cog)
;;                        (assoc cog-ptrs cog (inc i)))]
;;     (-> current
;;         (assoc :cog-ptrs new-cog-ptrs))))

;; (defn linearize [trace]
;;   (let [event-keys (trace->event-keys trace)
;;         cog-ptrs (reduce #(assoc %1 %2 0) {} (keys trace))
;;         fut-map (group-schedule-events-by-fut trace event-keys)
;;         fut-status {[:undefined :main] :unresolved}
;;         disabled #{}
;;         enabled (get-enabled trace fut-map fut-status disabled)
;;         sequential-trace []]
;;     (->> {:trace trace :cog-ptrs cog-ptrs
;;           :fut-map fut-map :fut-status fut-status
;;           :enabled enabled :disabled disabled
;;           :abs/sequential-trace sequential-trace}
;;          (iterate linearize-step)
;;          (take (reduce + (map count (vals trace))))
;;          doall)))

(defn abstract-event [event]
  (select-keys event [:abs/type :abs/caller_id :abs/local_id :abs/seq]))

(defn add-global-dep [mhb {:keys [:abs/type :abs/name :abs/seq] :as event}]
  (let [add-to-mhb (partial rels/relate mhb)
        ev (abstract-event event)]
    (cond
      ;; Special case for main-block, which does not depend on anything
      (or (and (= (:abs/local_id event) :main) (= type :schedule) (zero? seq))
          (and (= (:abs/local_id event) :run) (= type :schedule) (zero? seq)))
      mhb

      ;; Special case for the init-block, which depends on object creation
      (and (= type :schedule) (= name :init))
      (-> (dissoc ev :abs/seq)
          (assoc :abs/type :new_object)
          (add-to-mhb ev))

      ;; First schedule is dependent on an invoc
      (and (= type :schedule) (zero? seq))
      (-> (dissoc ev :abs/seq)
          (assoc :abs/type :invocation)
          (add-to-mhb ev))

      ;; Other scheduling-events are dependent on the previous
      (= type :schedule)
      (add-to-mhb (update ev :abs/seq dec) ev)

      ;; Future reads wait for future writes
      (= type :future_read)
      (add-to-mhb (assoc ev :abs/type :future_write) ev)

      ;; Awaiting futures wait for future writes
      (= type :await_future)
      (add-to-mhb (assoc ev :abs/type :future_write) ev)

      :else mhb)))

(defn add-seq-dep [mhb [e1 e2]]
  (if (or (s/valid? :abs/dc-event e1)
          (s/valid? :abs/dc-event e2)
          (= (:abs/type e2) :schedule))
    mhb
    (rels/relate mhb (abstract-event e1) (abstract-event e2))))

(defn atomic-blocks [local-trace]
  (partition-by ))

(defn add-local-dep [mhb cog local-trace]
  (if (and (s/valid? :abs/cog-local-trace local-trace)
           (not= cog main-cog))
    (let [init-ends (second local-trace)
          init-deps (map vector (repeat init-ends) (drop 2 local-trace))
          blocks (atomic-blocks local-trace)]
      (->> 
       (reduce-kv (fn [mhb init ev] (rels/relate mhb init ev)) mhb)))
    mhb))

(defn gen-mhb [trace]
  (let [events (mapcat second trace)
        global-mhb (reduce add-global-dep {} events)]
    (reduce-kv add-local-dep global-mhb trace)))

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
  [trace {:keys [:abs/id :abs/local_trace]}]
  (assoc trace id local_trace))

(defn json->trace
  "Take json as input, and return a mapping from cog/DC identifiers to their
  local trace."
  [json]
  (->> (json->clj json)
       (s/assert :abs/json-trace)
       (reduce add-cog {})
       (s/assert :abs/trace)
       (augment-trace)
       (s/assert :abs/trace)))

(def t (json->trace (slurp "resources/abs-models/shared-buffer-naive/trace.json")))
