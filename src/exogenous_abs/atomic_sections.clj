(ns exogenous-abs.atomic-sections
  (:require [clojure.spec.alpha :as s]))

;;; When working with traces, we apply an abstraction, capturing all the
;;; behavior of an /atomic section/ in a single structure. An atomic section is
;;; a section with no synchronization, i.e. the events between release points
;;; or future reads.

;;; A node is a cog or DC
(s/def :atomic/node :abs/node)

;;; Synchronization occurs with the following event types.
(s/def :atomic/sync-type #{:schedule :future_read :cpu :bw :memory})

;;; The range specifies the begin- and end indices of the section in the local
;;; trace of the node.
(s/def :atomic/range (s/tuple nat-int? nat-int?))

;;; We inherit the name of the first event of the section (used for debugging).
(s/def :atomic/name :abs/name)

;;; Get the time of the synchronization point.
(s/def :atomic/time :abs/time)

;;; All atomic sections are identified by a future.
(s/def :atomic/future-id (s/tuple :abs/caller_id :abs/local_id))

;; An atomic section may create multiple futures.
(s/def :atomic/creates (s/coll-of :atomic/future-id :kind set?))

;; An atomic section can resolve a single future.
(s/def :atomic/resolves (s/coll-of :atomic/future-id :kind set? :max-count 1))

;; An atomic section can depend on the creation of a single future. The main
;; block is an exception.
(s/def :atomic/depends-on-create (s/coll-of :atomic/future-id :kind set? :max-count 1))

;; An atomic section may depend on multiple futures being resolved.
(s/def :atomic/depends-on-resolve (s/coll-of :atomic/future-id :kind set?))

;;; An atomic section may enable multiple boolean guards.
(s/def :atomic/enables (s/coll-of :atomic/future-id :kind set?))

;;; An atomic section may disable multiple boolean guards.
(s/def :atomic/disables (s/coll-of :atomic/future-id :kind set?))

;;; An atomic section inherits the reads from the events of the section.
(s/def :atomic/reads :abs/reads)

;;; An atomic section inherits the writes from the events of the section.
(s/def :atomic/writes :abs/writes)

;;; An atomic section is a map with the keys specified above.
(s/def :atomic/section (s/keys :req [:atomic/node
                                     :atomic/sync-type
                                     :atomic/range
                                     :atomic/name
                                     :atomic/time
                                     :atomic/future-id
                                     :atomic/creates
                                     :atomic/resolves
                                     :atomic/depends-on-create
                                     :atomic/depends-on-resolve
                                     :atomic/enables
                                     :atomic/disables
                                     :atomic/reads
                                     :atomic/writes]))

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
          (->> [(:atomic/node init-block) :run]
               (update init-block :atomic/creates conj)
               (conj (rest sections))))
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