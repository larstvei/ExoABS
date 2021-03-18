(ns exogenous-abs.abs-trace-spec
  (:require [clojure.spec.alpha :as s]))

;;; The trace of a cog can emit events of the following type.
(s/def :abs/cog-type #{:schedule
                       :invocation
                       :new_object
                       :suspend
                       :await_future
                       :future_read
                       :future_write
                       :await_disable
                       :await_enable
                       :resource})

;;; The trace of a deployment component can emit events of the following type.
(s/def :abs/dc-type #{:cpu :bw :memory})

;;; The type of an event must fall in either category.
(s/def :abs/type (s/or :cog-type :abs/cog-type
                       :dc-type :abs/dc-type))

;;; Generally, a caller ID is given as a list of nats. The exception is the
;;; main and run methods, which are left undefined.
(s/def :abs/caller_id (s/or :generally (s/coll-of nat-int? :kind vector?)
                            :main-or-run #{:undefined}))

;;; Generally, a local ID is given by a nat. The exception is the main and run
;;; methods, which are identified by name.
(s/def :abs/local_id (s/or :generally nat-int?
                           :main-or-run #{:main :run}))

;;; A name is simply a keyword.
(s/def :abs/name keyword?)

;;; Both reads and writes are given as a sequence of keywords.
(s/def :abs/reads (s/coll-of string?))
(s/def :abs/writes (s/coll-of string?))

;;; Time is given as a non-negative number.
(s/def :abs/time (complement neg?))

;;; The amount (which refers to the resources provided by a deployment
;;; component) is given as a non-negative number.
(s/def :abs/amount (complement neg?))

;;; The event on a cog requires the following fields, with values determined by
;;; their respective specs.
(s/def :abs/cog-event (s/keys :req [:abs/type
                                    :abs/caller_id
                                    :abs/local_id
                                    :abs/name
                                    :abs/reads
                                    :abs/writes
                                    :abs/time]))

;;; The event on a DC requires the following fields, with values determined by
;;; their respective specs.
(s/def :abs/dc-event (s/keys :req [:abs/type
                                   :abs/caller_id
                                   :abs/local_id
                                   :abs/amount
                                   :abs/time]))

;;; A cog-local trace is a vector of cog-events.
(s/def :abs/cog-local-trace (s/coll-of :abs/cog-event :kind vector?))
;;; A dc-local trace is a vector of dc-events.
(s/def :abs/dc-local-trace (s/coll-of :abs/dc-event :kind vector?))

;;; A local trace is either a cog-local trace or a dc-local trace.
(s/def :abs/local_trace (s/or :cog-local-trace :abs/cog-local-trace
                              :dc-local-trace :abs/dc-local-trace))

;;; A trace belongs to either a cog or a DC, identified by a vector of
;;; integers. We refer to a cog or DC as a node, in a context where we do not
;;; wish to distinguish between them.
(s/def :abs/node (s/coll-of int? :kind vector?))

;;; An entry in a JSON collection is a map containing an ID to a cog or DC and
;;; a local trace.
(s/def :abs/json-entry (s/keys :req [:abs/node :abs/local_trace]))

;;; A JSON trace is a collection of local traces, owned by an identifiable cog
;;; or DC.
(s/def :abs/json-trace (s/coll-of :abs/json-entry))

;;; A trace is a mapping from a cog/DC to a local trace.
(s/def :abs/trace (s/map-of :abs/node :abs/local_trace))

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
