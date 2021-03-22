(ns exogenous-abs.abs-traces
  (:require [clojure.data.json :as json]
            [clojure.spec.alpha :as s]))

;;; Enable spec asserts.
(s/check-asserts true)

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
                   (assoc-in trace [node i :abs/caller_id] node)
                   trace)))
             trace (range (count local-trace))))
   trace trace))

(defn remove-caller-from-run [local-trace]
  (-> (fn [{:keys [:abs/local_id] :as event}]
        (if (= local_id :run)
          (assoc event :abs/caller_id :undefined)
          event))
      (mapv local-trace)))

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

(defn trace->json [trace]
  (-> (fn [json-trace node local-trace]
        (let [new-local (remove-caller-from-run local-trace)]
          (conj json-trace {:abs/node node :abs/local_trace new-local})))
      (reduce-kv [] trace)
      (json/write-str)))
