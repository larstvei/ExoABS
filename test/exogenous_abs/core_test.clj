(ns exogenous-abs.core-test
  (:require [exogenous-abs.abs-traces :as abs-traces]
            [exogenous-abs.atomic-sections :as atomic]
            [exogenous-abs.linearize :as linearize]
            [clojure.test :refer :all]))

(def json-trace1 (slurp "resources/abs-models/shared-buffer-naive/trace.json"))
(def json-trace2 (slurp "resources/abs-models/photo-video/trace.json"))

(def t1 (abs-traces/json->trace json-trace1))
(def t2 (abs-traces/json->trace json-trace2))

(deftest conversion
  ;; Node that we don't test if we produce the same JSON. Rather we test if
  ;; converting the trace to JSON, and convert back again to a trace, then we
  ;; do get the same trace back.
  (is (= t1 (abs-traces/json->trace (abs-traces/trace->json t1))))
  (is (= t2 (abs-traces/json->trace (abs-traces/trace->json t2)))))

(defn trace->seq->trace-is-id [trace]
  (let [sections (atomic/trace->atomic-sections trace)
        steps (linearize/linearize-sections sections)
        sequential-sections (mapv :selected (rest steps))
        sequential-trace (linearize/expand-sections trace sequential-sections)]
    (is (= trace (linearize/linearization->abs-trace sequential-trace)))))

(deftest linearization
  (trace->seq->trace-is-id t1)
  (trace->seq->trace-is-id t2))

