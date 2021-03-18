(ns exogenous-abs.core-test
  (:require [exogenous-abs.abs-traces :as abs-traces]
            [exogenous-abs.atomic-sections :as atomic]
            [exogenous-abs.linearize :as linearize]
            [clojure.test :refer :all]))

(def t1 (abs-traces/json->trace (slurp "resources/abs-models/shared-buffer-naive/trace.json")))
(def t2 (abs-traces/json->trace (slurp "resources/abs-models/photo-video/trace.json")))

(defn trace->seq->trace-is-id [trace]
  (let [sections (atomic/trace->atomic-sections trace)
        steps (linearize/linearize-sections sections)
        sequential-sections (mapv :selected (rest steps))
        sequential-trace (linearize/expand-sections trace sequential-sections)]
    (is (= trace (linearize/linearization->abs-trace sequential-trace)))))

(deftest linearization
  (trace->seq->trace-is-id t1)
  (trace->seq->trace-is-id t2))

