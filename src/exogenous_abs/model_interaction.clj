(ns exogenous-abs.model-interaction
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [exogenous-abs.abs-traces :as abs-traces]
            [exogenous-abs.atomic-sections :as atomic]
            [exogenous-abs.linearize :as linearize]
            [exogenous-abs.relations :as relations]))

(defn make-directory-structure! [model]
  (let [run-executable (io/file model)
        parent-dir (.getParent (io/file (.getParent run-executable)))
        ;; Main directory for exo-files
        exo-dir (str parent-dir "/exo")
        ;; Seed traces goes here
        seed-dir (str exo-dir "/seeds")
        ;; Model output (i.e. stdout) goes here
        output-dir (str exo-dir "/output")
        ;; Model errors (i.e. stderr) goes here
        error-dir (str exo-dir "/error")
        ;; Explored traces goes here
        explored-dir (str exo-dir "/explored")]
    (doseq [dir [exo-dir seed-dir output-dir error-dir explored-dir]]
      (.mkdir (io/file dir)))
    {:exo-dir exo-dir
     :seed-dir seed-dir
     :output-dir output-dir
     :error-dir error-dir
     :explored-dir explored-dir
     :next-trace-id (atom 0)}))

(defn new-file-names [{:keys [seed-dir output-dir error-dir explored-dir next-trace-id]}]
  (let [trace-id (swap! next-trace-id inc)]
    {:seed-file (str seed-dir "/" trace-id ".json")
     :output-file (str output-dir "/" trace-id ".out")
     :error-file (str error-dir "/" trace-id ".out")
     :explored-file (str explored-dir "/" trace-id ".json")}))

(defn simulate [model]
  (let [dirs (make-directory-structure! model)]
    (fn [seed-trace]
      (let [files (new-file-names dirs)]
        ;; Create the JSON seed trace and write it to file
        (->> seed-trace
             linearize/exo-trace->abs-trace
             abs-traces/trace->json
             (spit (:seed-file files)))
        ;; Run the model
        (let [{:keys [exit out err]}
              (sh model
                  "--replay" (:seed-file files)
                  "--record" (:explored-file files))]
          (when-not (zero? exit)
            (println "Model terminated abnormally."))
          (when-not (empty? out)
            (spit (:output-file files) out))
          (when-not (empty? err)
            (spit (:error-file files) err)))
        (let [json-trace (slurp (:explored-file files))
              raw-trace (abs-traces/json->trace json-trace)
              sections (atomic/trace->atomic-sections raw-trace)
              result (linearize/sections->exo seed-trace sections)]
          (assert (= seed-trace (subvec (:trace result) 0 (count seed-trace))))
          result)))))

(comment
  (exogenous-abs.core/run-exploration "resources/abs-models/simple/gen/erl/run" {:workers 1}))
