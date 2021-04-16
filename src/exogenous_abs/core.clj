(ns exogenous-abs.core
  (:gen-class :main true)
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.java.shell :refer [sh]]
            [clojure.set :refer [difference]]
            [clojure.string :as s]
            [clojure.tools.cli :refer [parse-opts]]
            [exogenous.core :as exo]
            [exogenous.relations :as rels]
            [exogenous-abs.model-interaction :as model]))

(defn run-exploration [model options]
  (exo/informed-explore (model/simulate model) options))

(def cli-options
  [["-w" "--workers" "Number of worker threads"
    :default (* 2 (.availableProcessors
                   (Runtime/getRuntime)))
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 20000) "Must be a number between 0 and 20000"]]

   ["-b" "--backtracking" "The backtracking set used"
    :default :backsets
    :parse-fn keyword
    :validate [#{:backsets :sleep-only :naive}
               "Must be 'breadth-first', 'depth-first' or 'random'"]]

   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["Usage: exo [options] path/to/gen/erl/run"
        ""
        "Options:"
        options-summary]
       (s/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (s/join \newline errors)))

(defn validate-args [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options)
      {:exit-message (usage summary) :ok? true}
      errors
      {:exit-message (error-msg errors)}
      (and (= 1 (count arguments))
           (let [file (clojure.java.io/file (first arguments))]
             (and (.exists file) (.canExecute file))))
      {:model (first arguments) :options options}
      :else {:exit-message (usage summary)})))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [model options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (do
        (clojure.pprint/pprint (run-exploration model options))
        (exit 1 "")))))

(comment
  (run-exploration "/Users/larstvei/Dropbox/ifi/phd/abs/dpor-tests/gen/erl/run" {})
  (run-exploration "/Users/larstvei/Dropbox/ifi/phd/abs/trace-tests/gen/erl/run" {})
  (run-exploration "/Users/larstvei/Dropbox/ifi/phd/abs/shared-buffer/naive/gen/erl/run" {})
  (run-exploration "/Users/larstvei/Dropbox/ifi/phd/abs/martimj-master/thesis/models/gen/erl/run" {})
  (run-exploration "/Users/larstvei/Dropbox/ifi/phd/papers/jlamp-exogenous/implementation/gen/erl/run" {}))
