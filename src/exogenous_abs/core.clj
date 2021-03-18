(ns exogenous-abs.core
  (:gen-class :main true)
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.java.shell :refer [sh]]
            [clojure.set :refer [difference]]
            [clojure.string :as s]
            [clojure.tools.cli :refer [parse-opts]]
            [exogenous.core :as exo]
            [exogenous.relations :as rels]))

(defn linerize-trace [seed-trace domain mhb]
  (let [local-occurs-before (for [[i [_ cog1 & _ :as e1]] domain
                                  [j [_ cog2 & _ :as e2]] domain
                                  :when (and (= cog1 cog2) (< i j))]
                              [e1 e2])
        strict-hb (-> (into mhb local-occurs-before)
                      rels/pairs->rel
                      rels/transitive-closure)
        domain (set (remove #(= (% 0) "future_read") (map second domain)))
        domain (difference domain (set seed-trace))]
    (rels/linerize domain strict-hb seed-trace)))

(defn simulate [model chan]
  (fn [seed-trace]
    (let [input-for-simulator (-> {:trace seed-trace} json/write-str)
          outputraw (:out (sh model "--exo" :in input-for-simulator))
          [pre output json] (s/split outputraw #"ExoJSON: ")
          {:keys [domain mhb interference]}
          (json/read-str json :key-fn keyword :value-fn (fn [_ v] (set v)))
          out-trace (linerize-trace seed-trace domain mhb)]
      (async/go (async/>! chan output))
      {:trace out-trace :mhb mhb :interference interference})))

(defn run-exploration [model options]
  (let [chan (async/chan)]
    ;; I'm not sure this loop terminates.
    (async/go-loop []
      (when-let [output (async/<! chan)]
        (println "-------------------------------------------------------")
        (println output)
        (recur)))
    (prn (exo/informed-explore (simulate model chan) options))
    (async/close! chan)))

(def cli-options
  [["-w" "--workers" "Number of worker threads"
    :default (* 2 (.availableProcessors
                   (Runtime/getRuntime)))
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 20000) "Must be a number between 0 and 20000"]]

   ["-s" "--strategy" "The search strategy applied"
    :default :random
    :parse-fn keyword
    :validate [#{:breadth-first :depth-first :random}
               "Must be 'breadth-first', 'depth-first' or 'random'"]]

   ["-b" "--backtracking" "The backtracking set used"
    :default :backsets
    :parse-fn keyword
    :validate [#{:backsets :sleep-only :naive}
               "Must be 'breadth-first', 'depth-first' or 'random'"]]

   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["Usage: exo [options] path/to/gen/erl"
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
           (.exists (clojure.java.io/file (first arguments))))
      {:model (first arguments) :options options}
      :else {:exit-message (usage summary)})))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [model options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (do (run-exploration model options)
          (exit 1 "")))))

(comment
  (run-exploration "/Users/larstvei/Dropbox/ifi/phd/abs/dpor-tests/gen/erl/run" {})
  (run-exploration "/Users/larstvei/Dropbox/ifi/phd/abs/trace-tests/gen/erl/run" {})
  (run-exploration "/Users/larstvei/Dropbox/ifi/phd/abs/shared-buffer/naive/gen/erl/run" {})
  (run-exploration "/Users/larstvei/Dropbox/ifi/phd/abs/martimj-master/thesis/models/gen/erl/run" {})
  (run-exploration "/Users/larstvei/Dropbox/ifi/phd/papers/jlamp-exogenous/implementation/gen/erl/run" {}))
