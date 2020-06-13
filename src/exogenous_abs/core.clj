(ns exogenous-abs.core
  (:gen-class)
  (:require [clojure.data.json :as json]
            [clojure.java.shell :refer [sh]]
            [clojure.set :refer [difference]]
            [clojure.string :as s]
            [exogenous.core :as exo]
            [exogenous.relations :as rels]))

(def outputs (atom []))

(defn linerize-trace [seed-trace domain mhb]
  (let [local-occurs-before (for [[i [cog1 & _ :as e1]] domain
                                  [j [cog2 & _ :as e2]] domain
                                  :when (and (= cog1 cog2) (< i j))]
                              [e1 e2])
        strict-hb (-> (into mhb local-occurs-before)
                      rels/pairs->rel
                      rels/transitive-closure)
        domain (difference (set (map second domain)) (set seed-trace))]
    (rels/linerize domain strict-hb seed-trace)))

(defn simulate [gen-run]
  (fn [seed-trace]
    (let [input-for-simulator (-> {:trace seed-trace} json/write-str)
          outputraw (:out (sh gen-run "--exo" :in input-for-simulator))
          [pre output json] (s/split outputraw #"ExoJSON: ")
          {:keys [domain mhb interference]}
          (json/read-str json :key-fn keyword :value-fn (fn [_ v] (set v)))
          out-trace (linerize-trace seed-trace domain mhb)]
      (swap! outputs conj output)
      {:trace out-trace :mhb mhb :interference interference})))

(defn -main [gen-run options]
  (reset! outputs [])
  (let [info (exo/informed-explore (simulate gen-run) options)]
    (doseq [[output f] (frequencies @outputs)]
      (println (str "------------------------------------------------------- (" f ")"))
      (println output))
    (prn info)))

(comment
  (-main "/Users/larstvei/Dropbox/ifi/phd/abs/trace-tests/gen/erl/run" {})
  (-main "/Users/larstvei/Dropbox/ifi/phd/abs/shared-buffer/naive/gen/erl/run" {})
  (-main "/Users/larstvei/Dropbox/ifi/phd/abs/martimj-master/thesis/models/gen/erl/run" {}))
