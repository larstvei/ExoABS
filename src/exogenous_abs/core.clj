(ns exogenous-abs.core
  (:gen-class)
  (:require [clojure.data.json :as json]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as s]
            [exogenous.core :as exo]
            [exogenous.relations :as rels]))

(defn linerize-trace [domain mhb]
  (let [local-occurs-before (for [[i [cog1 & _ :as e1]] domain
                                  [j [cog2 & _ :as e2]] domain
                                  :when (and (= cog1 cog2) (< i j))]
                              [e1 e2])
        strict-hb (-> (into mhb local-occurs-before)
                      rels/pairs->rel
                      rels/transitive-closure)]
    (rels/linerize (set (map second domain)) strict-hb)))

(defn simulate [gen-run]
  (fn [in-trace]
    (println in-trace)
    (let [input-for-simulator (-> {:trace in-trace} json/write-str)
          outputraw (:out (sh gen-run "--exo" :in input-for-simulator))
          [pre output json] (s/split outputraw #"ExoJSON: ")
          {:keys [domain mhb interference]}
          (json/read-str json :key-fn keyword :value-fn (fn [_ v] (set v)))
          out-trace (linerize-trace domain mhb)]
      (println output)
      (println out-trace)
      (println "----------------------------------------------------------------")
      {:trace out-trace :mhb mhb :interference interference})))

(defn -main [gen-run & args]
  (exo/explore (simulate gen-run)))
