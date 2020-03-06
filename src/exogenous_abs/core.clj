(ns exogenous-abs.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as s]
            [clojure.set :refer [difference]]
            [clojure.data.json :as json]
            [exogenous.core :as exo]
            [exogenous.relations :as rels])
  (:gen-class))

(defn simulate [gen-run]
  (fn [trace]
    (let [outputraw (:out (sh gen-run "--exo"))
          [output json] (s/split outputraw #"ExoJSON: ")
          {:keys [domain mhb interference]} (json/read-str json :key-fn keyword)
          domain (set domain)
          mhb (-> (into #{} (map vec mhb))
                  rels/pairs->rel
                  rels/transitive-closure)
          interference (-> (into #{} (map vec interference))
                           rels/pairs->rel
                           rels/transitive-closure)
          hb (rels/make-hb mhb interference)
          trace (rels/linerize domain hb)]
      (println output)
      (println "----------------------------------------------------------------")
      {:trace trace
       :mhb (rels/rel->pairs mhb)
       :interference (rels/rel->pairs interference)})))

#_(def options-spec [[:required]])

(defn -main [gen-run & args]
  ;;(parse-opts args options-spec)
  (exo/explore (simulate gen-run)))
