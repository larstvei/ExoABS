(defproject exogenous-abs "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "1.0.0"]
                 [org.clojure/tools.cli "1.0.194"]
                 [exogenous "0.1.0-SNAPSHOT"]
                 [org.clojure/core.async "1.2.603"]]
  :profiles {:uberjar {:aot [exogenous-abs.core]}}
  :repl-options {:init-ns exogenous-abs.core}
  :main exogenous-abs.core)
