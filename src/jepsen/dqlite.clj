(ns jepsen.dqlite
  (:require [jepsen.cli :as cli]
            [jepsen.tests :as tests]))

(defn dqlite-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn dqlite-test})
                   (cli/serve-cmd))
            args))
