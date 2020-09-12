(ns jepsen.dqlite.nemesis
  "Nemeses for Dqlite"
  (:require [jepsen.nemesis [combined :as nc]]))

(defn nemesis-package
  "Constructs a nemesis and generators for MongoDB."
  [opts]
  (let [opts (update opts :faults set)]
    (nc/nemesis-package opts)))
