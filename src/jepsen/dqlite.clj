(ns jepsen.dqlite
  (:gen-class)
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as jc]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.dqlite [db :as db]
                           [sets :as set]
                           [nemesis :as nemesis]]))

(def workloads
  "A map of workload names to functions that can take CLI opts and construct
  workloads."
  {:set             set/workload})

(def plot-spec
  "Specification for how to render operations in plots"
  {:nemeses #{{:name        "kill"
               :color       "#E9A4A0"
               :start       #{:kill-app}
               :stop        #{:start-app}}
              {:name        "pause"
               :color       "#C5A0E9"
               :start       #{:pause-app}
               :stop        #{:resume-app}}
              {:name        "partition"
               :color       "#A0C8E9"
               :start       #{:start-partition}
               :stop        #{:stop-partition}}
              {:name        "clock"
               :color       "#A0E9DB"
               :start       #{:strobe-clock :bump-clock}
               :stop        #{:reset-clock}
               :fs          #{:check-clock-offsets}}}})

(defn test
  "Constructs a test from a map of CLI options."
  [opts]
  (let [name (str "Dqlite " (:version opts))
        workload  ((get workloads (:workload opts)) opts)
        nemesis   (nemesis/nemesis opts)
        gen       (->> (:generator workload)
                       (gen/nemesis (:generator nemesis))
                       (gen/time-limit (:time-limit opts)))
        gen       (if (:final-generator workload)
                    (gen/phases gen
                                (gen/log "Healing cluster")
                                (gen/nemesis (:final-generator nemesis))
                                (gen/log "Waiting for recovery")
                                (gen/sleep (:final-recovery-time opts))
                                (gen/clients (:final-generator workload)))
                    gen)]
    (merge tests/noop-test
           opts
           (dissoc workload :final-generator)
           {:name      name
            :os        ubuntu/os
            :db        (db/db)
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :generator gen
            :plot       plot-spec
            :checker    (checker/compose
                          {:perf        (checker/perf)
                           :clock-skew  (checker/clock-plot)
                           :workload    (:checker workload)})})))

(def nemesis-specs
  "These are the types of failures that the nemesis can perform."
  #{:partition
    :partition-one
    :partition-leader
    :partition-half
    :partition-ring
    :kill
    :pause})

(defn parse-nemesis-spec
  "Parses a comma-separated string of nemesis types, and turns it into an
  option map like {:kill-alpha? true ...}"
  [s]
  (if (= s "none")
    {}
    (->> (str/split s #",")
         (map (fn [o] [(keyword o) true]))
         (into {}))))

(def cli-opts
  "Command line options for tools.cli"
  [["-v" "--version VERSION" "What version of Dqlite should to install"
    :default "master"]
   [nil "--nemesis SPEC" "A comma-separated list of nemesis types"
    :default {:interval 10}
    :parse-fn parse-nemesis-spec
    :assoc-fn (fn [m k v] (update m :nemesis merge v))
    :validate [(fn [parsed]
                 (and (map? parsed)
                      (every? nemesis-specs (keys parsed))))
               (str "Should be a comma-separated list of failure types. A failure "
                    (.toLowerCase (jc/one-of nemesis-specs))
                    ". Or, you can use 'none' to indicate no failures.")]]])

(def single-test-opts
  "CLI options for running a single test"
  [["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :missing (str "--workload " (jc/one-of workloads))
    :validate [workloads (jc/one-of workloads)]]])


(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (jc/run!
   (merge (jc/serve-cmd)
          (jc/single-test-cmd {:test-fn test
                               :opt-spec (concat cli-opts single-test-opts)}))
   args))
