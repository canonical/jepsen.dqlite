(ns jepsen.dqlite
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [knossos.op :as op]
            [knossos.model :as model]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]
                    [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.os.ubuntu :as ubuntu]
            [slingshot.slingshot :refer [try+ throw+]]
            [clj-http.client :as http]))

(def dir "/opt/dqlite")
(def binary (str dir "/app"))
(def logfile (str dir "/app.log"))
(def pidfile (str dir "/app.pid"))

(defn setup-ppa!
  "Adds the Dqlite PPA to the APT sources"
  [version]
  (let [keyserver "keyserver.ubuntu.com"
        key       "392A47B5A84EACA9B2C43CDA06CD096F50FB3D04"
        line      (str "deb http://ppa.launchpad.net/dqlite/"
                       version "/ubuntu focal main")]
    (debian/add-repo! "dqlite" line keyserver key)))

(defn build-app!
  "Build the Go dqlite test application."
  []
  (let [source (str dir "/app.go")]
    (c/su
     (c/exec "mkdir" "-p" dir)
     (c/upload "resources/app.go" source)
     (c/exec "go" "get" "-tags" "libsqlite3" "github.com/canonical/go-dqlite/app")
     (c/exec "go" "build" "-tags" "libsqlite3" "-o" binary source))))
  

(defn db
  "Dqlite test application"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info "installing dqlite test application" version)
      (setup-ppa! version)
      (debian/install [:libdqlite-dev])
      (debian/install [:golang])
      (build-app!)
      (c/su (cu/start-daemon!
             {:logfile logfile
              :pidfile pidfile
              :chdir   dir}
             binary
             :-dir dir
             :-node (name node)
             :-cluster (str/join "," (:nodes test)))
            (Thread/sleep 5000)))

    (teardown! [_ test node]
      (info "tearing down dqlite test application" version)
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir)))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn api-url
  "The HTTP url clients use to talk to the app API on a node."
  [node]
  (str "http://" (name node) ":" 8080))

(defn parse-int
  "Wrapper around Integer/parseInt."
  [s]
  (Integer/parseInt s))

(defn parse-list
  "Parses a list of values. Passes through `nil`."
  [s]
    (when s (map parse-int (str/split s #" "))))

(defn sets-add
  [conn value]
  (let [result (str (:body (http/post (str conn "/set")
                                      {:body (str value)
                                       :socket-timeout 5000
                                       :connection-timeout 5000})))]
    (if (str/includes? result "Error")
      (throw+ {:type :write-error :msg result})
      value)))

(defn sets-read
  [conn]
  (let [result (str (:body (http/get (str conn "/set")
                                     {:socket-timeout 5000
                                      :connection-timeout 5000})))]
    (if (str/includes? result "Error")
      (throw+ {:type :read-error :msg result})
      (parse-list result))))

(defrecord SetsClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (api-url node)))

  (setup! [this test])

  (invoke! [_ test op]
    (case (:f op)
      :add (try+
            (assoc op :type :ok, :value (sets-add conn (:value op)))
            (catch [:msg "Error: context deadline exceeded"] ex
              (assoc op :type :info, :error :timeout))
            (catch [:msg "Error: failed to create dqlite connection: no available dqlite leader server found"] ex
              (assoc op :type :fail, :error :unavailable))
            (catch [:msg "Error: database is locked"] ex
              (assoc op :type :fail, :error :locked)))
      :read (try+
             (assoc op :type :ok, :value (sets-read conn))
             (catch [:msg "Error: context deadline exceeded"] ex
               (assoc op :type :info, :error :timeout))
             (catch [:msg "Error: failed to create dqlite connection: no available dqlite leader server found"] ex
               (assoc op :type :fail, :error :unavailable))
             (catch [:msg "Error: database is locked"] ex
               (assoc op :type :fail, :error :locked)))
      ))

  (teardown! [this test])

  (close! [_ test]))

(defn check-sets
  "Given a set of :add operations followed by a final :read, verifies that
  every successfully added element is present in the read, and that the read
  contains only elements for which an add was attempted, and that all
  elements are unique."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [attempts (->> history
                          (r/filter op/invoke?)
                          (r/filter #(= :add (:f %)))
                          (r/map :value)
                          (into #{}))
            adds (->> history
                      (r/filter op/ok?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
            fails (->> history
                       (r/filter op/fail?)
                       (r/filter #(= :add (:f %)))
                       (r/map :value)
                       (into #{}))
            unsure (->> history
                        (r/filter op/info?)
                        (r/filter #(= :add (:f %)))
                        (r/map :value)
                        (into #{}))
            final-read-l (->> history
                              (r/filter op/ok?)
                              (r/filter #(= :read (:f %)))
                              (r/map :value)
                              (reduce (fn [_ x] x) nil))]
        (if-not final-read-l
          {:valid? :unknown
           :error  "Set was never read"}

          (let [final-read  (set final-read-l)
                dups        (into [] (for [[id freq] (frequencies final-read-l)
                                           :when (> freq 1)]
                                       id))

                ;;The OK set is every read value which we added successfully
                ok          (set/intersection final-read adds)

                ;; Unexpected records are those we *never* attempted.
                unexpected  (set/difference final-read attempts)

                ;; Revived records are those that were reported as failed and
                ;; still appear.
                revived  (set/intersection final-read fails)

                ;; Lost records are those we definitely added but weren't read
                lost        (set/difference adds final-read)

                ;; Recovered records are those where we didn't know if the add
                ;; succeeded or not, but we found them in the final set.
                recovered   (set/intersection final-read unsure)]

            {:valid?          (and (empty? lost)
                                   (empty? unexpected)
                                   (empty? dups)
                                   (empty? revived))
             :duplicates      dups
             :ok              (util/integer-interval-set-str ok)
             :lost            (util/integer-interval-set-str lost)
             :unexpected  (util/integer-interval-set-str unexpected)
             :recovered (util/integer-interval-set-str recovered)
             :revived (util/integer-interval-set-str revived)
             :ok-frac      (util/fraction (count ok) (count attempts))
             :revived-frac   (util/fraction (count revived) (count fails))
             :unexpected-frac (util/fraction (count unexpected) (count attempts))
             :lost-frac       (util/fraction (count lost) (count attempts))
             :recovered-frac  (util/fraction (count recovered) (count attempts))}))))))

(defn dqlite-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name      "dqlite"
          :os        ubuntu/os
          :db        (db "master")
          :client    (SetsClient. nil)
          :nemesis   (nemesis/partition-random-halves)
          :generator (gen/phases
                      (->> (range)
                           (map (partial array-map
                                         :type :invoke
                                         :f :add
                                         :value))
                           gen/seq
                           (gen/stagger 1/10)
                           (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info, :f :start}
                                             (gen/sleep 5)
                                             {:type :info, :f :stop}])))
                           (gen/time-limit 30))
                      (gen/each
                       (->> {:type :invoke, :f :read, :value nil}
                            (gen/limit 2)
                            gen/clients)))
          :checker   (checker/compose
                      {:perf     (checker/perf)
                       :details  (check-sets)})
          }))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn dqlite-test})
                   (cli/serve-cmd))
            args))
