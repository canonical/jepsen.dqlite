(ns jepsen.dqlite.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util :refer [timeout meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.dqlite [client :as client]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def dir "/opt/dqlite")
(def bin "app")
(def binary (str dir "/" bin))
(def logfile (str dir "/app.log"))
(def pidfile (str dir "/app.pid"))
(def data-dir (str dir "/data"))
(def core-dump-glob (str data-dir "/core*"))

(defn setup-ppa!
  "Adds the Dqlite PPA to the APT sources"
  [version]
  (let [keyserver "keyserver.ubuntu.com"
        key       "392A47B5A84EACA9B2C43CDA06CD096F50FB3D04"
        line      (str "deb http://ppa.launchpad.net/dqlite/"
                       version "/ubuntu focal main")]
    (debian/add-repo! "dqlite" line keyserver key)))

(defn install!
  "Install the Go dqlite test application."
  [test node]

  ;; If we're not running in local mode, install libdqlite from the PPA.
  (when-not (:local test)
    (info "Installing libdqlite from PPA")
    (c/su
     (setup-ppa! (:version test))
     (debian/install [:libdqlite0])))

  ;; Create the test directory.
  (let [user (c/exec :whoami)]
    (c/su
     (c/exec :mkdir :-p dir)
     (c/exec :chown user dir)))

  ;; If we were given a pre-built binary, copy it over, otherwise build it from
  ;; source.
  (if-let [pre-built-binary (:binary test)]
    (c/upload pre-built-binary binary)
    (let [source (str dir "/app.go")]
      (info "Building test dqlite application from source")
      (c/su (debian/install [:libdqlite-dev :golang]))
      (c/upload "resources/app.go" source)
      (c/exec "go" "get" "-tags" "libsqlite3" "github.com/canonical/go-dqlite/app")
      (c/exec "go" "build" "-tags" "libsqlite3" "-o" binary source))))

(defn start!
  "Start the Go dqlite test application"
  [test node]
  (if (cu/daemon-running? pidfile)
    :already-running
    (c/su
      (c/exec :mkdir :-p data-dir)
      (cu/start-daemon! {:env {:LIBDQLITE_TRACE "1"
                               :LIBRAFT_TRACE "1"}
                         :logfile logfile
                         :pidfile pidfile
                         :chdir   data-dir}
                        binary
                        :-dir data-dir
                        :-disk (:disk test)
                        :-node (name node)
                        :-latency (:latency test)
                        :-cluster (str/join "," (:nodes test))))))

(defn grepkill!
  "Kills processes by grepping for the given string. If a signal is given,
  sends that signal instead. Signals may be either numbers or names, e.g.
  :term, :hup, ..."
  ([signal pattern]
   (try+ (let [pids (->> pattern
                         (c/exec :pgrep)
                         (#(str/split %2 %1) #"\s+"))]
           (doseq [pid pids]
             (c/exec :kill (str "-" (name signal)) pid)
             (c/exec :tail :-f (str "--pid=" pid) "/dev/null")))
         (catch [:type :jepsen.control/nonzero-exit, :exit 0] _
           nil)
         (catch [:type :jepsen.control/nonzero-exit, :exit 1] _
           nil)
         (catch [:type :jepsen.control/nonzero-exit, :exit 123] e
           (if (re-find #"No such process" (:err e))
             ; Ah, process already exited
             nil
             (throw+ e))))))
(defn kill!
  "Gracefully kill, `SIGTERM`, the Go dqlite test application."
  [_test node]
  (info "Killing dqlite with SIGTERM on" node)
  (c/su (grepkill! :SIGTERM "dqlite"))
  (info "Killing" bin "with SIGTERM on" node)
  (c/su (grepkill! :SIGTERM bin))
  :killed)

(defn stop!
  "Stop the Go dqlite test application with `stop-daemon!`,
   which will `SIGKILL`."
  [_test _node]
  (if (not (cu/daemon-running? pidfile))
    :not-running
    (do
      (c/su
       (cu/stop-daemon! pidfile))
      :stopped)))

(defn members
  "Fetch the cluster members from a random node (who will ask the leader)."
  [test]
  (client/members test (rand-nth (vec @(:members test)))))

(defn refresh-members!
  "Takes a test and updates the current cluster membership, based on querying
  the test's cluster leader."
  [test]
  (let [members (members test)]
    (info "Current membership is" (pr-str members))
    (reset! (:members test) (set members))))

(defn addable-nodes
  "What nodes could we add to this cluster?"
  [test]
  (remove @(:members test) (:nodes test)))

(defn wipe!
  "Wipes data files on the current node and create a 'removed' flag file to
  indicate that the node has left the cluster and should not automatically
  rejoin it."
  [test node]
  (c/exec :rm :-rf
          (c/lit (str data-dir)))
  (c/exec "mkdir" "-p" data-dir)
  (c/exec "touch" (str data-dir "/removed")))

(defn grow!
  "Adds a random node from the test to the cluster, if possible. Refreshes
  membership."
  [test]
  ;; First, get a picture of who the nodes THINK is in the cluster
  (refresh-members! test)

  ;; Can we add a node?
  (if-let [addable-nodes (seq (addable-nodes test))]
    (let [new-node (rand-nth addable-nodes)]
      (info :adding new-node)

      ;; Update the test map to include the new node
      (swap! (:members test) conj new-node)

      ;; Start the new node--it'll add itself to the cluster
      (c/on-nodes test [new-node]
                  (fn [test node]
                    (db/kill! (:db test) test node)
                    (c/exec "mkdir" "-p" data-dir)
                    (c/exec "touch" (str data-dir "/rejoin"))
                    (db/start! (:db test) test node)))

      new-node)

    :no-nodes-available-to-add))

(defn shrink!
  "Removes a random node from the cluster, if possible. Refreshes membership."
  [test]
  ; First, get a picture of who the nodes THINK is in the cluster
  (refresh-members! test)
  ; Next, remove a node.
  (if (< (count @(:members test)) 2)
    :too-few-members-to-shrink

    (let [node (rand-nth (vec @(:members test)))]
      ; Ask cluster to remove it
      (let [contact (-> test :members deref (disj node) vec rand-nth)]
        (info :removing node :via contact)
        (client/remove-member! test contact node))

      ; Kill the node and wipe its data dir; otherwise we'll break the cluster
      ; when it restarts
      (c/on-nodes test [node]
                  (fn [test node]
                    (db/kill! (:db test) test node)
                    (info "Wiping" node)
                    (wipe! test node)))

      ; Record that the node's gone
      (swap! (:members test) disj node)
      node)))

(defn retry
  [retries f & args]
  (let [res (try {:value (apply f args)}
                 (catch Exception e
                   (if (zero? retries)
                     (throw e)
                     {:exception e})))]
    (if (:exception res)
      (recur (dec retries) f args)
      (:value res))))

(defn stable
  [test]
  (retry 5 (fn [] (client/stable test
                                 (rand-nth (vec @(:members test)))))))

(defn health
  [test]
  (retry 5 (fn [] (client/stable test
                                 (rand-nth (vec @(:members test)))
                                 :health))))

(defn primaries
  "Returns the set of all primaries visible to any node in the
  cluster."
  [test]
  (->> (:nodes test)
       (pmap (fn [node]
               (timeout 1000 nil
                        (try
                          (client/leader test node)
                          (catch Exception e
                            ; wooooo
                            nil)))))
       (remove nil?)
       set))

(defn db
  "Dqlite test application. Takes a tmpfs DB which is set up prior to setting
  up this DB."
  [tmpfs]
  (let [primary-cache  (atom [])
        primary-thread (atom nil)]
    (reify db/DB
      (setup! [_ test node]
        "Install and start the test application."
        ;; The tmpfs must exist *before* the application starts
        (when tmpfs
          (db/setup! tmpfs test node))
        (info "Setting up test application")
        (install! test node)
        (start! test node)
        ;; Wait until node is ready
        (retry (:cluster-setup-timeout test) (fn []
                                               (Thread/sleep 1000)
                                               (client/ready test node)))
        ;; Spawn primary monitoring thread
        (c/su
         (when (compare-and-set! primary-thread nil :mine)
           (compare-and-set! primary-thread :mine
                             (future
                               (let [running? (atom true)]
                                 (while @running?
                                   (try
                                     (Thread/sleep 1000)
                                     (reset! primary-cache (primaries test))
                                     (info "Primary cache now" @primary-cache)
                                     (catch InterruptedException e
                                       (reset! running? false))
                                     (catch Throwable t
                                       (warn t "Primary monitoring thread crashed")))))))))
        )

      (teardown! [_ test node]
        (info "Tearing down test application")
        (when-let [t @primary-thread]
          (future-cancel t))
        (kill! test node)
        (when tmpfs
          (db/teardown! tmpfs test node))
        (c/su (c/exec :rm :-rf dir)))

      db/LogFiles
      (log-files [_ test node]
        (let [tarball    (str dir "/data.tar.bz2")
              ls-cmd     (str "ls " core-dump-glob)
              lines      (-> (try (c/exec "sh" "-c" ls-cmd)
                               (catch Exception e ""))
                              (str/split #"\n"))
              core-dumps (->> lines
                              (remove str/blank?)
                              (into []))
              app-binary (when (seq core-dumps) binary)
              everything (remove nil? [logfile tarball app-binary])]
          (try
            (c/exec :sudo :tar :cjf tarball data-dir)
            (catch Exception e (info "caught exception: " (.getMessage e))))
          everything))

      db/Process
      (start! [_db test node]
        (start! test node))

      (kill! [_db test node]
        (kill! test node))

      db/Pause
      (pause!
        [_db _test node]
        (info "Pausing" bin "on" node)
        (c/su
         (cu/grepkill! :stop bin))
        :paused)

      (resume!
        [_db _test node]
        (info "Resuming" bin "on" node)
        (c/su
         (cu/grepkill! :cont bin))
        :resumed)

      db/Primary
      (setup-primary! [db test node])
      (primaries [db test]
        @primary-cache))))
