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

(defn setup-ppa!
  "Adds the Dqlite PPA to the APT sources"
  [version]
  (let [keyserver "keyserver.ubuntu.com"
        key       "392A47B5A84EACA9B2C43CDA06CD096F50FB3D04"
        line      (str "deb http://ppa.launchpad.net/dqlite/"
                       version "/ubuntu focal main")]
    (debian/add-repo! "dqlite" line keyserver key)))

(defn build!
  "Build the Go dqlite test application."
  []
  (let [source (str dir "/app.go")]
    (c/su
     (c/exec "mkdir" "-p" dir)
     (c/exec "chmod" "777" dir)
     (c/upload "resources/app.go" source)
     (c/exec "go" "get" "-tags" "libsqlite3" "github.com/canonical/go-dqlite/app")
     (c/exec "go" "build" "-tags" "libsqlite3" "-o" binary source))))

(defn start!
  "Start the Go dqlite test application"
  [test node]
  (c/su
    (c/exec "mkdir" "-p" data-dir)
    (cu/start-daemon! {:logfile logfile
                       :pidfile pidfile
                       :chdir   data-dir}
                      binary
                      :-dir data-dir
                      :-node (name node)
                      :-latency (:latency test)
                      :-cluster (str/join "," (:nodes test)))))

(defn kill!
  "Stop the Go dqlite test application"
  [test node]
  (c/su
    (cu/stop-daemon! binary pidfile)))

(defn stop!
  "Stops the Go dqlite test application"
  [test node]
  (c/su (cu/grepkill! 15 binary)))

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
  [node]
  (c/su
    (c/exec :rm :-rf
            (c/lit (str data-dir "/*"))
            (c/lit (str data-dir "/.*")))
    (c/exec "touch" (str data-dir "/removed"))))

(defn grow!
  "Adds a random node from the test to the cluster, if possible. Refreshes
  membership."
  [test]
  ; First, get a picture of who the nodes THINK is in the cluster
  (refresh-members! test)

  ; Can we add a node?
  (if-let [addable-nodes (seq (addable-nodes test))]
    (let [new-node (rand-nth addable-nodes)]
      (info :adding new-node)

      ; Update the test map to include the new node
      (swap! (:members test) conj new-node)

      ; Start the new node--it'll add itself to the cluster
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
                    (wipe! node)))

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

(def crash-pattern
  "An egrep pattern for finding crashes in log files."
  "Assertion|src\\/")

(defn logged-crashes
  "Returns a collection of log lines which look like crashes, across
  all nodes in the test."
  [test]
  (let [crashes
        (->> (c/on-many (:nodes test)
                        (try+
                          (c/su
                            (c/exec :egrep :-i crash-pattern
                                    logfile))
                          (catch [:type :jepsen.control/nonzero-exit] _
                            nil)))
             (remove (comp nil? val)))]
    (when (seq crashes)
      (into {} crashes))))

(defn db
  "Dqlite test application. Takes a tmpfs DB which is set up prior to setting
  up this DB."
  [tmpfs]
  (let [primary-cache  (atom [])
        primary-thread (atom nil)]
    (reify db/DB
      (setup! [_ test node]
        (info "installing dqlite test application" (:version test))
        (c/su
          (setup-ppa! (:version test))
          (debian/install [:libdqlite-dev :golang])
          (build!)
          (when tmpfs
            (db/setup! tmpfs test node))
          (start! test node)
          ; Wait until node is ready
          (retry (:cluster-setup-timeout test) (fn []
                                                 (Thread/sleep 1000)
                                                 (client/ready test node))))
        ; Spawn primary monitoring thread
        (when (compare-and-set! primary-thread nil :mine)
          (compare-and-set! primary-thread :mine
                            (future
                              (let [running? (atom true)]
                                (while @running?
                                  (try
                                    (Thread/sleep 1000)
                                    (reset! primary-cache
                                            (primaries test))
                                    ;(info "Primary cache now"
                                    ;      @primary-cache)
                                    (catch InterruptedException e
                                      (reset! running? false))
                                    (catch Throwable t
                                      (warn t "Primary monitoring thread crashed")))))))))

      (teardown! [_ test node]
        (when-let [t @primary-thread]
          (future-cancel t))
        (info "tearing down dqlite test application" (:version test))
        (kill! test node)
        (when tmpfs
          (db/teardown! tmpfs test node))
        (c/su (c/exec :rm :-rf dir)))

      db/LogFiles
      (log-files [_ test node]
        (let [tarball (str dir "/data.tar.bz2")]
          (c/su (c/exec :tar :cjf tarball data-dir))
          [logfile tarball]))

      db/Process
      (start! [_ test node]
        (start! test node))

      (kill! [_ test node]
        (kill! test node))

      db/Pause
      (pause!  [_ test node] (c/su (cu/grepkill! :stop "app")))
      (resume! [_ test node] (c/su (cu/grepkill! :cont "app")))

      db/Primary
      (setup-primary! [db test node])
      (primaries [db test]
        @primary-cache))))
