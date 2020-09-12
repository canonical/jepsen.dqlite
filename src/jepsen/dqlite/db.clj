(ns jepsen.dqlite.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

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

(defn build!
  "Build the Go dqlite test application."
  []
  (let [source (str dir "/app.go")]
    (c/su
     (c/exec "mkdir" "-p" dir)
     (c/upload "resources/app.go" source)
     (c/exec "go" "get" "-tags" "libsqlite3" "github.com/canonical/go-dqlite/app")
     (c/exec "go" "build" "-tags" "libsqlite3" "-o" binary source))))

(defn start!
  "Start the Go dqlite test application"
  [test node]
  (cu/start-daemon! {:logfile logfile
                     :pidfile pidfile
                     :chdir   dir}
                    binary
                    :-dir dir
                    :-node (name node)
                    :-cluster (str/join "," (:nodes test))))

(defn stop!
  "Stop the Go dqlite test application"
  [test node]
  (cu/stop-daemon! binary pidfile))
  
(defn db
  "Dqlite test application"
  []
  (reify db/DB
    (setup! [_ test node]
      (info "installing dqlite test application" (:version test))
      (setup-ppa! (:version test))
      (debian/install [:libdqlite-dev])
      (debian/install [:golang])
      (build!)
      (start! test node))

    (teardown! [_ test node]
      (info "tearing down dqlite test application" (:version test))
      (stop! test node)
      (c/su (c/exec :rm :-rf dir)))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))
