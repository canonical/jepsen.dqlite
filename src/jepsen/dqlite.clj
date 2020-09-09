(ns jepsen.dqlite
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.os.ubuntu :as ubuntu]))

(def dir "/opt/dqlite")
(def binary (str dir "app"))
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
             :-db (name node))
            (Thread/sleep 5000)))

    (teardown! [_ test node]
      (info "tearing down dqlite test application" version)
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir)))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn dqlite-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "dqlite"
          :os ubuntu/os
          :db (db "master")}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn dqlite-test})
                   (cli/serve-cmd))
            args))
