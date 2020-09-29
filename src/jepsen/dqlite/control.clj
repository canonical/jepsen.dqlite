(ns jepsen.dqlite.control
  "Implements the Remote protocol to run commands locally."
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.shell :refer [sh]]
            [slingshot.slingshot :refer [throw+]]
            [jepsen.os.container :refer [containers]]
            [jepsen.control :as c]
            [clojure.string :refer [split-lines, trim]]
            [clojure.tools.logging :refer [info]])
  (:import (jepsen.control Remote)))

(defn exec
  "Execute a shell command."
  [host {:keys [cmd] :as opts}]
  (let [user (System/getProperty "user.name")]
    (->> (apply sh
                "sudo" "nsenter" "-p" "-n" "-m" "-t" (get @containers host)
                "su" user "-c"
                cmd
                (if-let [in (:in opts)]
                  [:in in]
                  [])))))

(defn cp
  "Copy files."
  [host src-paths dst-path]
  (doseq [src-path (flatten [src-paths])]
    (let [user (System/getProperty "user.name")
          cmd (str/join " " ["cp" (c/escape src-path) (c/escape dst-path)])]
      (->> (sh
            "sudo" "nsenter" "-p" "-n" "-m" "-t" (get @containers host)
            "su" user "-c"
            cmd)
           (c/throw-on-nonzero-exit)))))


(defrecord NsenterRemote [_]
  Remote
  (connect [this host] (assoc this :host host))
  (disconnect! [this] this)
  (execute! [this action] (exec (:host this) action))
  (upload! [this local-paths remote-path _rest] (cp (:host this) local-paths remote-path))
  (download! [this remote-paths local-path _rest] (cp (:host this) remote-paths local-path)))

(def nsenter "A remote that does things via nsenter."  (NsenterRemote. nil))

(def ssh c/ssh)
