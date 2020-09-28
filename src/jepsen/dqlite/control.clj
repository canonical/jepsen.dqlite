(ns jepsen.dqlite.control
  "Implements the Remote protocol to run commands locally."
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.shell :refer [sh]]
            [slingshot.slingshot :refer [throw+]]
            [jepsen.control :as c]
            [clojure.string :refer [split-lines, trim]]
            [clojure.tools.logging :refer [info]])
  (:import (jepsen.control Remote)))

(defn- unwrap-result
  "Throws when shell returned with nonzero exit status."
  [exc-type {:keys [exit] :as result}]
  (if (zero? exit)
    result
    (throw+
     (assoc result :type exc-type)
     nil ; cause
     "Command exited with non-zero status %d:\nSTDOUT:\n%s\n\nSTDERR:\n%s"
     exit
     (:out result)
     (:err result))))

(defn exec
  "Execute a shell command."
  [host {:keys [cmd] :as opts}]
  (apply sh
         "sudo" "ip" "netns" "exec" (str "jepsen-" host)
         "sh"
         "-c"
         cmd
         (if-let [in (:in opts)]
           [:in in]
           [])))

(defn cp
  "Copy files."
  [src-paths dst-path]
  (doseq [src-path (flatten [src-paths])]
    (->> (sh
          "sudo"
          "cp"
          (c/escape src-path)
          (c/escape dst-path))
         (unwrap-result ::copy-failed))))


(defrecord ShellRemote [_]
  Remote
  (connect [this host] (assoc this :host host))
  (disconnect! [this] this)
  (execute! [this action] (exec (:host this) action))
  (upload! [this local-paths remote-path _rest] (cp local-paths remote-path))
  (download! [this remote-paths local-path _rest] (cp remote-paths local-path)))

(def shell "A remote that does things via `sh` and `cp`."  (ShellRemote. nil))

(def ssh c/ssh)
