(ns dqlite.os.ubuntu
  "Common tasks for Ubuntu/CockroachDB boxes."
  (:use clojure.tools.logging)
  (:require [clojure.set :as set]
            [jepsen.util :refer [meh]]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.net :as net]
            [clojure.string :as str]))

(def os
  (reify os/OS
    (setup! [_ test node]
      (info node "setting up ubuntu")

      (debian/setup-hostfile!)

      (debian/maybe-update!)

      (c/su
       ;; Packages!
       (debian/install [:apt-transport-https
                        :wget
                        :curl
                        :man-db
                        :faketime
                        :ntpdate
                        :unzip
                        :iptables
                        :psmisc
                        :tar
                        :bzip2
                        :iputils-ping
                        :iproute2
                        :rsyslog
                        :sudo
                        :logrotate]))

      (meh (net/heal! (:net test) test)))

    (teardown! [_ test node])))
