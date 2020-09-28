(ns jepsen.os.container
  "Set up an isolated node container using a network namespace."
  (:use clojure.tools.logging)
  (:require [clojure.set :as set]
            [clojure.java.shell :refer [sh]]
            [jepsen.util :refer [meh]]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.net :as net]
            [clojure.string :as str]))

(def prefix "jepsen-")

(def bridge (str prefix "br"))

(defn node-index
  "The index of the node within the test nodes array."
  [test node]
  (let [nodes (:nodes test)]
    (+ (.indexOf nodes node) 1)))

(defn iface-addr
  "The address/mask to assign to the container's network interface."
  [test node]
  (let [i (node-index test node)]
    (str "10.1.1.1" i "/24")))

(defn namespace-name
  [test node]
  "The name of the namespace to use for the given node"
  (str prefix node))


(defn exec
  "Execute a shell command without entering the node namespace."
  [& commands]
  (->> commands
       (map c/escape)
       (apply sh)))

(def os
  (reify os/OS
    (setup! [_ test node]
      (info node "Setting up container")

      (let [ns    (namespace-name test node)
            i     (node-index test node)
            veth1 (str prefix "veth" i)
            veth2 (str prefix "br-veth" i)
            addr  (iface-addr test node)]
        (meh (exec :sudo :ip :netns :del ns))
        (exec :sudo :ip :netns :add ns)
        (exec :sudo :ip :link :add veth1 :type :veth :peer :name veth2)
        (exec :sudo :ip :link :set veth1 :netns ns)
        (exec :sudo :ip :-n ns :addr :add addr :dev veth1)
        (exec :sudo :ip :-n ns :link :set :dev veth1 :up)
        (exec :sudo :ip :-n ns :link :set :dev :lo :up)
        (exec :sudo :ip :link :set veth2 :up)
        (exec :sudo :ip :link :set veth2 :master bridge))
      
      (meh (net/heal! (:net test) test)))

    (teardown! [_ test node]
      (info node "Tearding down container")
      (exec :sudo :ip :netns :del (namespace-name test node)))))
