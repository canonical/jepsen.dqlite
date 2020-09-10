(ns jepsen.dqlite.client
  "Helper functions for interacting with the test Dqlite application."
  (:require [clojure.string :as str]
            [slingshot.slingshot :refer [try+ throw+]]
            [clj-http.client :as http])
  (:import (java.net ConnectException SocketException)))

(defn endpoint
  "The root HTTP URL of the test Dqlite application API endpoint on a node."
  [node]
  (str "http://" (name node) ":" 8080))

(defn open
  "Opens a connection to the given node. TODO: use persistent HTTP connections."
  [test node]
  (endpoint node))

(defn request
  "Perform an API request"
  [conn method path & [opts]]
  (let [result (str (:body (http/request
                            (merge {:method method
                                    :url (str conn path)
                                    :socket-timeout 5000
                                    :connection-timeout 5000}
                                   opts))))]
    (if (str/includes? result "Error")
      (throw+ {:msg result})
      result)))

(defn leader
  "Return the node name of the current Dqlite leader."
  [test node]
  (let [conn   (open test node)
        result (request conn "GET" "/leader")]
    (if (= "" result)
      (throw+ {:msg "no leader"})
      result)))

(defmacro with-errors
  "Takes an operation and a body; evals body, turning known errors into :fail
  or :info ops."
  [op & body]
  `(try+ ~@body
         (catch [:msg "Error: database is locked"] e#
           (assoc ~op :type :fail, :error :locked))
         (catch [:msg "Error: context deadline exceeded"] e#
           (assoc ~op :type :info, :error :timeout))
         (catch [:msg "Error: failed to create dqlite connection: no available dqlite leader server found"] e#
           (assoc ~op :type :fail, :error :unavailable))
         (catch SocketException e#
           (assoc ~op :type :info, :error :connection-dropped))
         (catch ConnectException e#
           (assoc ~op :type :fail, :error :connection-refused))))
