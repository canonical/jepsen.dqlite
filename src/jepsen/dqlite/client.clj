(ns jepsen.dqlite.client
  "Helper functions for interacting with the test Dqlite application."
  (:require [clojure.string :as str]
            [slingshot.slingshot :refer [try+ throw+]]
            [clj-http.client :as http])
  (:import (java.net ConnectException SocketException SocketTimeoutException)))

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
  (let [response (str (:body (http/request
                              (merge {:method method
                                      :url (str conn path)
                                      :socket-timeout 5000
                                      :connection-timeout 5000}
                                     opts))))]
    (if (str/includes? response "Error")
      (throw+ {:msg response})
      (eval (read-string response)))))

(defn leader
  "Return the node name of the current Dqlite leader."
  [test node]
  (let [conn (open test node)
        leader (request conn "GET" "/leader")]
    (if (= "" leader)
      (throw+ {:msg "no leader"})
      leader)))

(defn members
  "Return the names of the current cluster members."
  [test node]
  (let [conn (open test node)]
    (request conn "GET" "/members")))

(defn remove-member!
  "Remove a cluster member."
  [test node old-node]
  (let [conn (open test node)
        body (str old-node)]
    (request conn "DELETE" "/members" {:body body})))

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
         (catch [:msg "Error: driver: bad connection"] e#
           (assoc ~op :type :info, :error :bad-connection))
         (catch SocketTimeoutException e#
           (assoc ~op :type :info, :error :connection-timeout))
         (catch SocketException e#
           (assoc ~op :type :info, :error :connection-error))
         (catch ConnectException e#
           (assoc ~op :type :fail, :error :connection-refused))))
