(ns jepsen.dqlite.sets
  (:require [clojure.string :as str]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.dqlite [client :as c]])
  (:import (java.net ConnectException SocketException)))

(defn parse-int
  "Wrapper around Integer/parseInt."
  [s]
  (Integer/parseInt s))

(defn parse-list
  "Parses a list of values. Passes through the empty string."
  [s]
    (when-not (= s "") (map parse-int (str/split s #" "))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :add (c/with-errors op
             (let [body     (str (:value op))
                   response (c/request conn "POST" "/set" {:body body})
                   value    (parse-int response)]
               (assoc op :type :ok, :value value)))
      :read (c/with-errors op
              (let [response (c/request conn "GET" "/set")
                    value    (parse-list response)]
                (assoc op :type :ok, :value value)))))

  (teardown! [_ test])

  (close! [_ test]))

(defn adds
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x, :bar "baz"}))
       (gen/seq)))

(defn reads
  []
  {:type :invoke, :f :read, :value nil})


(defn workload
  [opts]
  (let [c (:concurrency opts)]
    {:client (Client. nil)
     :generator (->> (gen/reserve (/ c 2) (adds) (reads))
                     (gen/stagger 1/10))
     :checker (checker/set-full)}))
