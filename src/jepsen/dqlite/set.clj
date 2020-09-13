(ns jepsen.dqlite.set
  (:require [clojure.string :as str]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen [util :as util :refer [parse-long]]]
            [jepsen.dqlite [client :as c]])
  (:import (java.net ConnectException SocketException)))

(defn parse-list
  "Parses a list of values. Passes through the empty string."
  [s]
    (when-not (= s "") (map parse-long (str/split s #" "))))

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
                   value    (parse-long response)]
               (assoc op :type :ok, :value value)))
      :read (c/with-errors op
              (let [response (c/request conn "GET" "/set")
                    value    (parse-list response)]
                (assoc op :type :ok, :value value)))))

  (teardown! [_ test])

  (close! [_ test]))

(defn w
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))))

(defn r
  []
  {:type :invoke, :f :read, :value nil})


(defn workload
  [opts]
  (let [c (:concurrency opts)]
    {:client (Client. nil)
     :generator (gen/reserve (/ c 2) (repeat (r)) (w))
     :checker (checker/set-full)}))
