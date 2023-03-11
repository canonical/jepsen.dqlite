(ns jepsen.dqlite.set
  (:require [clojure.string :as str]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
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

  (close! [_ test])

  (setup! [this test])

  (teardown! [_ test])

  (invoke! [this test op]
    (case (:f op)
      :add (c/with-errors op
             (let [body     (str (:value op))
                   value    (c/request conn "POST" "/set" {:body body})]
               (assoc op :type :ok, :value value)))
      :read (c/with-errors op
              (let [value (c/request conn "GET" "/set")]
                (assoc op :type :ok, :value value)))))

  client/Reusable
  (reusable? [client test]))

(defn w
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))))

(defn r
  []
  (repeat {:type :invoke, :f :read, :value nil}))

(defn workload
  [_opts]
  {:client (Client. nil)
   :generator (gen/mix [r w])
   :final-generator (gen/phases
                     (gen/log "Final reads...")
                     (->> (gen/once (r))
                          (gen/map #(assoc % :final? true))
                          (gen/each-thread)))
   :checker (checker/set-full {:linearizable? true})})
