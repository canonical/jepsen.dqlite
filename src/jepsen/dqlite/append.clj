(ns jepsen.dqlite.append
  "Test for transactional list append."
  (:require [clojure [string :as str]]
            [jepsen [client :as client]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.dqlite [client :as c]]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (setup! [this test])

  (invoke! [_ test op]
    (c/with-errors op
      (let [body     (str (:value op))
            response (c/request conn "POST" "/append" {:body body})]
        (assoc op :type :ok, :value (eval (read-string response))))))

  (teardown! [_ test])

  (close! [this test]))

(defn workload
  "A list append workload."
  [opts]
  (assoc (append/test {:key-count         3
                       :max-txn-length    4
                       :consistency-models [:serializable]})
         :client (Client. nil)))
