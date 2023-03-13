(ns jepsen.dqlite.append
  "Test for transactional list append."
  (:require [jepsen [client :as client]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.dqlite [client :as c]]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (close! [this test])

  (setup! [this test])

  (teardown! [_ test])

  (invoke! [_ test op]
    (c/with-errors op
      (let [body  (str (:value op))
            value (c/request conn "POST" "/append" {:body body})]
        (assoc op :type :ok, :value value))))

  client/Reusable
  (reusable? [client test]))

(defn workload
  "A list append workload."
  [{:keys [key-count min-txn-length max-txn-length max-writes-per-key] :as _opts}]
  (merge (append/test {:key-count          (or key-count 12)
                       :min-txn-length     (or min-txn-length 1)
                       :max-txn-length     (or max-txn-length 4)
                       :max-writes-per-key (or max-writes-per-key 128)
                       :consistency-models [:serializable
                                            :strict-serializable]})
         {:client (Client. nil)}))
