(ns jepsen.dqlite.bank
  "Implements a bank-account test, where we transfer amounts between a pool of
  accounts, and verify that reads always see a constant amount."
  (:require [jepsen.dqlite [client :as c]]
            [jepsen [client :as client]
                    [generator :as gen]]
            [jepsen.tests.bank :as bank]))

(def options {:accounts           (vec (range 8))
              :max-transfer       10
              :total-amount       0})

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (close! [_ test])

  (setup! [this test]
    (let [body (str options)]
      (Thread/sleep 5000) ; TODO: retry on failed connection instead
      (c/request conn "PUT" "/bank" {:body body})))

  (teardown! [_ test])

  (invoke! [this test op]
    (case (:f op)
      :read (c/with-errors op
              (let [value (c/request conn "GET" "/bank")]
                (assoc op :type :ok, :value value)))
      :transfer (c/with-errors op
                  (let [body     (str (:value op))
                        value    (c/request conn "POST" "/bank" {:body body})]
                    (assoc op :type :ok)))))

  client/Reusable
  (reusable? [client test]))

(defn workload
  "A bank workload."
  [_opts]
  (merge (bank/test {:negative-balances? true})
         options
         {:client (Client. nil)
          :final-generator (gen/phases
                            (gen/log "Final reads...")
                            (->> (bank/read nil nil)
                                 (gen/map #(assoc % :final? true))
                                 (gen/each-thread)))}))
