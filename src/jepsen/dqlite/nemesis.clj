(ns jepsen.dqlite.nemesis
  "Nemeses for Dqlite"
  (:require [jepsen [control :as c]
                    [nemesis :as n]
		    [util :refer [random-nonempty-subset]]
                    [generator :as gen]]
            [jepsen.nemesis [combined :as nc]]
            [jepsen.dqlite.db :as db]))

(defn member-nemesis
  "A nemesis for adding and removing nodes from the cluster."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assoc op :value
             (case (:f op)
               :grow     (db/grow! test)
               :shrink   (db/shrink! test))))

    (teardown! [this test])

    n/Reflection
    (fs [_] [:grow :shrink])))

(defn member-generator
  "A generator for membership operations."
  [opts]
  (->> (gen/mix [{:type :info, :f :grow}
                 {:type :info, :f :shrink}])
       (gen/delay (:interval opts))))

(defn member-package
  "A combined nemesis package for adding and removing nodes."
  [opts]
  (when ((:faults opts) :member)
    {:nemesis   (member-nemesis opts)
     :generator (member-generator opts)
     :perf      #{{:name  "grow"
                   :fs    [:grow]
                   :color "#E9A0E6"}
                  {:name  "shrink"
                   :fs    [:shrink]
                   :color "#ACA0E9"}}}))

(defn stop-generator
  [opts]
  (let [stop (fn [test _] {:type :info, :f :stop-node, :value (rand-nth (:nodes test))})
        start (fn [test _] {:type :info, :f :start-node, :value nil})]
    (->> (gen/mix [stop stop start])
         (gen/stagger (:interval opts)))))

(defn stop-nemesis
  "A nemesis which responds to stop-node and start-node by politely
  stopping and starting the dqlite process."
  [opts]
  (reify
    n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assoc op :value
             (case (:f op)
               :start-node (c/on-nodes test db/start!)
               :stop-node  (c/on-nodes test [(:value op)] db/stop!))))

    (teardown! [this test])

    n/Reflection
    (fs [this]
      #{:start-node :stop-node})))

(defn stop-package
  "A nemesis package for politely stopping and restarting nodes."
  [opts]
  (when ((:faults opts) :stop)
    {:nemesis         (stop-nemesis opts)
     :generator       (stop-generator opts)
     :final-generator {:type :info, :f :start-node, :value nil}
     :perf            #{{:name  "stop"
                         :start #{:stop-node}
                         :stop  #{:start-node}
                         :color "#86DC68"}}}))

(defn stable-nemesis
  [opts]
  (reify
    n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (do (case (:f op)
            :stable (db/stable test))
          (assoc op :value nil)))

    (teardown! [this test])

    n/Reflection
    (fs [_] [:stable])))

(defn stable-package
  [opts]
  {:nemesis (stable-nemesis opts)
   :generator nil})

(defn nemesis-package
  "Constructs a nemesis and generators for dqlite."
  [opts]
  (let [opts (update opts :faults set)]
    (-> (nc/nemesis-packages opts)
        (concat [(member-package opts)
                 (stop-package opts)
                 (stable-package opts)]
                (:extra-packages opts))
        (->> (remove nil?))
        nc/compose-packages)))
