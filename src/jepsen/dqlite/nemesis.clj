(ns jepsen.dqlite.nemesis
  "Nemeses for Dqlite"
  (:require [jepsen
             [control :as c]
             [nemesis :as n]
             [generator :as gen]]
            [jepsen.dqlite.db :as db]
            [jepsen.nemesis.combined :as nc]))

(defn member-nemesis
  "A nemesis for adding and removing nodes from the cluster."
  [_opts]
  (reify n/Nemesis
    (setup! [this _test] this)

    (invoke! [_this test op]
      (assoc op :value
             (case (:f op)
               :grow     (db/grow! test)
               :shrink   (db/shrink! test))))

    (teardown! [_this _test])

    n/Reflection
    (fs [_test]
      #{:grow :shrink})))

(defn member-generator
  "A generator for membership operations."
  [opts]
  (->> (gen/mix [(repeat {:type :info, :f :grow})
                 (repeat {:type :info, :f :shrink})])
       (gen/stagger (or (:interval opts) nc/default-interval))))

(defn member-package
  "A combined nemesis package for adding and removing nodes."
  [opts]
  (when ((:faults opts) :member)
    {:nemesis   (member-nemesis opts)
     :generator (member-generator opts)
     :perf      #{{:name  "grow"
                   :fs    #{:grow}
                   :start #{}
                   :stop  #{}
                   :color "#E9A0E6"}
                  {:name  "shrink"
                   :fs    #{:shrink}
                   :start #{}
                   :stop  #{}
                   :color "#ACA0E9"}}}))

(defn stop-nemesis
  "A nemesis which responds to `:stop-node` and `:start-node` by politely
  stopping and starting the dqlite app."
  [db]
  (reify
    n/Nemesis
    (setup! [this _test] this)

    (invoke! [_this test {:keys [f value] :as op}]
      (let [targets (nc/db-nodes test db value)]
        (assoc op :value
               (case f
                 :start-node (c/on-nodes test targets db/start!)
                 :stop-node  (c/on-nodes test targets db/stop!)))))

    (teardown! [_this _test])

    n/Reflection
    (fs [_this]
      #{:start-node :stop-node})))

(defn stop-package
  "A nemesis package for politely stopping and restarting the dqlite app."
  [{:keys [faults stop db interval] :as _opts}]
  (when (:stop faults)
    (let [targets (:targets stop)
          stops  (fn [_ _]
                   {:type  :info
                    :f     :stop-node
                    :value (rand-nth targets)})
          starts (repeat
                  {:type  :info
                   :f     :start-node
                   :value :all})
          interval (or interval nc/default-interval)]
      {:nemesis         (stop-nemesis db)
       :generator       (->> (gen/flip-flop stops starts)
                             (gen/stagger interval))
       :final-generator (gen/once starts)
       :perf            #{{:name  "stop"
                           :start #{:stop-node}
                           :stop  #{:start-node}
                           :color "#86DC68"}}})))

(defn stable-nemesis
  [opts]
  (reify
    n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (do (case (:f op)
            :stable (db/stable test)
            :health (db/health test))
          (assoc op :value nil)))

    (teardown! [this test])

    n/Reflection
    (fs [_] [:stable :health])))

(defn stable-package
  [opts]
  {:nemesis (stable-nemesis opts)
   :generator nil
   :perf #{{:name  "stable"
            :fs    #{:stable}
            :start #{}
            :stop  #{}
            :color "#90EEA8"}
           {:name  "health"
            :fs    #{:health}
            :start #{}
            :stop  #{}
            :color "#90EE90"}}})

(defn nemesis-package
  "Constructs a nemesis and generators for dqlite."
  [opts]
  (let [opts (update opts :faults set)]
    (->> (concat [(nc/partition-package opts)
                  (nc/db-package opts)
                  (member-package opts)
                  (stop-package opts)
                  (stable-package opts)]
                (:extra-packages opts))
        (remove nil?)
        nc/compose-packages)))
