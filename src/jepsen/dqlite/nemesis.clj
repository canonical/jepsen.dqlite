(ns jepsen.dqlite.nemesis
  "Nemeses for Dqlite"
  (:require [jepsen [nemesis :as n]
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

(defn nemesis-package
  "Constructs a nemesis and generators for dqlite."
  [opts]
  (let [opts (update opts :faults set)]
    (-> (nc/nemesis-packages opts)
        (concat [(member-package opts)])
        (->> (remove nil?))
        nc/compose-packages)))
