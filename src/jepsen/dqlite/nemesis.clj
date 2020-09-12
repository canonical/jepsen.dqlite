(ns jepsen.dqlite.nemesis
  "Nemeses for Dqlite"
  (:require [jepsen
             [client :as client]
             [control :as c]
             [nemesis :as nemesis]
             [net :as net]
             [generator :as gen]
             [util :as util :refer [letr]]]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.time :as nt]
            [jepsen.dqlite [client :as dc]
                           [db :as db]]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn process-nemesis
  "A nemesis that can pause, resume, start, stop, and kill the test application."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [nodes (:nodes test)
            nodes (case (:f op)
                    ; When resuming, resume all nodes
                    (:resume-app :start-app) nodes

                    (util/random-nonempty-subset nodes))
            ; If the op wants to give us nodes, that's great
            nodes (or (:value op) nodes)]
        (assoc op :value
               (c/on-nodes test nodes
                           (fn [test node]
                             (case (:f op)
                               :start-app  (db/start! test node)
                               :kill-app   (db/stop!  test node)
                               :pause-app  (cu/signal! db/bin :STOP)
                               :resume-app (cu/signal! db/bin :CONT)))))))

    (teardown! [this test])))

(defn op
  "Shorthand for constructing a nemesis op"
  ([f]
   (op f nil))
  ([f v]
   {:type :info, :f f, :value v})
  ([f v & args]
   (apply assoc (op f v) args)))

(defn partition-one-gen
  "A generator for a partition that isolates one node."
  [test process]
  (op :start-partition
     (->> test :nodes nemesis/split-one nemesis/complete-grudge)
     :partition-type :single-node))

(defn partition-leader-gen
  "A generator for a partition that isolates the current leader in a
  minority."
  [test process]
  (let [leader (dc/leader test (rand-nth (:nodes test)))
        followers (shuffle (remove #{leader} (:nodes test)))
        nodes       (cons leader followers)
        components  (split-at 1 nodes) ; Maybe later rand(n/2+1?)
        grudge      (nemesis/complete-grudge components)]
    (op :start-partition, grudge, :partition-type :pd-leader)))

(defn partition-half-gen
  "A generator for a partition that cuts the network in half."
  [test process]
  (op :start-partition
      (->> test :nodes shuffle nemesis/bisect nemesis/complete-grudge)
      :partition-type :half))

(defn partition-ring-gen
  "A generator for a partition that creates overlapping majority rings"
  [test process]
  (op :start-partition
      (->> test :nodes nemesis/majorities-ring)
      :partition-type :ring))

(defn full-nemesis
  "Merges together all nemeses"
  []
  (nemesis/compose
    {#{:start-app
       :kill-app
       :pause-app
       :resume-app}                         (process-nemesis)
     {:start-partition :start
      :stop-partition  :stop}               (nemesis/partitioner nil)}))

(defn flip-flop
  "Switches between ops from two generators: a, b, a, b, ..."
  [a b]
  (gen/seq (cycle [a b])))

(defn opt-mix
  "Given a nemesis map n, and a map of options to generators to use if that
  option is present in n, constructs a mix of generators for those options. If
  no options match, returns `nil`."
  [n possible-gens]
  (let [gens (reduce (fn [gens [option gen]]
                       (if (option n)
                         (conj gens gen)
                         gens))
                     []
                     possible-gens)]
    (when (seq gens)
      (gen/mix gens))))

(defn mixed-generator
  "Takes a nemesis options map `n`, and constructs a generator for all nemesis
  operations. This generator is used during normal nemesis operations."
  [n]
  ; Shorthand: we're going to have a bunch of flip-flops with various types of
  ; failure conditions and a single recovery.
  (let [o (fn [possible-gens recovery]
            ; We return nil when mix does to avoid generating flip flops when
            ; *no* options are present in the nemesis opts.
            (when-let [mix (opt-mix n possible-gens)]
              (flip-flop mix recovery)))]

    ; Mix together our different types of process crashes, partitions, and
    ; clock skews.
    (->> [(o {:kill-app (op :kill-app)}
             (op :start-app))
          (o {:pause-app (op :pause-app)}
             (op :resume-app))
          (o {:partition-one        partition-one-gen
              :partition-leader     partition-leader-gen
              :partition-half       partition-half-gen
              :partition-ring       partition-ring-gen}
             (op :stop-partition))]
         ; For all options relevant for this nemesis, mix them together
         (remove nil?)
         gen/mix
         ; Introduce either random or fixed delays between ops
         ((case (:schedule n)
            (nil :random)    gen/stagger
            :fixed           gen/delay-til)
          (:interval n)))))

(defn full-generator
  "Takes a nemesis options map `n`. If `n` has a :long-recovery option, builds
  a generator which alternates between faults (mixed-generator) and long
  recovery windows (final-generator). Otherwise, just emits faults from
  mixed-generator, or whatever special-case generator we choose."
  [n]
  (cond true
        (mixed-generator n)))

(defn final-generator
  "Takes a nemesis options map `n`, and constructs a generator to stop all
  problems. This generator is called at the end of a test, before final client
  operations."
  [n]
  (->> (cond-> []
         (:pause-app n)        (conj :resume-app)
         (:kill-app n)         (conj :start-app)

         (some n [:partition-one :partition-half :partition-ring])
         (conj :stop-partition))
       (map op)
       gen/seq))

(defn expand-options
  "We support shorthand options in nemesis maps, like :partition, which expands
  to :partition-one, :partition-leader, :partition-half and and :partition-ring.
  This function expands those."
  [n]
  (cond-> n
    (:kill n) (assoc :kill-app true)
    (:stop n) (assoc :kill-app true)
    (:pause n) (assoc :pause-app true)
    (:schedules n) (assoc :shuffle-leader true
                          :shuffle-region true
                          :random-merge true)
    (:partition n) (assoc :partition-one    true
                          :partition-leader true
                          :partition-half   true
                          :partition-ring   true)))

(defn nemesis
  "Composite nemesis and generator, given test options."
  [opts]
  (let [n (expand-options (:nemesis opts))]
    {:nemesis         (full-nemesis)
     :generator       (full-generator n)
     :final-generator (final-generator n)}))
