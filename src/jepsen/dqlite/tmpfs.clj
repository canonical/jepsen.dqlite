(ns jepsen.dqlite.tmpfs
  "Provides a database and nemesis package which can work together to fill up
  disk space."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as nem]
                    [util :as util :refer [meh]]]
            [jepsen.nemesis.combined :as nc]
            [slingshot.slingshot :refer [try+ throw+]]))

(defrecord DB [dir size-mb]
  db/DB
  (setup! [this test node]
    (info "Setting up tmpfs at" dir)
    (c/su (c/exec :mkdir :-p dir)
          (c/exec :chmod 777 dir)
          (c/exec :mount :-t :tmpfs :tmpfs dir
                  :-o (str "size=" size-mb "M,mode=0755"))))

  (teardown! [this test node]
    (info "Unmounting tmpfs at" dir)
    (c/su (meh (c/exec :umount :-l dir)))))

(def balloon-file
  "The name of the file which we use to eat up all available disk space."
  "jepsen-balloon")

(defn fill!
  "Inflates the balloon file, causing the given DB to run out of disk space."
  [db]
  (c/su (try+ (c/exec :dd "if=/dev/zero" (str "of=" (:dir db) "/" balloon-file))
              (catch [:type :jepsen.control/nonzero-exit] e
                ; Normal, disk is full!
                )))
  :filled)

(defn free!
  "Releases the balloon file's data for the given DB."
  [db]
  (c/su (c/exec :rm :-f (str (:dir db) "/" balloon-file)))
  :freed)

(defrecord Nemesis [db]
  nem/Nemesis
  (setup! [this _test] this)

  (invoke! [_this test op]
    (assoc op :value
           (case (:f op)
             :fill-disk (let [targets (nc/db-nodes test (:db test) (:value op))]
                          (c/on-nodes test targets
                                      (fn [_ _] (fill! db))))
             :free-disk (c/on-nodes test
                                    (fn [_ _] (free! db))))))

  (teardown! [_this _test])

  nem/Reflection
  (fs [_this]
    #{:fill-disk :free-disk}))

(defn package
  "Options:
   ```clj
   :faults #{:disk}
   :disk {:targets [nil :one :primaries :majority :all]
          :dir     db/data-dir
          :size-mb 100}
   ```

   Returns:
   ```clj
   {:db              ; tmpfs
    :nemesis         ; disk nemesis for tmpfs
    :generator       ; fill/free disk
    :final-generator ; free disk
    :perf            ; pretty plots            
   ```"
  [{:keys [faults disk interval] :as _opts}]
  (when (:disk faults)
    (let [{:keys [targets dir size-mb]} disk
          _  (assert (string? dir))
          _  (assert (pos? size-mb))
          db (DB. dir size-mb)
          fills (fn [_ _]
                   {:type  :info
                    :f     :fill-disk
                    :value (rand-nth targets)})
          frees (repeat
                  {:type  :info
                   :f     :free-disk
                   :value :all})
          interval (or interval nc/default-interval)]
      {:db              db
       :nemesis         (Nemesis. db)
       :generator       (->> (gen/flip-flop fills frees)
                             (gen/stagger interval))
       :final-generator (gen/once frees)
       :perf            #{{:name  "disk"
                           :start #{:fill-disk}
                           :stop  #{:free-disk}
                           :color "#99DC58"}}})))
