(defproject jepsen.dqlite "0.1.0-SNAPSHOT"
  :description "A Jepsen test for dqlite"
  :url "http://github.com/free.ekanayaka/jepsen.dqlite"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main jepsen.dqlite
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.1.19"]
                 [clj-http "3.10.1"]]
  :repl-options {:init-ns jepsen.dqlite}
  :jvm-opts ["-Xmx2g"])
