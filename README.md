# Dqlite Jepsen Test

A Clojure library designed to test Dqlite, an embedded SQL database with Raft
consensus.

## What is being tested?

The tests run concurrent operations to some shared data from different nodes in
a Dqlite cluster, checking that the operations preserve the consistency
properties defined in each test.  During the tests, various combinations of
nemeses can be added to interfere with the database operations and exercise the
database's consistency protocols.

## Running

To run a single test, try

```
lein run test --workload sets --nemesis kill --time-limit 60 --test-count 1 --concurrency 2n
```
