module jepsen.dqlite

go 1.13

require (
	github.com/canonical/go-dqlite v1.11.7
	golang.org/x/sync v0.1.0
)

replace github.com/canonical/go-dqlite v1.11.7 => github.com/cole-miller/go-dqlite dbg-cantopen
