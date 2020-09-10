package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
)

const (
	schema = "CREATE TABLE IF NOT EXISTS test_set (val INT)"
)

func makeAddress(addr *net.IPAddr, port int) string {
	return fmt.Sprintf("%s:%d", addr.IP.String(), port)
}

func main() {
	dir := flag.String("dir", "", "data directory")
	node := flag.String("node", "", "node name")
	cluster := flag.String("cluster", "", "names of all nodes in the cluster")
	join := []string{}

	flag.Parse()

	addr, err := net.ResolveIPAddr("ip", *node)
	if err != nil {
		log.Fatalf("resolve node address: %v", err)
	}

	log.Printf("starting %q with IP %q and cluster %q", *node, addr.IP.String(), *cluster)

	logFunc := func(l client.LogLevel, format string, a ...interface{}) {
		log.Printf(fmt.Sprintf("%s: %s\n", l.String(), format), a...)
	}

	// Figure out the nodes to use for joining.
	nodes := strings.Split(*cluster, ",")
	for i, name := range nodes {
		if name == *node {
			for j := 0; j < i; j++ {
				join = append(join, fmt.Sprintf("%s:8081", nodes[j]))
			}
			break
		}
	}

	app, err := app.New(*dir,
		app.WithAddress(makeAddress(addr, 8081)),
		app.WithCluster(join),
		app.WithLogFunc(logFunc),
		app.WithNetworkLatency(5*time.Millisecond),
		app.WithVoters(len(nodes)),
	)
	if err != nil {
		log.Fatalf("create app: %v", err)
	}

	if err := app.Ready(context.Background()); err != nil {
		log.Fatalf("wait app ready: %v", err)
	}

	db, err := app.Open(context.Background(), "demo")
	if err != nil {
		log.Fatalf("open database: %v", err)
	}

	for i := 0; i < 10; i++ {
		_, err := db.Exec(schema)
		if err == nil {
			break
		}
		if err.Error() != "database is locked" {
			log.Fatalf("create schema: %v", err)
		}
		if i == 9 {
			log.Fatalf("create schema: database still locked after 10 retries", err)
		}
		time.Sleep(250 * time.Millisecond)
	}

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		result := ""
		switch r.Method {
		case "GET":
			rows, err := db.QueryContext(ctx, "SELECT val FROM test_set")
			if err != nil {
				result = fmt.Sprintf("Error: %s", err.Error())
				goto done
			}
			defer rows.Close()
			for i := 0; rows.Next(); i++ {
				var val string
				err := rows.Scan(&val)
				if err != nil {
					result = fmt.Sprintf("Error: %s", err.Error())
					goto done
				}
				if i == 0 {
					result = fmt.Sprintf("%s", val)
				} else {
					result += fmt.Sprintf(" %s", val)
				}
			}
			err = rows.Err()
			if err != nil {
				result = fmt.Sprintf("Error: %s", err.Error())
			}
		case "POST":
			result = "done"
			value, _ := ioutil.ReadAll(r.Body)
			if _, err := db.ExecContext(ctx, "INSERT INTO test_set(val) VALUES(?)", value); err != nil {
				result = fmt.Sprintf("Error: %s", err.Error())
			}
		}
	done:
		fmt.Fprintf(w, "%s", result)
	})

	listener, err := net.Listen("tcp", makeAddress(addr, 8080))
	if err != nil {
		log.Fatalf("listen to API address: %v", err)
	}
	go http.Serve(listener, nil)

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT)
	signal.Notify(ch, syscall.SIGTERM)

	<-ch

	db.Close()
	app.Close()
}
