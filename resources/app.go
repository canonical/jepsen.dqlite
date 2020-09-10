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
	"syscall"

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
	node := flag.String("node", "", "node host name")
	join := []string{}

	flag.Parse()

	addr, err := net.ResolveIPAddr("ip", *node)
	if err != nil {
		log.Fatalf("resolve node address: %v", err)
	}

	logFunc := func(l client.LogLevel, format string, a ...interface{}) {
		log.Printf(fmt.Sprintf("%s: %s\n", l.String(), format), a...)
	}

	switch *node {
	case "n2":
		join = []string{"n1:8081"}
	case "n3":
		join = []string{"n1:8081", "n2:8081"}
	case "n4":
		join = []string{"n1:8081", "n2:8081", "n3:8081"}
	case "n5":
		join = []string{"n1:8081", "n2:8081", "n3:8081", "n4:8081"}
	}

	app, err := app.New(*dir,
		app.WithAddress(makeAddress(addr, 8081)),
		app.WithCluster(join),
		app.WithLogFunc(logFunc))
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

	if _, err := db.Exec(schema); err != nil {
		log.Fatalf("create schema: %v", err)
	}

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		result := ""
		switch r.Method {
		case "GET":
			rows, err := db.Query("SELECT val FROM test_set")
			if err != nil {
				result = fmt.Sprintf("Error: %s", err.Error())
			}
			defer rows.Close()
			for i := 0; rows.Next(); i++ {
				var val string
				err := rows.Scan(&val)
				if err != nil {
					result = fmt.Sprintf("Error: %s", err.Error())
					break
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
			if _, err := db.Exec("INSERT INTO test_set(val) VALUES(?)", value); err != nil {
				result = fmt.Sprintf("Error: %s", err.Error())
			}
		}
		fmt.Fprintf(w, "%s\n", result)
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
