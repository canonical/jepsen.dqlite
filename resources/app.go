package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
)

const dir = "/opt/db"

func main() {
	db := flag.String("db", "", "db replication address")

	flag.Parse()

	logFunc := func(l client.LogLevel, format string, a ...interface{}) {
		log.Printf(fmt.Sprintf("%s: %s\n", l.String(), format), a...)
	}
	addr, err := net.ResolveIPAddr("ip", *db)
	if err != nil {
		log.Fatal(err)
	}

	app, err := app.New(dir, app.WithAddress(fmt.Sprintf("%s:8080", addr.IP.String())), app.WithLogFunc(logFunc))
	if err != nil {
		log.Fatal(err)
	}

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT)
	signal.Notify(ch, syscall.SIGTERM)

	<-ch

	app.Close()
}
