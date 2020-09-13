package main

import (
	"context"
	"database/sql"
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
	"github.com/canonical/go-dqlite/driver"
)

const (
	port   = 8080 // This is the API port, the internal dqlite port+1
	schema = "CREATE TABLE IF NOT EXISTS model (key INT, value INT)"
)

func dqliteLog(l client.LogLevel, format string, a ...interface{}) {
	log.Printf(fmt.Sprintf("%s: %s\n", l.String(), format), a...)
}

func makeAddress(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

// Return the dqlite addresses of all nodes preceeding the given one.
//
// E.g. with node="n2" and cluster="n1,n2,n3" return ["n1:8081"]
func preceedingAddresses(node string, nodes []string) []string {
	preceeding := []string{}
	for i, name := range nodes {
		if name != node {
			continue
		}
		for j := 0; j < i; j++ {
			preceeding = append(preceeding, makeAddress(nodes[j], port+1))
		}
		break
	}
	return preceeding
}

func withTx(db *sql.DB, f func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	if err := f(tx); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func setGet(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, "SELECT value FROM model")
	if err != nil {
		return "", err
	}
	defer rows.Close()

	vals := []string{}
	for i := 0; rows.Next(); i++ {
		var val string
		err := rows.Scan(&val)
		if err != nil {
			return "", err
		}
		vals = append(vals, val)
	}
	err = rows.Err()
	if err != nil {
		return "", err
	}

	return strings.Join(vals, " "), nil
}

func setPost(ctx context.Context, db *sql.DB, value string) (string, error) {
	if _, err := db.ExecContext(ctx, "INSERT INTO model(value) VALUES(?)", value); err != nil {
		return "", err
	}
	return value, nil
}

func appendPost(ctx context.Context, db *sql.DB, value string) (string, error) {
	result := "["

	err := withTx(db, func(tx *sql.Tx) error {
		if value[0] != '[' || value[len(value)-1] != ']' {
			return fmt.Errorf("bad request")
		}

		value = value[1 : len(value)-1]
		for {
			if value[0] != '[' {
				return fmt.Errorf("bad request")
			}
			value = value[1:]
			i := strings.Index(value, "]")
			if i == -1 {
				return fmt.Errorf("bad request")
			}
			op := strings.Split(value[:i], " ")
			if len(op) != 3 {
				return fmt.Errorf("bad request")
			}

			switch op[0] {
			case ":r":
				values := []int{}
				if op[2] != "nil" {
					return fmt.Errorf("bad request")
				}

				rows, err := tx.QueryContext(ctx, "SELECT value FROM model WHERE key=?", op[1])
				if err != nil {
					return err
				}
				defer rows.Close()

				for i := 0; rows.Next(); i++ {
					var val int
					err := rows.Scan(&val)
					if err != nil {
						return err
					}
					values = append(values, val)
				}
				err = rows.Err()
				if err != nil {
					return err
				}

				result += fmt.Sprintf("[:r %s %v]", op[1], values)
			case ":append":
				if _, err := tx.ExecContext(
					ctx, "INSERT INTO model(key, value) VALUES(?, ?)",
					op[1], op[2]); err != nil {
					return err
				}
				result += fmt.Sprintf("[:append %s %s]", op[1], op[2])
			default:
				return fmt.Errorf("bad request")
			}

			if i == len(value)-1 {
				result += "]"
				break
			}

			value = value[i+2:]
			result += " "
		}
		return nil
	})

	if err != nil {
		return "", err
	}

	return result, nil
}

func leaderGet(ctx context.Context, app *app.App) (string, error) {
	cli, err := app.Leader(ctx)
	if err != nil {
		return "", err
	}
	defer cli.Close()

	node, err := cli.Leader(ctx)
	if err != nil {
		return "", err
	}

	leader := ""
	if node != nil {
		addr := strings.Split(node.Address, ":")[0]
		hosts, err := net.LookupAddr(addr)
		if err != nil {
			return "", fmt.Errorf("%q: %v", node.Address, err)
		}
		if len(hosts) != 1 {
			return "", fmt.Errorf("more than one host associated with %s: %v", node.Address, hosts)
		}
		leader = hosts[0]
	}

	return leader, nil
}

func main() {
	dir := flag.String("dir", "", "data directory")
	node := flag.String("node", "", "node name")
	cluster := flag.String("cluster", "", "names of all nodes in the cluster")

	flag.Parse()

	addr, err := net.ResolveIPAddr("ip", *node)
	if err != nil {
		log.Fatalf("resolve node address: %v", err)
	}

	log.Printf("starting %q with IP %q and cluster %q", *node, addr.IP.String(), *cluster)

	nodes := strings.Split(*cluster, ",")

	// Spawn the dqlite server thread.
	app, err := app.New(*dir,
		app.WithAddress(makeAddress(addr.IP.String(), port+1)),
		app.WithCluster(preceedingAddresses(*node, nodes)),
		app.WithLogFunc(dqliteLog),
		app.WithNetworkLatency(5*time.Millisecond),
		app.WithVoters(len(nodes)),
	)
	if err != nil {
		log.Fatalf("create app: %v", err)
	}

	// Wait for the cluster to be stable (possibly joining this node).
	if err := app.Ready(context.Background()); err != nil {
		log.Fatalf("wait app ready: %v", err)
	}

	// Open the app database.
	db, err := app.Open(context.Background(), "app")
	if err != nil {
		log.Fatalf("open database: %v", err)
	}

	// Ensure the SQL schema is there, possibly retrying upon contention.
	for i := 0; i < 10; i++ {
		_, err := db.Exec(schema)
		if err == nil {
			break
		}
		if i == 9 {
			log.Fatalf("create schema: database still locked after 10 retries", err)
		}
		if err, ok := err.(driver.Error); ok && err.Code == driver.ErrBusy {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		log.Fatalf("create schema: %v", err)
	}

	// Handle API requests.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		result := ""
		err := fmt.Errorf("bad request")

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		switch r.URL.Path {
		case "/set":
			switch r.Method {
			case "GET":
				result, err = setGet(ctx, db)
			case "POST":
				value, _ := ioutil.ReadAll(r.Body)
				result, err = setPost(ctx, db, string(value))
			}
		case "/append":
			switch r.Method {
			case "POST":
				value, _ := ioutil.ReadAll(r.Body)
				result, err = appendPost(ctx, db, string(value))
			}
		case "/leader":
			switch r.Method {
			case "GET":
				result, err = leaderGet(ctx, app)
			}
		}
		if err != nil {
			result = fmt.Sprintf("Error: %s", err.Error())
		}
		fmt.Fprintf(w, "%s", result)
	})

	listener, err := net.Listen("tcp", makeAddress(addr.IP.String(), port))
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
