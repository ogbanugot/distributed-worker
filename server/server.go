package main

/* All useful imports */
import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/ogbanugot/distributed-worker/messages"
	"github.com/ogbanugot/distributed-worker/task"
)

func main() {
	/* Parse the provided parameters on command line */
	clusterip := flag.String("clusterip", "127.0.0.1", "ip address of server")
	port := flag.String("myport", ":8001", "port to run on. default is 8001.")

	flag.Parse()

	fmt.Println("Starting http server")
	fmt.Println("cluster ip :", *clusterip)
	fmt.Println("Port :", *port)

	Redis, QueueFactory, _ := task.NewClient()

	RQueue := task.NewQueue(Redis, QueueFactory, "EventQueue2")

	go func() {
		for i := 0; i < 2000000000; i++ {
			text := fmt.Sprint(i, "message")
			msg := task.SimpleTask.WithArgs(context.Background(), text)
			msg.Name = fmt.Sprint(i, "message")
			err := RQueue.Queue.Add(msg)
			if err != nil {
				log.Fatal(err)
			}
		}
	}()
	startHttpServer(*port)
}

func startHttpServer(port string) {
	fmt.Println("Starting http server.")
	http.HandleFunc("/registerworker", queryHandler)
	http.ListenAndServe(port, nil)
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var n messages.NodeInfo
	decoder.Decode(&n)
	fmt.Println("Add node to cluster : ", n.NodeId)

	n.QueueName = "EventQueue2"

	responseJson := n
	json.NewEncoder(w).Encode(responseJson)
}
