package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	//"time"

	"github.com/google/uuid"
	"github.com/ogbanugot/distributed-worker/messages"
	"github.com/ogbanugot/distributed-worker/task"
)

func main() {
	clusterip := flag.String("clusterip", "127.0.0.1:8001", "ip address of server")
	flag.Parse()

	myIp, _ := net.InterfaceAddrs()

	/* Try to connect to the cluster, and send request to cluster if able to connect */
	fmt.Println("Initiating client. Connecting to cluster.")
	queuename := connectToCluster(myIp[0].String(), *clusterip)
	StartConsumer(queuename)
}

func connectToCluster(myIp string, clusterip string) (queueName string) {
	url := fmt.Sprintf("http://%s/registerworker", clusterip)
	fmt.Println("URL: ", url)
	workerNode := messages.NodeInfo{NodeId: uuid.New().String(), NodeIpAddr: myIp, Port: "9001"}

	fmt.Println("Json req:", workerNode)
	var buf []byte
	buf, _ = json.Marshal(workerNode)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(buf))

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, _ := client.Do(req)
	decoder := json.NewDecoder(resp.Body)

	var n messages.NodeInfo
	decoder.Decode(&n)
	fmt.Println("QueueName : ", n.QueueName)
	return n.QueueName
}

func StartConsumer(queueName string) {

	Redis, QueueFactory, _ := task.NewClient()

	RQueue := task.NewQueue(Redis, QueueFactory, queueName)
	consumer := RQueue.Queue.Consumer()

	err := consumer.Start(context.TODO())
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	sig := task.WaitSignal()
	log.Println(sig.String())

	err = QueueFactory.Close()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
}
