package main

import (
	"fmt"
	"net/http"
	"time"
)

const _numNodes = 4

func main() {
	cluster := RunCluster(_numNodes)

	handler := NewHandler(cluster)
	handler.RegisterAllHandlers()

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		panic("failed to start the http server")
	}

	fmt.Println("http server started")

	// TODO replace this with a blocking call
	time.Sleep(time.Minute * 2)
}
