package main

import (
	"log"
	"net/http"
	"os"

	"github.com/zzszmyf/rsi-state/evolution"
)

func main() {
	address := os.Getenv("RSI_STATE_ADDR")
	if address == "" {
		address = ":8080"
	}

	service := evolution.NewService(evolution.NewInMemoryStore(), nil)
	log.Printf("rsi-state listening on %s", address)
	if err := http.ListenAndServe(address, evolution.NewHTTPServer(service)); err != nil {
		log.Fatal(err)
	}
}
