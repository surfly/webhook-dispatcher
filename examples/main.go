package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	dispatcher "github.com/surfly/webhook-dispatcher"
	"go.etcd.io/bbolt"
)

var D *dispatcher.WebhookDispatcher

func main() {
	var err error
	// Open the BoltDB database.
	db, err := bbolt.Open("webhook_events.db", 0600, nil)
	if err != nil {
		log.Fatalf("Failed to open BoltDB: %v", err)
	}
	defer db.Close()

	// Create a new WebhookDispatcher.
	D, err = dispatcher.NewWebhookDispatcher(db, "my_events")
	if err != nil {
		log.Fatalf("Failed to create WebhookDispatcher: %v", err)
	}

	// Start the WebhookDispatcher.
	D.Start()

	// Set up HTTP handlers.
	http.HandleFunc("/gen/simple/", handleGenerateSimpleEvents)
	log.Println("Starting server on :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}

// handleGenerateSimpleEvents handles HTTP requests to generate and enqueue simple webhook events.
func handleGenerateSimpleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	countStr := r.URL.Path[len("/gen/simple/"):]
	countStr = strings.TrimRight(countStr, "/")
	count, err := strconv.Atoi(countStr)
	if err != nil {
		http.Error(w, "Invalid count", http.StatusBadRequest)
		return
	}

	for i := 0; i < count; i++ {
		event := dispatcher.WebhookEvent{
			Category:  "simple",
			CreatedAt: time.Now(),
			Data:      map[string]interface{}{"message": fmt.Sprintf("Event %d", i+1)},
		}

		err = D.QuickEnqueue(event, "http://localhost:8008/webhook/?verbose=true")
		if err != nil {
			log.Printf("Failed to enqueue event: %v", err)
			http.Error(w, "Failed to enqueue event", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%d events generated and enqueued\n", count)
}
