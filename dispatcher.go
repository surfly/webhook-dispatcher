package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

var (
	defaultBucketName    = "webhook_events"
	defaultUserAgent     = "WebhookDispatcher/1.0"
	defaultReqTimeout    = 10 * time.Second
	defaultEventCooldown = 60 * time.Second
	defaultConcurrency   = 1
)

// WebhookDispatcher manages event delivery.
type WebhookDispatcher struct {
	db *bbolt.DB

	// bucketName is the name of the bucket in which events are stored.
	bucketName []byte

	// user-agent to use for webhook requests
	reqUserAgent string
	// reqTimeout for webhook requests, seconds
	reqTimeout time.Duration
	// default eventCooldown time for failed event delivery, time.Duration
	eventCooldown time.Duration

	// number of events to process concurrently
	concurrency int

	queue      chan string
	inProgress sync.Map
}

// NewWebhookDispatcher creates a new WebhookDispatcher. Pass *bbolt.DB instance
// which will be used to store events.
func NewWebhookDispatcher(db *bbolt.DB, bucketName string) (*WebhookDispatcher, error) {
	if bucketName == "" {
		bucketName = defaultBucketName
	}
	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		return nil, err
	}

	d := &WebhookDispatcher{
		db:            db,
		bucketName:    []byte(bucketName),
		reqUserAgent:  defaultUserAgent,
		concurrency:   defaultConcurrency,
		reqTimeout:    defaultReqTimeout,
		eventCooldown: defaultEventCooldown,
	}
	d.queue = make(chan string, d.concurrency*2)
	d.inProgress = sync.Map{}
	return d, nil
}

// Start starts the dispatcher's workers and the database monitor.
func (d *WebhookDispatcher) Start() {
	for range d.concurrency {
		go d.worker()
	}
	go d.monitorDB()
}

// worker processes webhook tasks.
func (d *WebhookDispatcher) worker() {
	for eventID := range d.queue {
		// Mark as in progress *before* retrieving from DB
		if _, loaded := d.inProgress.LoadOrStore(eventID, true); loaded {
			// Another worker is already processing this event, skip it
			continue
		}

		event, err := d.getEventFromDB(eventID)
		if err != nil {
			log.Printf("ERROR: Retrieving event %s: %v", eventID, err)
			d.inProgress.Delete(eventID) // Remove from inProgress
			continue
		}

		jsonPayload, err := json.Marshal(event.WebhookEvent)
		if err != nil {
			log.Printf("ERROR: Encoding payload for %s: %v", event.EventID, err)
			d.inProgress.Delete(eventID) // Clean up
			continue
		}

		err = d.sendWebhook(event.URL, jsonPayload)
		if err != nil {
			log.Printf("ERROR: Sending webhook to %s: %v", event.URL, err)
			event.RetryCount++
			event.LastUpdatedAt = time.Now()
			nextRetryAt, err := GetNextRetryTime(&event, d.eventCooldown)
			if err != nil {
				log.Printf("ERROR: Getting next retry time for %s: %v", event.EventID, err)
				if err := d.deleteEventFromDB(eventID); err != nil {
					log.Printf("ERROR: Deleting event %s: %v", eventID, err)
				}
			} else {
				event.RetryAfter = nextRetryAt
				if err := d.saveEventInDB(&event); err != nil {
					log.Printf("ERROR: Saving event %s after retry update: %v", event.EventID, err)
				}
			}
		} else {
			if err := d.deleteEventFromDB(eventID); err != nil {
				log.Printf("ERROR: Deleting event %s: %v", eventID, err)
			}
			log.Printf("Successfully sent webhook to %s", event.URL)
		}

		// After sending (successful or not), remove from inProgress and update status
		d.inProgress.Delete(eventID)
	}
}

// QuickEnqueue adds an event to the queue with the given URL.
func (d *WebhookDispatcher) QuickEnqueue(event WebhookEvent, url string) error {
	queuedEvent := NewQueuedEvent(event, url)
	if queuedEvent.EventID == "" {
		// generate a new time-ordered UUID
		uuid, err := uuid.NewV7()
		if err != nil {
			return err
		}
		queuedEvent.EventID = uuid.String()
	}

	return d.saveEventInDB(queuedEvent)
}

// getEventFromDB retrieves the event from the database.
func (d *WebhookDispatcher) getEventFromDB(eventID string) (QueuedEvent, error) {
	var event QueuedEvent
	err := d.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(d.bucketName)
		data := bucket.Get([]byte(eventID))
		if data == nil {
			return nil
		}
		return json.Unmarshal(data, &event)
	})
	return event, err
}

// deleteEventFromDB deletes the event from the database.
func (d *WebhookDispatcher) deleteEventFromDB(eventID string) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(d.bucketName)
		return bucket.Delete([]byte(eventID))
	})
}

// saveEventInDB saves the event in the database. EventID is used as the key.
func (d *WebhookDispatcher) saveEventInDB(event *QueuedEvent) error {
	jsonData, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return d.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(d.bucketName)
		return bucket.Put([]byte(event.EventID), jsonData)
	})
}

// monitorDB monitors the database for new events and sends their IDs to the queue.
func (d *WebhookDispatcher) monitorDB() {
	for {
		// Use a transaction to get a consistent view of the data.
		err := d.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(d.bucketName))
			if b == nil {
				return nil // No events bucket, nothing to do.
			}

			// Iterate over all keys (event IDs) in the bucket.
			return b.ForEach(func(k, v []byte) error {
				eventID := string(k) // Convert key to string
				// Check if the event is already in progress
				if _, loaded := d.inProgress.Load(eventID); loaded {
					return nil // Skip if already in progress
				}
				select {
				case d.queue <- eventID:
					// Send to queue
				default: //Queue is full
					log.Printf("Queue is full, cannot send eventID %v", eventID)
				}
				return nil
			})
		})

		if err != nil {
			log.Printf("ERROR: Monitoring database: %v", err)
		}

		time.Sleep(1 * time.Second) // Check for new events every second
	}
}

func (d *WebhookDispatcher) sendWebhook(url string, payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), d.reqTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("User-Agent", d.reqUserAgent)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("error: received status code %d", resp.StatusCode)
}
