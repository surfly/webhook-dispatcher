package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"go.etcd.io/bbolt"
)

var (
	// Bucket name to store queued events
	defaultBucketName = "webhook_events"
	// Default user-agent for webhook requests
	defaultUserAgent = "WebhookDispatcher/1.0"
	// Default request timeout for webhook requests
	defaultReqTimeout = 10 * time.Second
	// Default event cooldown time for failed event delivery
	defaultEventCooldown = 60 * time.Second
	// Default concurrency for processing events
	defaultConcurrency = 2
	// Default retry schedule for failed events
	defaultRetrySchedule = []string{"0s", "5s", "10s", "30s", "1m", "5m", "10m", "15m", "30m", "1h", "3h", "6h", "12h", "24h"}
	// Default maximum retry count for failed events. After this count, the event will be deleted.
	defaultMaxRetryCount = 30

	// monitorDBInterval is the interval for monitoring the database for new events.
	monitorDBInterval = 1 * time.Second

	// compressionThreshold is the size threshold for compressing the payload.
	compressionThreshold = 1024 // 1KB
)

type contextKey string

var workerIDKey = contextKey("workerID")

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

	logger *log.Logger

	sendEventQueue chan string

	inProgress sync.Map

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
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

	ctx, cancel := context.WithCancel(context.Background())

	d := &WebhookDispatcher{
		db:            db,
		bucketName:    []byte(bucketName),
		reqUserAgent:  defaultUserAgent,
		concurrency:   defaultConcurrency,
		reqTimeout:    defaultReqTimeout,
		eventCooldown: defaultEventCooldown,
		logger:        log.New(io.Discard, "", 0),
		ctx:           ctx,
		cancel:        cancel,
	}
	d.sendEventQueue = make(chan string, d.concurrency*2)
	d.inProgress = sync.Map{}
	return d, nil
}

// SetUserAgent sets the user-agent for webhook requests.
func (d *WebhookDispatcher) SetUserAgent(ua string) {
	d.reqUserAgent = ua
}

// SetReqTimeout sets the request timeout for webhook requests.
func (d *WebhookDispatcher) SetReqTimeout(t time.Duration) {
	d.reqTimeout = t
}

// SetConcurrency sets the number of events to process concurrently (number of workers).
func (d *WebhookDispatcher) SetConcurrency(c int) {
	d.concurrency = c
}

// SetLogger sets the logger for the dispatcher.
func (d *WebhookDispatcher) SetLogger(logger *log.Logger) {
	d.logger = logger
}

// Start is the entry point for the dispatcher. It starts the workers and monitors the database.
func (d *WebhookDispatcher) Start() {
	d.logger.Printf("Starting WebhookDispatcher with %d workers", d.concurrency)
	for i := range d.concurrency {
		d.wg.Add(1) // Increment the WaitGroup counter
		ctx := context.WithValue(d.ctx, workerIDKey, i)
		go d.worker(ctx)
	}
	go d.monitorDB()
}

// worker processes webhook tasks.
func (d *WebhookDispatcher) worker(ctx context.Context) {
	workerID := ctx.Value(workerIDKey).(int)
	logger := log.New(d.logger.Writer(), fmt.Sprintf("%s[workerID=%d] ", d.logger.Prefix(), workerID), d.logger.Flags())
	logger.Println("Worker started")
	defer func() {
		d.wg.Done() // Decrement the WaitGroup counter when the worker finishes
	}()

	for {
		select {
		case <-ctx.Done():
			logger.Println("Worker received stop signal")
			return
		case eventID, ok := <-d.sendEventQueue:
			if !ok {
				return
			}
			d.handleEvent(ctx, eventID)
		}
	}
}

func (d *WebhookDispatcher) handleEvent(ctx context.Context, eventID string) {
	workerID := ctx.Value(workerIDKey).(int)
	logger := log.New(d.logger.Writer(), fmt.Sprintf("%s[workerID=%d] [eventID=%s] ", d.logger.Prefix(), workerID, eventID), d.logger.Flags())

	logger.Printf("Processing event")
	startedAt := time.Now()

	defer func() {
		d.inProgress.Delete(eventID)
		logger.Printf("[took=%s] Finished processing event", time.Since(startedAt))
	}()

	// Mark as in progress *before* retrieving from DB
	if _, loaded := d.inProgress.LoadOrStore(eventID, true); loaded {
		// Another worker is already processing this event, skip it
		logger.Println("Skipping event, already in progress")
		return
	}

	event, err := d.getEventFromDB(eventID)
	if err != nil {
		logger.Printf("Unable to get event from DB: %v", err)
		return
	}

	jsonPayload, err := json.Marshal(event.WebhookEvent)
	if err != nil {
		logger.Printf("Unable to marshal event: %v", err)
		if err = d.deleteEventFromDB(eventID); err != nil {
			logger.Printf("Error deleting event from DB: %v", err)
		}
		return
	}

	logger.Printf("Sending webhook to %s", event.URL)
	err = d.sendWebhook(ctx, event.URL, jsonPayload)
	if err != nil {
		event.RetryCount++
		logger.Printf("[attempt=%d] Unable to send [send_error=%s]", event.RetryCount, err)
		nextRetryAt, err := GetNextRetryTime(&event, d.eventCooldown)
		if err != nil {
			logger.Printf("Unable to get next retry time for %s: %v", event.EventID, err)
			if err = d.deleteEventFromDB(eventID); err != nil {
				logger.Printf("Error deleting event from DB: %v", err)
			}
		} else {
			event.RetryAfter = nextRetryAt
			logger.Printf("Next retry in %s", nextRetryAt.Sub(time.Now()))
			err = d.saveEventInDB(&event)
			if err != nil {
				logger.Printf("Unable to save event %s: %v", eventID, err)
			}
		}
	} else {
		logger.Printf("Successfully sent webhook to %s", event.URL)
		if err = d.deleteEventFromDB(eventID); err != nil {
			logger.Printf("Error deleting event from DB: %v", err)
		}
	}
}

// QuickEnqueue adds a new webhook event to the queue.
// - url - is the webhook URL to which this event should be sent.
// - category - is the category of the event.
// - data - is the data to be sent in the webhook payload. Should be JSON serializable.
func (d *WebhookDispatcher) QuickEnqueue(url string, category string, data any) error {
	// Generate a new time-ordered UUID
	uuid, err := uuid.NewV7()
	if err != nil {
		return err
	}

	event := WebhookEvent{
		Category:  category,
		CreatedAt: time.Now(),
		Data:      data,
		EventID:   uuid.String(),
	}

	queuedEvent := NewQueuedEvent(event, url)

	return d.saveEventInDB(queuedEvent)
}

// Enqueue adds a new webhook event to the queue. Make sure eventID is unique.
func (d *WebhookDispatcher) Enqueue(event *QueuedEvent) error {
	return d.saveEventInDB(event)
}

// getEventFromDB retrieves the event from the database.
func (d *WebhookDispatcher) getEventFromDB(eventID string) (QueuedEvent, error) {
	var event QueuedEvent
	err := d.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(d.bucketName)
		data := bucket.Get([]byte(eventID))
		if data == nil {
			return fmt.Errorf("event not found")
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
	logger := log.New(d.logger.Writer(), fmt.Sprintf("%s[kind=monitorDB] ", d.logger.Prefix()), d.logger.Flags())
	logger.Println("Monitoring database for new events")

	ticker := time.NewTicker(monitorDBInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			err := d.db.View(func(tx *bbolt.Tx) error {
				// Iterate over all keys (event IDs) in the bucket.
				return tx.Bucket([]byte(d.bucketName)).ForEach(func(k, v []byte) error {
					eventID := string(k) // Convert key to string

					// Check if the event is already being processed by a worker.
					if _, loaded := d.inProgress.Load(eventID); loaded {
						return nil // Skip if already in progress
					}

					// Check if it is time to send the event (retry after time has passed).
					var event QueuedEvent
					if err := json.Unmarshal(v, &event); err != nil {
						logger.Printf("Unable to unmarshal event: %v", err)
						if err = d.deleteEventFromDB(eventID); err != nil {
							logger.Printf("Error deleting event from DB: %v", err)
						}
						return nil
					}

					if event.RetryAfter.After(time.Now()) {
						return nil // Skip if not ready to be sent
					}

					select {
					case d.sendEventQueue <- eventID:
						// Send to queue
					case <-d.ctx.Done():
						return fmt.Errorf("context canceled")
					default: // Queue is full
					}
					return nil
				})
			})

			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Printf("ERROR: Monitoring database: %v", err)
			}
		}
	}
}

func (d *WebhookDispatcher) sendWebhook(ctx context.Context, url string, payload []byte) error {
	compressed := false
	if len(payload) > compressionThreshold {
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
		if err != nil {
			return fmt.Errorf("error creating zstd encoder: %w", err)
		}
		payload = encoder.EncodeAll(payload, nil)
		if err := encoder.Close(); err != nil {
			return fmt.Errorf("error closing zstd encoder: %w", err)
		}
		compressed = true
	}

	reqCtx, cancel := context.WithTimeout(ctx, d.reqTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, "POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("User-Agent", d.reqUserAgent)
	req.Header.Set("Content-Type", "application/json")
	if compressed {
		req.Header.Set("Content-Encoding", "zstd")
	}

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

// Stop gracefully shuts down the dispatcher.
func (d *WebhookDispatcher) Stop() {
	d.logger.Println("Stopping WebhookDispatcher...")

	// Signal all goroutines to stop
	d.cancel()

	// Wait for all workers to finish
	d.wg.Wait()

	// Now it's safe to close the channels since no workers are running
	close(d.sendEventQueue)

	d.logger.Println("WebhookDispatcher stopped")
}
