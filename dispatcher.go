package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
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
	defaultRetrySchedule = []string{"0s", "5s", "10s", "30s", "1m", "30m", "1h", "3h", "6h", "12h", "24h"}
	// Default maximum retry count for failed events. After this count, the event will be deleted.
	defaultMaxRetryCount = 30

	// monitorDBInterval is the interval for monitoring the database for new events.
	monitorDBInterval = 1 * time.Second
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

	logger     *log.Logger
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
		logger:        log.New(io.Discard, "", 0),
	}
	d.queue = make(chan string, d.concurrency*2)
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
		ctx := context.Background()
		ctx = context.WithValue(ctx, "workerID", i)
		ctx = context.WithValue(ctx, "loggerOutput", d.logger.Writer())
		go d.worker(ctx)
	}
	ctx := context.Background()
	ctx = context.WithValue(ctx, "loggerOutput", d.logger.Writer())
	go d.monitorDB(ctx)
}

// worker processes webhook tasks.
func (d *WebhookDispatcher) worker(ctx context.Context) {
	workerID := ctx.Value("workerID").(int)
	logger := log.New(ctx.Value("loggerOutput").(io.Writer), fmt.Sprintf("[workerID=%d] ", workerID), 0)
	logger.Println("Worker started")
	defer logger.Println("Worker stopped")
	for eventID := range d.queue {
		d.handleEvent(ctx, eventID)
	}
}

func (d *WebhookDispatcher) handleEvent(ctx context.Context, eventID string) {
	logOutput := ctx.Value("loggerOutput").(io.Writer)
	workerID := ctx.Value("workerID").(int)
	logger := log.New(logOutput, fmt.Sprintf("[workerID=%d] [eventID=%s] ", workerID, eventID), 0)

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
		return
	}

	err = d.sendWebhook(event.URL, jsonPayload)
	if err != nil {
		logger.Printf("Unable to send webhook to %s: %v", event.URL, err)
		event.RetryCount++
		nextRetryAt, err := GetNextRetryTime(&event, d.eventCooldown)
		if err != nil {
			logger.Printf("Unable to get next retry time for %s: %v", event.EventID, err)
			if err := d.deleteEventFromDB(eventID); err != nil {
				logger.Printf("Removing event %s: %v", eventID, err)
			}
		} else {
			event.RetryAfter = nextRetryAt
			err = d.saveEventInDB(&event)
			if err != nil {
				logger.Printf("Unable to save event %s: %v", eventID, err)
			}
		}
	} else {
		// Event was successfully sent, delete it from the database
		if err := d.deleteEventFromDB(eventID); err != nil {
			logger.Println("Unable to delete event from DB:", err)
		}
		logger.Printf("Successfully sent webhook to %s", event.URL)
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
	queuedEvent.MaxRetryCount = defaultMaxRetryCount
	queuedEvent.RetrySchedule = defaultRetrySchedule

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
func (d *WebhookDispatcher) monitorDB(ctx context.Context) {
	logger := log.New(ctx.Value("loggerOutput").(io.Writer), "[monitorDB] ", 0)
	for {
		err := d.db.View(func(tx *bbolt.Tx) error {
			// Iterate over all keys (event IDs) in the bucket.
			return tx.Bucket([]byte(d.bucketName)).ForEach(func(k, v []byte) error {
				eventID := string(k) // Convert key to string
				// Check if the event is already in progress
				if _, loaded := d.inProgress.Load(eventID); loaded {
					return nil // Skip if already in progress
				}
				select {
				case d.queue <- eventID:
					// Send to queue
				default: // Queue is full
				}
				return nil
			})
		})

		if err != nil {
			logger.Printf("ERROR: Monitoring database: %v", err)
		}

		time.Sleep(monitorDBInterval)
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
