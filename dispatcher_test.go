package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.etcd.io/bbolt"
)

func setupTestDB(t *testing.T) *bbolt.DB {
	// Create a temporary database for testing
	dbFile, err := os.CreateTemp("", "testdb")
	if err != nil {
		t.Fatalf("Failed to create temporary database file: %v", err)
	}
	dbFile.Close() // Close the file, bbolt will open it.
	db, err := bbolt.Open(dbFile.Name(), 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	return db
}

func cleanupTestDB(t *testing.T, db *bbolt.DB) {
	// Close the database and remove the temporary file
	dbPath := db.Path()
	err := db.Close()
	if err != nil {
		t.Errorf("Failed to close database: %v", err)
	}
	err = os.Remove(dbPath)
	if err != nil {
		t.Errorf("Failed to remove temporary database file: %v", err)
	}
}

func TestSendWebhook(t *testing.T) {
	testCases := []struct {
		name             string
		payload          []byte
		responseStatus   int
		compressPayload  bool
		checkCompression bool
		expectError      bool
		checkHeaders     bool
	}{
		{
			name:            "successful request",
			payload:         []byte(`{"key": "value"}`),
			responseStatus:  http.StatusOK,
			compressPayload: false,
			expectError:     false,
			checkHeaders:    true,
		},
		{
			name:             "successful request with compression",
			payload:          bytes.Repeat([]byte(`{"key": "value"}`), 500), // large payload to trigger compression
			responseStatus:   http.StatusOK,
			compressPayload:  true,
			expectError:      false,
			checkCompression: true,
			checkHeaders:     true,
		},
		{
			name:            "unsuccessful request",
			payload:         []byte(`{"key": "value"}`),
			responseStatus:  http.StatusInternalServerError,
			compressPayload: false,
			expectError:     true,
			checkHeaders:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mock HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.checkHeaders {
					if r.Header.Get("User-Agent") != defaultUserAgent {
						t.Errorf("Expected User-Agent header to be %q, got %q", defaultUserAgent, r.Header.Get("User-Agent"))
					}
					if r.Header.Get("Content-Type") != "application/json" {
						t.Errorf("Expected Content-Type header to be %q, got %q", "application/json", r.Header.Get("Content-Type"))
					}
					if tc.checkCompression {
						if r.Header.Get("Content-Encoding") != "zstd" {
							t.Errorf("Expected Content-Encoding header to be %q, got %q", "zstd", r.Header.Get("Content-Encoding"))
						}
					}
				}

				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatalf("Failed to read request body: %v", err)
				}

				if tc.checkCompression {
					reader, err := zstd.NewReader(bytes.NewReader(body))
					if err != nil {
						t.Fatalf("Failed to create zstd reader: %v", err)
					}
					defer reader.Close()
					body, err = io.ReadAll(reader)
					if err != nil {
						t.Fatalf("Failed to read compressed body: %v", err)
					}
				}

				if !bytes.Equal(body, tc.payload) {
					t.Errorf("Expected payload %q, got %q", string(tc.payload), string(body))
				}

				w.WriteHeader(tc.responseStatus)
			}))
			defer server.Close()

			// Create a mock dispatcher
			dispatcher := &WebhookDispatcher{
				reqUserAgent: defaultUserAgent,
				reqTimeout:   defaultReqTimeout,
				logger:       log.New(io.Discard, "", 0),
			}

			// Call sendWebhook
			err := dispatcher.sendWebhook(context.TODO(), server.URL, tc.payload)

			// Check for error
			if tc.expectError && err == nil {
				t.Errorf("Expected error, got nil")
			}
			if !tc.expectError && err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		})
	}
}
func TestNewWebhookDispatcher(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	t.Run("with default bucket name", func(t *testing.T) {
		dispatcher, err := NewWebhookDispatcher(db, "")
		if err != nil {
			t.Fatalf("Failed to create WebhookDispatcher: %v", err)
		}
		if string(dispatcher.bucketName) != defaultBucketName {
			t.Errorf("Expected bucket name %q, got %q", defaultBucketName, dispatcher.bucketName)
		}

		// Check if the bucket exists
		err = db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket([]byte(defaultBucketName))
			if bucket == nil {
				return fmt.Errorf("Bucket %q does not exist", defaultBucketName)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Error checking bucket existence: %v", err)
		}
	})

	t.Run("with custom bucket name", func(t *testing.T) {
		customBucketName := "custom_bucket"
		dispatcher, err := NewWebhookDispatcher(db, customBucketName)
		if err != nil {
			t.Fatalf("Failed to create WebhookDispatcher: %v", err)
		}
		if string(dispatcher.bucketName) != customBucketName {
			t.Errorf("Expected bucket name %q, got %q", customBucketName, dispatcher.bucketName)
		}

		// Check if the bucket exists
		err = db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket([]byte(customBucketName))
			if bucket == nil {
				return fmt.Errorf("Bucket %q does not exist", customBucketName)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Error checking bucket existence: %v", err)
		}
	})
}

func TestQuickEnqueue(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	dispatcher, err := NewWebhookDispatcher(db, "")
	if err != nil {
		t.Fatalf("Failed to create WebhookDispatcher: %v", err)
	}

	url := "http://example.com/webhook"
	category := "test_category"
	data := map[string]string{"key": "value"}

	err = dispatcher.QuickEnqueue(url, category, data)
	if err != nil {
		t.Fatalf("Failed to enqueue event: %v", err)
	}

	// Check that at least one event is in the database
	var eventCount int
	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(dispatcher.bucketName)
		return bucket.ForEach(func(k, v []byte) error {
			eventCount++
			var event QueuedEvent
			if err := json.Unmarshal(v, &event); err != nil {
				return err
			}
			if event.URL != url {
				t.Errorf("Expected URL %q, got %q", url, event.URL)
			}
			if event.Category != category {
				t.Errorf("Expected category %q, got %q", category, event.Category)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("Failed to count events: %v", err)
	}

	if eventCount != 1 {
		t.Errorf("Expected 1 event in database, got %d", eventCount)
	}
}

func TestSettingMethods(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	dispatcher, err := NewWebhookDispatcher(db, "")
	if err != nil {
		t.Fatalf("Failed to create WebhookDispatcher: %v", err)
	}

	t.Run("SetUserAgent", func(t *testing.T) {
		customUA := "CustomUserAgent/1.0"
		dispatcher.SetUserAgent(customUA)
		if dispatcher.reqUserAgent != customUA {
			t.Errorf("Expected user agent %q, got %q", customUA, dispatcher.reqUserAgent)
		}
	})

	t.Run("SetReqTimeout", func(t *testing.T) {
		customTimeout := 30 * time.Second
		dispatcher.SetReqTimeout(customTimeout)
		if dispatcher.reqTimeout != customTimeout {
			t.Errorf("Expected timeout %v, got %v", customTimeout, dispatcher.reqTimeout)
		}
	})

	t.Run("SetConcurrency", func(t *testing.T) {
		customConcurrency := 5
		dispatcher.SetConcurrency(customConcurrency)
		if dispatcher.concurrency != customConcurrency {
			t.Errorf("Expected concurrency %d, got %d", customConcurrency, dispatcher.concurrency)
		}
	})

	t.Run("SetLogger", func(t *testing.T) {
		customLogger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
		dispatcher.SetLogger(customLogger)
		if dispatcher.logger != customLogger {
			t.Errorf("Expected logger to be set to the custom logger")
		}
	})
}

func TestEnqueueAndGetEventFromDB(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	dispatcher, err := NewWebhookDispatcher(db, "")
	if err != nil {
		t.Fatalf("Failed to create WebhookDispatcher: %v", err)
	}

	eventID := "test_event_id"
	url := "http://example.com/webhook"
	category := "test_category"
	data := map[string]string{"key": "value"}

	event := WebhookEvent{
		Category:  category,
		CreatedAt: time.Now(),
		Data:      data,
		EventID:   eventID,
	}

	queuedEvent := NewQueuedEvent(event, url)

	// Test Enqueue
	err = dispatcher.Enqueue(queuedEvent)
	if err != nil {
		t.Fatalf("Failed to enqueue event: %v", err)
	}

	// Test getEventFromDB
	retrievedEvent, err := dispatcher.getEventFromDB(eventID)
	if err != nil {
		t.Fatalf("Failed to get event from DB: %v", err)
	}

	if retrievedEvent.EventID != eventID {
		t.Errorf("Expected EventID %q, got %q", eventID, retrievedEvent.EventID)
	}
	if retrievedEvent.Category != category {
		t.Errorf("Expected Category %q, got %q", category, retrievedEvent.Category)
	}
	if retrievedEvent.URL != url {
		t.Errorf("Expected URL %q, got %q", url, retrievedEvent.URL)
	}
}

func TestStop(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	dispatcher, err := NewWebhookDispatcher(db, "")
	if err != nil {
		t.Fatalf("Failed to create WebhookDispatcher: %v", err)
	}
	dispatcher.concurrency = 2 // Use 2 workers for testing
	dispatcher.logger = log.New(io.Discard, "", 0)

	// Start the dispatcher
	dispatcher.Start()

	// Give it time to start workers
	time.Sleep(50 * time.Millisecond)

	// Enqueue some events
	for i := 0; i < dispatcher.concurrency*2; i++ {
		eventID := fmt.Sprintf("test_event_%d", i)
		event := &WebhookEvent{
			EventID:   eventID,
			Category:  "test",
			CreatedAt: time.Now(),
			Data:      map[string]interface{}{"test": "test"},
		}
		queuedEvent := NewQueuedEvent(*event, "http://example.com")
		err = dispatcher.Enqueue(queuedEvent)
		if err != nil {
			t.Fatalf("Failed to enqueue event: %v", err)
		}
	}

	// Stop the dispatcher
	dispatcher.Stop()

	// Verify that the sendEventQueue is closed
	_, ok := <-dispatcher.sendEventQueue
	if ok {
		t.Error("sendEventQueue should be closed")
	}

	// Try to send to sendEventQueue (should panic if not caught)
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when sending to closed channel")
			}
		}()
		dispatcher.sendEventQueue <- "this_should_panic"
	}()
}

func TestContextCancellation(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	dispatcher, err := NewWebhookDispatcher(db, "")
	if err != nil {
		t.Fatalf("Failed to create WebhookDispatcher: %v", err)
	}
	dispatcher.concurrency = 1
	dispatcher.logger = log.New(io.Discard, "", 0)

	// Start the dispatcher
	dispatcher.Start()

	// Wait a moment for everything to start
	time.Sleep(50 * time.Millisecond)

	// Manually cancel the context (simulates what happens in Stop)
	dispatcher.cancel()

	// Give some time for goroutines to respond to the cancellation
	time.Sleep(100 * time.Millisecond)

	// Now try to add an event to the queue - workers should have stopped processing
	eventID := "test_event_after_cancel"
	event := &WebhookEvent{
		EventID:   eventID,
		Category:  "test",
		CreatedAt: time.Now(),
		Data:      map[string]interface{}{"test": "test"},
	}
	queuedEvent := NewQueuedEvent(*event, "http://example.com")
	err = dispatcher.Enqueue(queuedEvent)
	if err != nil {
		t.Fatalf("Failed to enqueue event: %v", err)
	}

	// The event should stay in the DB but not be processed
	time.Sleep(200 * time.Millisecond)

	// Verify the event is still in the DB
	retrievedEvent, err := dispatcher.getEventFromDB(eventID)
	if err != nil {
		t.Fatalf("Failed to get event from DB: %v", err)
	}
	if retrievedEvent.EventID != eventID {
		t.Errorf("Expected to find event %s in DB", eventID)
	}

	// Clean up
	dispatcher.Stop()
}

func TestDeleteEvent(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	dispatcher, err := NewWebhookDispatcher(db, "")
	if err != nil {
		t.Fatalf("Failed to create WebhookDispatcher: %v", err)
	}

	// Create and save a test event
	eventID := "test_delete_event"
	event := &WebhookEvent{
		EventID:   eventID,
		Category:  "test",
		CreatedAt: time.Now(),
		Data:      map[string]interface{}{"test": "test"},
	}
	queuedEvent := NewQueuedEvent(*event, "http://example.com")
	err = dispatcher.saveEventInDB(queuedEvent)
	if err != nil {
		t.Fatalf("Failed to save event: %v", err)
	}

	// Verify the event exists in the DB
	_, err = dispatcher.getEventFromDB(eventID)
	if err != nil {
		t.Fatalf("Event should exist in DB: %v", err)
	}

	// Call deleteEvent
	dispatcher.deleteEventFromDB(eventID)

	// Verify the event is deleted
	_, err = dispatcher.getEventFromDB(eventID)
	if err == nil {
		t.Error("Event should be deleted from DB")
	}
}

func TestGetEventFromDBNotFound(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	dispatcher, err := NewWebhookDispatcher(db, "")
	if err != nil {
		t.Fatalf("Failed to create WebhookDispatcher: %v", err)
	}

	// Try to get a non-existent event
	_, err = dispatcher.getEventFromDB("non_existent_event")
	if err == nil {
		t.Error("Expected error when getting non-existent event, got nil")
	}
}

func TestHandleEvent(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create a test server that returns success
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dispatcher, err := NewWebhookDispatcher(db, "")
	if err != nil {
		t.Fatalf("Failed to create WebhookDispatcher: %v", err)
	}

	// Create a test context
	ctx := context.WithValue(context.Background(), workerIDKey, 0)

	// Create and save a test event
	eventID := "test_handle_event"
	event := &WebhookEvent{
		EventID:   eventID,
		Category:  "test",
		CreatedAt: time.Now(),
		Data:      map[string]interface{}{"test": "test"},
	}
	queuedEvent := NewQueuedEvent(*event, server.URL)
	err = dispatcher.saveEventInDB(queuedEvent)
	if err != nil {
		t.Fatalf("Failed to save event: %v", err)
	}

	// Handle the event
	dispatcher.handleEvent(ctx, eventID)

	// Check that the event was deleted from the database
	_, err = dispatcher.getEventFromDB(eventID)
	if err == nil {
		t.Error("Event should have been deleted from the database")
	}
}

func TestHandleEventFailedSend(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create a test server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dispatcher, err := NewWebhookDispatcher(db, "")
	if err != nil {
		t.Fatalf("Failed to create WebhookDispatcher: %v", err)
	}

	// Create a test context
	ctx := context.WithValue(context.Background(), workerIDKey, 0)

	// Create and save a test event
	eventID := "test_handle_event_failure"
	event := &WebhookEvent{
		EventID:   eventID,
		Category:  "test",
		CreatedAt: time.Now(),
		Data:      map[string]interface{}{"test": "test"},
	}
	queuedEvent := NewQueuedEvent(*event, server.URL)
	queuedEvent.RetrySchedule = []string{"0s", "1s"} // Short retry schedule
	err = dispatcher.saveEventInDB(queuedEvent)
	if err != nil {
		t.Fatalf("Failed to save event: %v", err)
	}

	// Handle the event
	dispatcher.handleEvent(ctx, eventID)

	// Check that the event was updated with retry information
	updatedEvent, err := dispatcher.getEventFromDB(eventID)
	if err != nil {
		t.Fatalf("Failed to get event: %v", err)
	}

	if updatedEvent.RetryCount != 1 {
		t.Errorf("Expected retry count to be 1, got %d", updatedEvent.RetryCount)
	}

	if !updatedEvent.RetryAfter.After(time.Now()) {
		t.Errorf("Expected retry after to be in the future")
	}
}

func TestMonitorDBComprehensive(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	dispatcher, err := NewWebhookDispatcher(db, "")
	if err != nil {
		t.Fatalf("Failed to create WebhookDispatcher: %v", err)
	}

	// Set a small interval for faster testing
	origInterval := monitorDBInterval
	monitorDBInterval = 50 * time.Millisecond
	defer func() { monitorDBInterval = origInterval }()

	// Create a buffered channel to collect events
	sendQueue := make(chan string, 5)
	dispatcher.sendEventQueue = sendQueue

	// 1. Create a regular event that should be processed
	eventID1 := "test_monitor_normal"
	event1 := &WebhookEvent{
		EventID:   eventID1,
		Category:  "test",
		CreatedAt: time.Now(),
		Data:      map[string]interface{}{"test": "test"},
	}
	queuedEvent1 := NewQueuedEvent(*event1, "http://example.com")
	queuedEvent1.RetryAfter = time.Now().Add(-1 * time.Second) // Ready to be sent
	err = dispatcher.saveEventInDB(queuedEvent1)
	if err != nil {
		t.Fatalf("Failed to save event: %v", err)
	}

	// 2. Create an event that's already in progress (should be skipped)
	eventID2 := "test_monitor_in_progress"
	event2 := &WebhookEvent{
		EventID:   eventID2,
		Category:  "test",
		CreatedAt: time.Now(),
		Data:      map[string]interface{}{"test": "test"},
	}
	queuedEvent2 := NewQueuedEvent(*event2, "http://example.com")
	queuedEvent2.RetryAfter = time.Now().Add(-1 * time.Second) // Ready to be sent
	err = dispatcher.saveEventInDB(queuedEvent2)
	if err != nil {
		t.Fatalf("Failed to save event: %v", err)
	}
	// Mark as in progress
	dispatcher.inProgress.Store(eventID2, true)

	// 3. Create an event with future retry time (should be skipped)
	eventID3 := "test_monitor_future"
	event3 := &WebhookEvent{
		EventID:   eventID3,
		Category:  "test",
		CreatedAt: time.Now(),
		Data:      map[string]interface{}{"test": "test"},
	}
	queuedEvent3 := NewQueuedEvent(*event3, "http://example.com")
	queuedEvent3.RetryAfter = time.Now().Add(10 * time.Second) // Not ready yet
	err = dispatcher.saveEventInDB(queuedEvent3)
	if err != nil {
		t.Fatalf("Failed to save event: %v", err)
	}

	// 4. Create an event with invalid JSON (should be detected and deleted)
	eventID4 := "test_monitor_invalid"
	err = db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(dispatcher.bucketName))
		return bucket.Put([]byte(eventID4), []byte("invalid json"))
	})
	if err != nil {
		t.Fatalf("Failed to save invalid event: %v", err)
	}

	// 5. Set up context to cancel
	ctx, cancel := context.WithCancel(context.Background())
	dispatcher.ctx = ctx
	dispatcher.cancel = cancel

	// Use a wait group to track when monitorDB has fully exited
	var wg sync.WaitGroup
	wg.Add(1)

	// Start monitorDB with tracking for when it exits
	go func() {
		dispatcher.monitorDB()
		wg.Done()
	}()

	// Wait for the first event to be processed
	var receivedEventID string
	select {
	case receivedEventID = <-sendQueue:
		if receivedEventID != eventID1 {
			t.Errorf("Expected event ID %q, got %q", eventID1, receivedEventID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Event %s not sent to queue within timeout", eventID1)
	}

	// Allow some time for monitoring to detect and delete the invalid event
	time.Sleep(100 * time.Millisecond)

	// Check that the invalid event has been deleted from the database
	_, err = dispatcher.getEventFromDB(eventID4)
	if err == nil {
		t.Errorf("Invalid event %s should have been deleted", eventID4)
	}

	// Make sure the future event and in-progress event are not sent to the queue
	select {
	case eventID := <-sendQueue:
		if eventID == eventID2 || eventID == eventID3 {
			t.Errorf("Event %s should not have been sent to queue", eventID)
		}
	case <-time.After(200 * time.Millisecond):
		// No events sent, as expected
	}

	// Now test context cancellation
	cancel() // Cancel the context

	// Instead of looping to drain the queue (which could block indefinitely),
	// wait for monitor to exit and then check
	wg.Wait()

	// Delete ALL existing events from the database to start fresh
	err = db.Update(func(tx *bbolt.Tx) error {
		// Delete the bucket and recreate it to clear all events
		if err := tx.DeleteBucket(dispatcher.bucketName); err != nil {
			return err
		}
		_, err := tx.CreateBucket(dispatcher.bucketName)
		return err
	})
	if err != nil {
		t.Fatalf("Failed to clear database: %v", err)
	}

	// Add a new event with a future retry time
	eventIDPost := "test_after_cancel"
	eventPost := &WebhookEvent{
		EventID:   eventIDPost,
		Category:  "test",
		CreatedAt: time.Now(),
		Data:      map[string]interface{}{"test": "test"},
	}
	queuedEventPost := NewQueuedEvent(*eventPost, "http://example.com")
	queuedEventPost.RetryAfter = time.Now().Add(10 * time.Second) // Set to future time
	err = dispatcher.saveEventInDB(queuedEventPost)
	if err != nil {
		t.Fatalf("Failed to save event: %v", err)
	}

	// Create a completely new dispatcher with a fresh context
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	dispatcher2, err := NewWebhookDispatcher(db, string(dispatcher.bucketName))
	if err != nil {
		t.Fatalf("Failed to create new dispatcher: %v", err)
	}
	dispatcher2.ctx = ctx2
	dispatcher2.cancel = cancel2
	dispatcher2.sendEventQueue = sendQueue // Reuse the same queue for testing

	// Clear the queue
	for len(sendQueue) > 0 {
		<-sendQueue
	}

	// Start monitoring with the new dispatcher
	go dispatcher2.monitorDB()

	// Check that no events are sent within the timeout period
	select {
	case eventID := <-sendQueue:
		t.Errorf("Event %s should not have been sent to queue after restart", eventID)
	case <-time.After(200 * time.Millisecond):
		// No events sent, as expected
	}
}
