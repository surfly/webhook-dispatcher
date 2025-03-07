package dispatcher

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
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

func TestDeleteWorker(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	dispatcher, err := NewWebhookDispatcher(db, "")
	if err != nil {
		t.Fatalf("Failed to create WebhookDispatcher: %v", err)
	}

	// Create a test event
	dispatcher.QuickEnqueue("http://127.0.0.1:8008/webhook", "test_event", nil)

	// Enqueue the event ID for deletion
	dispatcher.deleteEventQueue <- "test_event"
	close(dispatcher.deleteEventQueue) // close the channel to signal the end of work

	// Start the delete worker in a goroutine
	go dispatcher.deleteWorker()

	// Wait for a short time to allow the delete worker to process the event
	time.Sleep(100 * time.Millisecond)

	// Verify that the event is deleted from the database
	_, err = dispatcher.getEventFromDB("test_event")
	if err == nil {
		err = db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket([]byte(dispatcher.bucketName))
			data := bucket.Get([]byte("test_event"))
			if data != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Event was not deleted from the database")
		}
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
			err := dispatcher.sendWebhook(server.URL, tc.payload)

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
