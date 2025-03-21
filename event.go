package dispatcher

import (
	"fmt"
	"time"
)

// QueuedEvent is the event structure which is stored in the database.
type QueuedEvent struct {
	WebhookEvent
	// URL is the webhook URL to which this event should be sent.
	URL string `json:"url"`
	// Headers are the headers to be sent with the webhook request. Useful for
	// authentication or custom headers.
	Headers map[string]string `json:"headers"`
	// RetryCount is the number of times this event has been retried.
	RetryCount int `json:"retry_count"`
	// RetryAfter is the time after which this event should be retried.
	RetryAfter time.Time `json:"retry_after"` // RFC3339
	// MaxRetryCount is the maximum number of times this event should be retried.
	// If this is 0, the event will be retried indefinitely.
	MaxRetryCount int `json:"max_retry_count"`
	// RetrySchedule is the schedule for retrying this event.
	// If this is empty, the event will be retried immediately. Valid values are
	// durations in the format of Go's time.ParseDuration.
	// For example, "1s", "10m", "24h"
	RetrySchedule []string `json:"retry_schedule"`
}

// NewQueuedEvent creates a new QueuedEvent with the given event and URL.
func NewQueuedEvent(event WebhookEvent, url string, headers map[string]string) *QueuedEvent {
	return &QueuedEvent{
		WebhookEvent:  event,
		URL:           url,
		Headers:       headers,
		RetryCount:    0,
		RetryAfter:    time.Now(),
		MaxRetryCount: defaultMaxRetryCount,
		RetrySchedule: defaultRetrySchedule,
	}
}

// GetNextRetryTime returns the next retry time for the given event.
func GetNextRetryTime(queuedEvent *QueuedEvent, delay time.Duration) (time.Time, error) {
	// Check if the max retry count has been reached
	if queuedEvent.MaxRetryCount > 0 && queuedEvent.RetryCount >= queuedEvent.MaxRetryCount {
		return time.Time{}, fmt.Errorf("max retry count reached")
	}

	if len(queuedEvent.RetrySchedule) > 0 {
		retryIndex := queuedEvent.RetryCount

		// Check if the retry index is out of bounds
		if len(queuedEvent.RetrySchedule) <= retryIndex {
			retryIndex = len(queuedEvent.RetrySchedule) - 1
		}

		customDelay := queuedEvent.RetrySchedule[retryIndex]
		parsed, err := time.ParseDuration(customDelay)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to parse retry schedule: %w", err)
		}
		return time.Now().Add(parsed), nil
	}
	return time.Now().Add(delay), nil

}

// WebhookEvent is the event structure which is sent as a webhook.
type WebhookEvent struct {
	Category  string    `json:"category"`
	CreatedAt time.Time `json:"created_at"` // RFC3339
	EventID   string    `json:"event_id"`   // Unique event ID
	Data      any       `json:"data"`
}
