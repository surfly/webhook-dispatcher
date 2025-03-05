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
	// For example, "1s", "1m", "1h", "1d", "1w", "1M", "1y".
	RetrySchedule []string `json:"retry_schedule"`
	// LastUpdatedAt is the time at which this event was last processed.
	LastUpdatedAt time.Time `json:"last_updated_at"` // RFC3339
	// ProcessingFinished is true if the event has been processed.
	ProcessingFinished bool `json:"processing_finished"`
}

func NewQueuedEvent(event WebhookEvent, url string) *QueuedEvent {
	return &QueuedEvent{
		WebhookEvent:       event,
		URL:                url,
		RetryCount:         0,
		RetryAfter:         time.Now(),
		MaxRetryCount:      0,
		RetrySchedule:      []string{},
		LastUpdatedAt:      time.Now(),
		ProcessingFinished: false,
	}
}

func GetNextRetryTime(queuedEvent *QueuedEvent, delay time.Duration) (time.Time, error) {
	// Check if the max retry count has been reached
	if queuedEvent.MaxRetryCount > 0 && queuedEvent.RetryCount >= queuedEvent.MaxRetryCount {
		return time.Time{}, fmt.Errorf("max retry count reached")
	}

	// There is no schedule, retry after the default delay
	if len(queuedEvent.RetrySchedule) == 0 {
		return time.Now().Add(delay), nil
	}

	if len(queuedEvent.RetrySchedule) > queuedEvent.RetryCount {
		customDelay := queuedEvent.RetrySchedule[queuedEvent.RetryCount]
		parsed, err := time.ParseDuration(customDelay)
		if err != nil {
			fmt.Println(err)
		} else {
			return time.Now().Add(parsed), nil
		}
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
