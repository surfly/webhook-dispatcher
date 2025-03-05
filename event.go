package dispatcher

// QueuedEvent is the event structure which is stored in the database.
type QueuedEvent struct {
	WebhookEvent
	// URL is the webhook URL to which this event should be sent.
	URL string `json:"url"`
	// RetryCount is the number of times this event has been retried.
	RetryCount int `json:"retry_count"`
	// RetryAfter is the time after which this event should be retried.
	RetryAfter string `json:"retry_after"` // RFC3339
	// LastUpdatedAt is the time at which this event was last processed.
	LastUpdatedAt string `json:"last_updated_at"` // RFC3339
}

// WebhookEvent is the event structure which is sent as a webhook.
type WebhookEvent struct {
	Category  string `json:"category"`
	CreatedAt string `json:"created_at"` // RFC3339
	EventID   string `json:"event_id"`   // Unique event ID
	Data      any    `json:"data"`
}
