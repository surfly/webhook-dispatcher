package dispatcher

import "go.etcd.io/bbolt"

const BucketName = "webhook_events"

// WebhookDispatcher manages event delivery.
type WebhookDispatcher struct {
	db *bbolt.DB
}

// NewWebhookDispatcher creates a new WebhookDispatcher. Pass *bbolt.DB instance
// which will be used to store events.
func NewWebhookDispatcher(db *bbolt.DB) (*WebhookDispatcher, error) {
	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(BucketName))
		return err
	})
	if err != nil {
		return nil, err
	}

	return &WebhookDispatcher{
		db: db,
	}, nil
}
