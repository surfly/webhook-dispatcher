package dispatcher

import "go.etcd.io/bbolt"

// WebhookDispatcher manages event delivery.
type WebhookDispatcher struct {
	db *bbolt.DB
	// bucketName is the name of the bucket in which events are stored.
	bucketName []byte
}

// NewWebhookDispatcher creates a new WebhookDispatcher. Pass *bbolt.DB instance
// which will be used to store events.
func NewWebhookDispatcher(db *bbolt.DB, bucketName string) (*WebhookDispatcher, error) {
	if bucketName == "" {
		bucketName = "webhook_events"
	}
	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		return nil, err
	}

	return &WebhookDispatcher{
		db:         db,
		bucketName: []byte(bucketName),
	}, nil
}
