package dispatcher

import (
	"testing"
	"time"
)

func TestGetNextRetryTime(t *testing.T) {
	now := time.Now()
	defaultDelay := 1 * time.Minute

	t.Run("max retry count reached", func(t *testing.T) {
		event := &QueuedEvent{
			MaxRetryCount: 2,
			RetryCount:    2,
		}
		_, err := GetNextRetryTime(event, defaultDelay)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("retry schedule defined", func(t *testing.T) {
		event := &QueuedEvent{
			RetryCount:    0,
			RetrySchedule: []string{"1s", "2s", "3s"},
		}
		nextRetryTime, err := GetNextRetryTime(event, defaultDelay)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		expectedRetryTime := now.Add(1 * time.Second)

		// Allow a small buffer for time comparison
		if nextRetryTime.Before(expectedRetryTime.Add(-100 * time.Millisecond)) {
			t.Errorf("expected retry time to be approximately %v, got %v", expectedRetryTime, nextRetryTime)
		}
	})

	t.Run("retry schedule index out of bounds", func(t *testing.T) {
		event := &QueuedEvent{
			RetryCount:    5,
			RetrySchedule: []string{"1s", "2s", "3s"},
		}
		nextRetryTime, err := GetNextRetryTime(event, defaultDelay)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		expectedRetryTime := now.Add(3 * time.Second)

		// Allow a small buffer for time comparison
		if nextRetryTime.Before(expectedRetryTime.Add(-100 * time.Millisecond)) {
			t.Errorf("expected retry time to be approximately %v, got %v", expectedRetryTime, nextRetryTime)
		}
	})

	t.Run("no max retry count or schedule defined", func(t *testing.T) {
		event := &QueuedEvent{
			RetryCount:    0,
			RetrySchedule: []string{},
		}
		nextRetryTime, err := GetNextRetryTime(event, defaultDelay)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		expectedRetryTime := now.Add(defaultDelay)

		// Allow a small buffer for time comparison
		if nextRetryTime.Before(expectedRetryTime.Add(-100 * time.Millisecond)) {
			t.Errorf("expected retry time to be approximately %v, got %v", expectedRetryTime, nextRetryTime)
		}
	})

	t.Run("invalid schedule value", func(t *testing.T) {
		event := &QueuedEvent{
			RetryCount:    0,
			RetrySchedule: []string{"invalid"},
		}
		_, err := GetNextRetryTime(event, defaultDelay)
		if err == nil {
			t.Errorf("expected error for invalid schedule value, got nil")
		}
	})
}
