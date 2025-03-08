# Webhook Dispatcher

Webhook Dispatcher is a Go package that provides a robust solution for reliable webhook delivery. It is designed to handle
webhooks in a fault-tolerant manner, ensuring that events are delivered even in the face of network issues or server downtime.

## Features

- **Persistent Storage**: Uses [BBolt](https://github.com/etcd-io/bbolt) as an embedded key-value database to store webhooks until successful delivery
- **Automatic Retries**: Configurable retry mechanism with exponential backoff
- **Concurrent Processing**: Multiple worker goroutines process webhooks in parallel
- **Compression**: Automatic payload compression using zstd for large payloads
- **Context-Aware**: Graceful shutdown with context cancellation
- **Customizable**: Configurable timeouts, user agents, concurrency, and more

## Installation

```bash
go get github.com/surfly/webhook-dispatcher
```

## Usage

### Basic Setup

```go
package main

import (
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/surfly/webhook-dispatcher"
    "go.etcd.io/bbolt"
)

func main() {
    // Open the database
    db, err := bbolt.Open("webhooks.db", 0600, nil)
    if err != nil {
        log.Fatalf("Failed to open database: %v", err)
    }
    defer db.Close()

    // Create a new webhook dispatcher
    d, err := dispatcher.NewWebhookDispatcher(db, "")
    if err != nil {
        log.Fatalf("Failed to create dispatcher: %v", err)
    }

    // Configure the dispatcher
    d.SetConcurrency(5)  // Use 5 worker goroutines
    d.SetReqTimeout(30 * time.Second)

    // Start the dispatcher
    d.Start()

    // Queue a webhook
    err = d.QuickEnqueue(
        "https://example.com/webhook",
        "user.created",
        map[string]interface{}{"user_id": 123, "name": "Amy Pond"},
    )
    if err != nil {
        log.Printf("Failed to enqueue webhook: %v", err)
    }

    // Handle graceful shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
    <-sigChan

    // Stop the dispatcher gracefully
    d.Stop()
}
```

### Enqueueing Events

There are two ways to enqueue events:

1. **Quick Enqueue** - Simple method for basic use cases:

```go
d.QuickEnqueue(
    "https://example.com/webhook",  // Destination URL
    "order.created",                // Event category/type
    orderData,                      // Event payload (any JSON-serializable data)
)
```

2. **Custom Enqueue** - For more control:

```go
// Create a custom event
event := dispatcher.WebhookEvent{
    EventID:   "unique-event-id", // Or generate with uuid.NewV7()
    Category:  "order.refunded",
    CreatedAt: time.Now(),
    Data:      orderRefundData,
}

// Create a queued event with custom retry settings
queuedEvent := dispatcher.NewQueuedEvent(event, "https://example.com/webhooks")
queuedEvent.RetrySchedule = []string{"5s", "30s", "2m", "5m", "10m"} // Custom retry schedule
queuedEvent.MaxRetryCount = 10                                       // Custom retry limit

// Enqueue the event
d.Enqueue(queuedEvent)
```

## Technical Implementation Details

### Storage

Webhook Dispatcher uses [BBolt](https://github.com/etcd-io/bbolt) as an embedded key-value database. Each webhook event
is serialized to JSON and stored with its event ID as the key. This ensures:

- Persistence across application restarts
- No dependency on external databases
- Atomic operations for event storage and retrieval

### Concurrency Model

The dispatcher uses a producer-consumer pattern:

1. A monitoring goroutine (`monitorDB`) scans the database periodically for events that need to be processed
2. Events are sent to a channel (`sendEventQueue`)
3. Worker goroutines consume from this channel and process events
4. Events to be deleted are sent to another channel (`deleteEventQueue`)
5. A deletion goroutine processes this queue

This model allows for controlled concurrency while preventing race conditions.

### Retry Mechanism

Failed webhook deliveries are rescheduled based on:

1. A configurable retry schedule, which defaults to increasing intervals (`0s`, `5s`, `10s`, `30s`, `1m`, `30m`, etc.)
2. Each retry increments a counter and calculates the next retry time
3. When the maximum retry count is reached, the event is deleted

### Payload Compression

For payloads larger than 1KB, the dispatcher automatically compresses them using Zstandard (zstd) compression:

This reduces bandwidth usage significantly for larger payloads.

### Graceful Shutdown

The dispatcher uses Go's `context` package for graceful shutdown:

1. When `Stop()` is called, it cancels the context
2. All workers and monitoring goroutines check for context cancellation
3. Workers finish their current tasks but don't pick up new ones
4. The main goroutine waits for all workers to finish using `sync.WaitGroup`

### Time-Ordered UUIDs

For automatic event IDs, the dispatcher uses UUID v7 which are time-ordered, making them ideal for sorted retrieval.
