// Command producer publishes synthetic user-event JSON messages to a Kafka
// topic. It's a standalone test/dev tool, not one of DAL's components — it
// exists only to exercise the kafka->postgres example slice end-to-end
// without a real event source.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var eventTypes = []string{"signup", "login", "purchase", "logout"}

type event struct {
	EventID    string `json:"event_id"`
	UserID     string `json:"user_id"`
	EventType  string `json:"event_type"`
	OccurredAt string `json:"occurred_at"`
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka broker addresses")
	topic := flag.String("topic", "user-events", "Kafka topic to publish to")
	count := flag.Int("count", 0, "number of events to publish (0 = run forever)")
	interval := flag.Duration("interval", time.Second, "delay between published events")
	users := flag.Int("users", 5, "number of distinct synthetic user IDs to cycle through")
	backdate := flag.Duration("backdate", 0, "base age applied to every published event's occurred_at (e.g. 720h for 30 days ago); for seeding historical/pre-retention test data")
	backdateJitter := flag.Duration("backdate-jitter", 0, "additional random age in [0, backdate-jitter) applied on top of --backdate, so a single load job spreads timestamps across a range instead of one instant")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	writer := &kafka.Writer{
		Addr:     kafka.TCP(strings.Split(*brokers, ",")...),
		Topic:    *topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	ctx := context.Background()
	for i := 0; *count == 0 || i < *count; i++ {
		age := *backdate
		if *backdateJitter > 0 {
			age += time.Duration(rand.Int63n(int64(*backdateJitter)))
		}
		evt := event{
			EventID:    fmt.Sprintf("evt-%d-%d", time.Now().UnixNano(), i),
			UserID:     fmt.Sprintf("user-%d", rand.Intn(*users)+1),
			EventType:  eventTypes[rand.Intn(len(eventTypes))],
			OccurredAt: time.Now().UTC().Add(-age).Format(time.RFC3339),
		}
		body, err := json.Marshal(evt)
		if err != nil {
			log.Error("failed to marshal event", "err", err)
			os.Exit(1)
		}

		if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(evt.EventID), Value: body}); err != nil {
			log.Error("failed to publish event", "err", err)
			os.Exit(1)
		}
		log.Info("published event", "event_id", evt.EventID, "user_id", evt.UserID, "event_type", evt.EventType)

		if *interval > 0 {
			time.Sleep(*interval)
		}
	}
}
