package v1alpha1

import (
	"os"
	"path/filepath"
	"testing"
)

func readExample(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "examples", "tiered-events", name))
	if err != nil {
		t.Fatalf("reading example %s: %v", name, err)
	}
	return raw
}

func TestDecodeConnection_Valid(t *testing.T) {
	for _, name := range []string{"connection-kafka.yaml", "connection-postgres.yaml"} {
		raw := readExample(t, name)
		c, err := DecodeConnection(raw)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", name, err)
		}
		if c.Metadata.Name == "" {
			t.Fatalf("%s: expected a name", name)
		}
	}
}

func TestDecodeSync_Valid(t *testing.T) {
	raw := readExample(t, "sync-postgres.yaml")
	s, err := DecodeSync(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Spec.Source.Topic != "user-events" {
		t.Fatalf("expected topic user-events, got %q", s.Spec.Source.Topic)
	}
	if s.Spec.Retention == nil || s.Spec.Retention.Days != 7 {
		t.Fatalf("expected 7-day retention, got %+v", s.Spec.Retention)
	}
}

func TestDecodeProjection_Valid(t *testing.T) {
	raw := readExample(t, "projection-postgres.yaml")
	p, err := DecodeProjection(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(p.Spec.Sources) != 1 || p.Spec.Sources[0].ConnectionRef != "postgres" {
		t.Fatalf("unexpected sources: %+v", p.Spec.Sources)
	}
}

func TestValidateConnection_RejectsMultipleTypes(t *testing.T) {
	c := &Connection{
		TypeMeta: TypeMeta{APIVersion: APIVersion, Kind: KindConnection},
		Metadata: ObjectMeta{Name: "bad"},
		Spec: ConnectionSpec{
			Type:     ConnectionTypeKafka,
			Kafka:    &KafkaConnection{Brokers: []string{"kafka:9092"}},
			Postgres: &PostgresConnection{DSN: "postgres://x"},
		},
	}
	if err := ValidateConnection(c); err == nil {
		t.Fatal("expected error for multiple populated store blocks")
	}
}

func TestValidateConnection_RejectsMissingName(t *testing.T) {
	c := &Connection{
		TypeMeta: TypeMeta{APIVersion: APIVersion, Kind: KindConnection},
		Spec: ConnectionSpec{
			Type:  ConnectionTypeKafka,
			Kafka: &KafkaConnection{Brokers: []string{"kafka:9092"}},
		},
	}
	if err := ValidateConnection(c); err == nil {
		t.Fatal("expected error for missing metadata.name")
	}
}

func TestValidateSync_RequiresScheduleWhenScheduled(t *testing.T) {
	s := &Sync{
		TypeMeta: TypeMeta{APIVersion: APIVersion, Kind: KindSync},
		Metadata: ObjectMeta{Name: "s"},
		Spec: SyncSpec{
			Source:      SyncSource{ConnectionRef: "a", Topic: "t"},
			Target:      SyncTarget{ConnectionRef: "b", Table: "tbl"},
			Mode:        SyncModeScheduled,
			Replication: SyncReplicationSnapshot,
			Mapping: SyncMapping{
				KeyField: "id",
				Schema:   []SyncFieldMapping{{JSONPath: "$.id", Column: "id", Type: "text"}},
			},
		},
	}
	if err := ValidateSync(s); err == nil {
		t.Fatal("expected error for scheduled mode without a schedule")
	}
}

func TestValidateProjection_RequiresSources(t *testing.T) {
	p := &Projection{
		TypeMeta: TypeMeta{APIVersion: APIVersion, Kind: KindProjection},
		Metadata: ObjectMeta{Name: "p"},
		Spec:     ProjectionSpec{},
	}
	if err := ValidateProjection(p); err == nil {
		t.Fatal("expected error for empty sources")
	}
}
