package v1alpha1

// ConnectionType discriminates which store-specific block of ConnectionSpec
// is populated.
type ConnectionType string

const (
	ConnectionTypeKafka    ConnectionType = "kafka"
	ConnectionTypePostgres ConnectionType = "postgres"
	ConnectionTypeDatalake ConnectionType = "datalake"
)

// Connection describes how to reach one data store.
type Connection struct {
	TypeMeta `yaml:",inline"`
	Metadata ObjectMeta     `json:"metadata" yaml:"metadata"`
	Spec     ConnectionSpec `json:"spec" yaml:"spec"`
}

type ConnectionSpec struct {
	Type     ConnectionType      `json:"type" yaml:"type"`
	Kafka    *KafkaConnection    `json:"kafka,omitempty" yaml:"kafka,omitempty"`
	Postgres *PostgresConnection `json:"postgres,omitempty" yaml:"postgres,omitempty"`
	Datalake *DatalakeConnection `json:"datalake,omitempty" yaml:"datalake,omitempty"`
}

type KafkaConnection struct {
	Brokers       []string `json:"brokers" yaml:"brokers"`
	SASLMechanism string   `json:"saslMechanism,omitempty" yaml:"saslMechanism,omitempty"`
	// CredentialsRef is reserved for a future secrets backend; unused (inline-only) in this slice.
	CredentialsRef string `json:"credentialsRef,omitempty" yaml:"credentialsRef,omitempty"`
}

type PostgresConnection struct {
	DSN string `json:"dsn" yaml:"dsn"`
	// CredentialsRef is reserved for a future secrets backend; unused (inline-only) in this slice.
	CredentialsRef string `json:"credentialsRef,omitempty" yaml:"credentialsRef,omitempty"`
}

type DatalakeConnection struct {
	Endpoint       string `json:"endpoint" yaml:"endpoint"`
	Bucket         string `json:"bucket" yaml:"bucket"`
	CredentialsRef string `json:"credentialsRef,omitempty" yaml:"credentialsRef,omitempty"`
}
