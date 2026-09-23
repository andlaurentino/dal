//go:build integration

// This file is the Go half of a two-language contract test: workerd
// resolves S3 auth via object_store::aws::AmazonS3Builder (see
// workerd/tests/s3_contract_test.rs), broker resolves the same connection's
// credentials via DuckDB's CREATE SECRET (lake/executor.go's configure).
// Nothing in the type system enforces these two independently-implemented
// paths stay behaviorally identical — they drifted once already (the
// RustFS migration outage: one side read S3_ACCESS_KEY/S3_SECRET_KEY, the
// stale other side still had a hardcoded literal). Both halves of this test
// read the same DAL_TEST_S3_* env vars and must both authenticate
// successfully against the same running S3-compatible store for this
// contract to hold.
//
// Run against a live store (e.g. this repo's RustFS NodePort):
//
//	DAL_TEST_S3_ENDPOINT=http://localhost:30900 \
//	DAL_TEST_S3_BUCKET=dal-lake \
//	S3_ACCESS_KEY=dalrustfs S3_SECRET_KEY=dalrustfs123 \
//	go test -tags=integration ./internal/executors/lake/...
package lake

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/andersonlaurentino/dal-broker/internal/executors"
)

func TestS3ConfigContract(t *testing.T) {
	endpoint := os.Getenv("DAL_TEST_S3_ENDPOINT")
	bucket := os.Getenv("DAL_TEST_S3_BUCKET")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Skip("DAL_TEST_S3_ENDPOINT, DAL_TEST_S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY must all be set")
	}

	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("opening duckdb: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("opening duckdb connection: %v", err)
	}
	defer conn.Close()

	req := executors.QueryRequest{
		Endpoint:        endpoint,
		Bucket:          bucket,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	}
	if err := configure(ctx, conn, req); err != nil {
		t.Fatalf("configuring duckdb s3 secret: %v", err)
	}

	// glob() only needs LIST/HEAD permissions and doesn't require an actual
	// Delta table to exist — it's the cheapest operation that still forces
	// DuckDB to authenticate against the store, surfacing exactly the class
	// of 403 InvalidAccessKeyId this test exists to catch.
	var count int
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM glob(?)", "s3://"+bucket+"/*").Scan(&count); err != nil {
		t.Fatalf("listing bucket %q via DuckDB (auth likely failed): %v", bucket, err)
	}
}
