// Package lake executes paginated, optionally time-filtered reads against a
// Delta table on an S3-compatible store (RustFS in this deployment) via
// DuckDB's httpfs/delta extensions — the read-side counterpart to workerd's
// deltalake-crate write path, chosen because no comparably mature Go Delta
// Lake reader exists (same reasoning that pushed workerd to Rust). Kept as
// an in-process CGO dependency rather than a new service: broker stays a
// single deployable, and this is a straightforward read, not the write/CDF
// path that justified Rust for workerd.
package lake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Config identifies the S3-compatible datalake connection to read from.
type Config struct {
	Endpoint string
	Bucket   string
	// AccessKeyID/SecretAccessKey are the S3 credentials read from the
	// S3_ACCESS_KEY/S3_SECRET_KEY env vars (see internal/httpapi.New),
	// matching workerd's own storage_options — reserved for a future
	// per-Connection secrets backend, same "unused (inline-only) in this
	// slice" status as Connection.CredentialsRef elsewhere.
	AccessKeyID     string
	SecretAccessKey string
}

// Executor runs reads against Delta tables via DuckDB. Each query opens its
// own DuckDB connection (rather than pooling) since extension LOAD state is
// per-connection — simple and correct over clever, given query volume here
// is not perf-critical.
type Executor struct{}

func New() *Executor {
	return &Executor{}
}

// Query returns the Delta table's columns, a page of rows, and the total
// row count. When cutoverBefore is non-nil, only rows whose cutoverColumn
// value is strictly before it are considered — this is how a tiered
// projection asks the lake for only the portion of history not already
// covered by its postgres "recent" tier.
func (e *Executor) Query(ctx context.Context, cfg Config, path, cutoverColumn string, cutoverBefore *time.Time, limit, offset int) ([]Column, [][]any, int64, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, nil, 0, fmt.Errorf("opening duckdb: %w", err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("opening duckdb connection: %w", err)
	}
	defer conn.Close()

	if err := configure(ctx, conn, cfg); err != nil {
		return nil, nil, 0, err
	}

	source := fmt.Sprintf("delta_scan('s3://%s/%s')", cfg.Bucket, strings.Trim(path, "/"))

	where := ""
	var args []any
	if cutoverBefore != nil {
		where = fmt.Sprintf(" WHERE %s < ?", quoteIdent(cutoverColumn))
		args = append(args, *cutoverBefore)
	}

	var total int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", source, where)
	if err := conn.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, nil, 0, fmt.Errorf("counting lake rows: %w", err)
	}
	if total == 0 {
		return nil, nil, 0, nil
	}

	orderBy := ""
	if cutoverColumn != "" {
		orderBy = fmt.Sprintf(" ORDER BY %s DESC", quoteIdent(cutoverColumn))
	}
	selectQuery := fmt.Sprintf("SELECT * FROM %s%s%s LIMIT ? OFFSET ?", source, where, orderBy)
	selectArgs := append(append([]any{}, args...), limit, offset)
	rows, err := conn.QueryContext(ctx, selectQuery, selectArgs...)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("querying lake rows: %w", err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, 0, err
	}
	columns := make([]Column, len(colTypes))
	for i, ct := range colTypes {
		columns[i] = Column{Name: ct.Name(), Type: ct.DatabaseTypeName()}
	}

	page, err := scanRows(rows, len(columns))
	if err != nil {
		return nil, nil, 0, err
	}
	return columns, page, total, nil
}

// configure installs/loads the extensions and S3 credentials this
// connection needs to scan a Delta table on the S3-compatible datalake
// store. Credentials are set via DuckDB's CREATE SECRET mechanism rather
// than the legacy `SET s3_*` session variables: delta_scan resolves
// credentials through delta-rs's own object_store layer, which only picks
// them up via a secret — with the legacy SET vars it silently falls back to
// the default AWS credential chain (including an EC2 instance-metadata-
// service lookup that hangs/times out off-AWS). DuckDB's s3_endpoint wants
// a bare host:port (no scheme); use_ssl carries the scheme instead.
func configure(ctx context.Context, conn *sql.Conn, cfg Config) error {
	endpoint := cfg.Endpoint
	useSSL := "false"
	switch {
	case strings.HasPrefix(endpoint, "https://"):
		endpoint = strings.TrimPrefix(endpoint, "https://")
		useSSL = "true"
	case strings.HasPrefix(endpoint, "http://"):
		endpoint = strings.TrimPrefix(endpoint, "http://")
	}

	stmts := []string{
		"INSTALL httpfs", "LOAD httpfs",
		"INSTALL delta", "LOAD delta",
		fmt.Sprintf(`
			CREATE OR REPLACE SECRET s3 (
				TYPE s3,
				KEY_ID '%s',
				SECRET '%s',
				ENDPOINT '%s',
				URL_STYLE 'path',
				USE_SSL %s
			)`, cfg.AccessKeyID, cfg.SecretAccessKey, endpoint, useSSL),
	}
	for _, stmt := range stmts {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("configuring duckdb (%q): %w", stmt, err)
		}
	}
	return nil
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func scanRows(rows *sql.Rows, numCols int) ([][]any, error) {
	var page [][]any
	for rows.Next() {
		dest := make([]any, numCols)
		ptrs := make([]any, numCols)
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		for i, v := range dest {
			if b, ok := v.([]byte); ok {
				dest[i] = string(b)
			}
		}
		page = append(page, dest)
	}
	return page, rows.Err()
}
