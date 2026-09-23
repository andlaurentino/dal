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

	"github.com/andersonlaurentino/dal-broker/internal/executors"
)

// Executor runs reads against Delta tables via DuckDB. Each query opens its
// own DuckDB connection (rather than pooling) since extension LOAD state is
// per-connection — simple and correct over clever, given query volume here
// is not perf-critical. Implements executors.Executor.
type Executor struct{}

func New() *Executor {
	return &Executor{}
}

// Query returns req.Columns (or, for passthrough, every column DuckDB
// discovers), a page of rows, and the total row count. req.Table is the
// lake path; req.Endpoint/Bucket/AccessKeyID/SecretAccessKey identify and
// authenticate the datalake connection (req.DSN is ignored — meaningless
// for this store type). req.After/req.Before bound req.OrderBy's value —
// this is how a multi-source projection asks the lake for only the
// portion of history a different source doesn't already cover.
func (e *Executor) Query(ctx context.Context, req executors.QueryRequest) (executors.QueryResult, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return executors.QueryResult{}, fmt.Errorf("opening duckdb: %w", err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return executors.QueryResult{}, fmt.Errorf("opening duckdb connection: %w", err)
	}
	defer conn.Close()

	if err := configure(ctx, conn, req); err != nil {
		return executors.QueryResult{}, err
	}

	source := fmt.Sprintf("delta_scan('s3://%s/%s')", req.Bucket, strings.Trim(req.Table, "/"))

	where, args := rangeWhere(req.OrderBy, req.After, req.Before)

	var total int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", source, where)
	if err := conn.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return executors.QueryResult{}, fmt.Errorf("counting lake rows: %w", err)
	}
	if total == 0 {
		return executors.QueryResult{}, nil
	}

	orderBy := ""
	if req.OrderBy != "" {
		orderBy = fmt.Sprintf(" ORDER BY %s DESC", quoteIdent(req.OrderBy))
	}
	selectExpr := selectClause(req.Columns)
	selectQuery := fmt.Sprintf("SELECT %s FROM %s%s%s LIMIT ? OFFSET ?", selectExpr, source, where, orderBy)
	selectArgs := append(append([]any{}, args...), req.Limit, req.Offset)
	rows, err := conn.QueryContext(ctx, selectQuery, selectArgs...)
	if err != nil {
		return executors.QueryResult{}, fmt.Errorf("querying lake rows: %w", err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return executors.QueryResult{}, err
	}
	columns := make([]executors.Column, len(colTypes))
	for i, ct := range colTypes {
		columns[i] = executors.Column{Name: ct.Name(), Type: ct.DatabaseTypeName()}
	}

	page, err := scanRows(rows, len(columns))
	if err != nil {
		return executors.QueryResult{}, err
	}
	return executors.QueryResult{Columns: columns, Rows: page, Total: total}, nil
}

// selectClause builds the SELECT column list: "*" for passthrough, or an
// explicit "source AS as, ..." list — DuckDB's own query-result column
// metadata (rows.ColumnTypes()) then naturally reflects the requested
// aliases, so no separate metadata lookup is needed here (unlike
// postgres's executor, which has to introspect information_schema up
// front).
func selectClause(spec []executors.ColumnSpec) string {
	if len(spec) == 0 {
		return "*"
	}
	parts := make([]string, len(spec))
	for i, c := range spec {
		as := c.As
		if as == "" {
			as = c.Source
		}
		parts[i] = fmt.Sprintf("%s AS %s", quoteIdent(c.Source), quoteIdent(as))
	}
	return strings.Join(parts, ", ")
}

// rangeWhere builds a " WHERE ..." clause (or "" when neither bound is
// set) filtering orderBy's value to (after, before] — see
// executors.QueryRequest's After/Before doc comment for the exact
// semantics.
func rangeWhere(orderBy string, after, before *time.Time) (string, []any) {
	if orderBy == "" || (after == nil && before == nil) {
		return "", nil
	}
	ident := quoteIdent(orderBy)
	var conds []string
	var args []any
	if after != nil {
		conds = append(conds, ident+" > ?")
		args = append(args, *after)
	}
	if before != nil {
		conds = append(conds, ident+" <= ?")
		args = append(args, *before)
	}
	return " WHERE " + strings.Join(conds, " AND "), args
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
func configure(ctx context.Context, conn *sql.Conn, req executors.QueryRequest) error {
	endpoint := req.Endpoint
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
			)`, req.AccessKeyID, req.SecretAccessKey, endpoint, useSSL),
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
