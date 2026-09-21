// Package postgres executes paginated reads against a resolved Postgres
// target table, returning both row data and column metadata (discovered
// live via information_schema) so callers can render results without
// hardcoding a schema per projection.
package postgres

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"context"
)

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Executor caches one *sql.DB per DSN so repeated queries against the same
// connection reuse pooled connections instead of dialing per request.
type Executor struct {
	mu  sync.Mutex
	dbs map[string]*sql.DB
}

func New() *Executor {
	return &Executor{dbs: make(map[string]*sql.DB)}
}

func (e *Executor) db(dsn string) (*sql.DB, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if db, ok := e.dbs[dsn]; ok {
		return db, nil
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	e.dbs[dsn] = db
	return db, nil
}

// Query returns the target table's columns (in ordinal order), a page of
// rows, and the total row count. orderBy, when non-empty, names a column to
// sort by descending before applying limit/offset — required for stable,
// recency-first pagination when this executor is one half of a tiered
// (postgres + lake) projection; left empty it preserves the historical
// unordered-scan behavior for plain single-store projections.
func (e *Executor) Query(ctx context.Context, dsn, table, orderBy string, limit, offset int) ([]Column, [][]any, int64, error) {
	db, err := e.db(dsn)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("connecting to postgres: %w", err)
	}

	ident := pgx.Identifier{table}.Sanitize()

	columns, err := e.columns(ctx, db, table)
	if err != nil {
		return nil, nil, 0, err
	}
	if len(columns) == 0 {
		return nil, nil, 0, fmt.Errorf("table %q not found", table)
	}

	var total int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", ident)).Scan(&total); err != nil {
		return nil, nil, 0, fmt.Errorf("counting rows: %w", err)
	}

	query := fmt.Sprintf("SELECT * FROM %s", ident)
	if orderBy != "" {
		query += fmt.Sprintf(" ORDER BY %s DESC", pgx.Identifier{orderBy}.Sanitize())
	}
	query += " LIMIT $1 OFFSET $2"

	rows, err := db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("querying rows: %w", err)
	}
	defer rows.Close()

	page, err := scanRows(rows, len(columns))
	if err != nil {
		return nil, nil, 0, err
	}

	return columns, page, total, nil
}

func (e *Executor) columns(ctx context.Context, db *sql.DB, table string) ([]Column, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`, table)
	if err != nil {
		return nil, fmt.Errorf("introspecting columns: %w", err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var c Column
		if err := rows.Scan(&c.Name, &c.Type); err != nil {
			return nil, err
		}
		columns = append(columns, c)
	}
	return columns, rows.Err()
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
