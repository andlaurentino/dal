// Package postgres executes paginated reads against a resolved Postgres
// target table, returning both row data and column metadata (discovered
// live via information_schema) so callers can render results without
// hardcoding a schema per projection.
package postgres

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"context"

	"github.com/andersonlaurentino/dal-broker/internal/executors"
)

// Executor caches one *sql.DB per DSN so repeated queries against the same
// connection reuse pooled connections instead of dialing per request.
// Implements executors.Executor.
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

// Query returns req.Columns (or, for passthrough, every column of
// req.Table in ordinal order), a page of rows, and the total row count.
// req.OrderBy, when non-empty, names a column to sort by descending before
// applying limit/offset, and is what req.After/req.Before filter
// against — required for stable, recency-first pagination and range
// filtering when this executor is one source of a multi-source
// projection; left empty (with After/Before both nil) it preserves the
// unordered, unfiltered scan behavior of a plain single-source
// projection.
func (e *Executor) Query(ctx context.Context, req executors.QueryRequest) (executors.QueryResult, error) {
	db, err := e.db(req.DSN)
	if err != nil {
		return executors.QueryResult{}, fmt.Errorf("connecting to postgres: %w", err)
	}

	ident := pgx.Identifier{req.Table}.Sanitize()

	allColumns, err := e.columns(ctx, db, req.Table)
	if err != nil {
		return executors.QueryResult{}, err
	}
	if len(allColumns) == 0 {
		return executors.QueryResult{}, fmt.Errorf("table %q not found", req.Table)
	}

	selectExpr, columns, err := selectClause(allColumns, req.Columns)
	if err != nil {
		return executors.QueryResult{}, err
	}

	where, whereArgs := rangeWhere(req.OrderBy, req.After, req.Before, 1)

	var total int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", ident, where)
	if err := db.QueryRowContext(ctx, countQuery, whereArgs...).Scan(&total); err != nil {
		return executors.QueryResult{}, fmt.Errorf("counting rows: %w", err)
	}

	query := fmt.Sprintf("SELECT %s FROM %s%s", selectExpr, ident, where)
	if req.OrderBy != "" {
		query += fmt.Sprintf(" ORDER BY %s DESC", pgx.Identifier{req.OrderBy}.Sanitize())
	}
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(whereArgs)+1, len(whereArgs)+2)

	args := append(append([]any{}, whereArgs...), req.Limit, req.Offset)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return executors.QueryResult{}, fmt.Errorf("querying rows: %w", err)
	}
	defer rows.Close()

	page, err := scanRows(rows, len(columns))
	if err != nil {
		return executors.QueryResult{}, err
	}

	return executors.QueryResult{Columns: columns, Rows: page, Total: total}, nil
}

// selectClause builds the SELECT column list and result metadata: every
// column of allColumns (in ordinal order) for passthrough, or the
// requested subset, renamed and reordered per spec, when the caller asked
// for specific columns.
func selectClause(allColumns []executors.Column, spec []executors.ColumnSpec) (string, []executors.Column, error) {
	if len(spec) == 0 {
		return "*", allColumns, nil
	}

	types := make(map[string]string, len(allColumns))
	for _, c := range allColumns {
		types[c.Name] = c.Type
	}

	parts := make([]string, len(spec))
	columns := make([]executors.Column, len(spec))
	for i, c := range spec {
		typ, ok := types[c.Source]
		if !ok {
			return "", nil, fmt.Errorf("column %q not found", c.Source)
		}
		as := c.As
		if as == "" {
			as = c.Source
		}
		parts[i] = fmt.Sprintf("%s AS %s", pgx.Identifier{c.Source}.Sanitize(), pgx.Identifier{as}.Sanitize())
		columns[i] = executors.Column{Name: as, Type: typ}
	}
	return strings.Join(parts, ", "), columns, nil
}

// rangeWhere builds a " WHERE ..." clause (or "" when neither bound is
// set) filtering orderBy's value to (after, before] — see
// executors.QueryRequest's After/Before doc comment for the exact
// semantics — with placeholders starting at argN.
func rangeWhere(orderBy string, after, before *time.Time, argN int) (string, []any) {
	if orderBy == "" || (after == nil && before == nil) {
		return "", nil
	}
	ident := pgx.Identifier{orderBy}.Sanitize()
	var conds []string
	var args []any
	if after != nil {
		conds = append(conds, fmt.Sprintf("%s > $%d", ident, argN))
		args = append(args, *after)
		argN++
	}
	if before != nil {
		conds = append(conds, fmt.Sprintf("%s <= $%d", ident, argN))
		args = append(args, *before)
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

func (e *Executor) columns(ctx context.Context, db *sql.DB, table string) ([]executors.Column, error) {
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

	var columns []executors.Column
	for rows.Next() {
		var c executors.Column
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
