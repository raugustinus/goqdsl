package goqdsl

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

// DB wraps a *sql.DB and provides query execution helpers.
type DB struct {
	conn *sql.DB
}

// NewDB creates a new DB wrapper around the given *sql.DB connection.
func NewDB(conn *sql.DB) *DB {
	return &DB{conn: conn}
}

// Conn returns the underlying *sql.DB.
func (db *DB) Conn() *sql.DB {
	return db.conn
}

// Exec executes a builder's query (INSERT, UPDATE, DELETE) and returns the result.
func (db *DB) Exec(ctx context.Context, b Builder) (sql.Result, error) {
	query, args := b.Build()
	return db.conn.ExecContext(ctx, query, args...)
}

// QueryRow executes a builder's query and returns a single *sql.Row.
func (db *DB) QueryRow(ctx context.Context, b Builder) *sql.Row {
	query, args := b.Build()
	return db.conn.QueryRowContext(ctx, query, args...)
}

// Query executes a builder's query and returns *sql.Rows.
func (db *DB) Query(ctx context.Context, b Builder) (*sql.Rows, error) {
	query, args := b.Build()
	return db.conn.QueryContext(ctx, query, args...)
}

// FetchOne executes the builder's query and scans the first row into a struct
// of type T. Struct fields are matched to columns by their `db` tag, or by
// the lowercase field name if no tag is present.
//
// Usage:
//
//	type User struct {
//	    UUID string `db:"uuid"`
//	    Name string `db:"name"`
//	}
//	user, err := goqdsl.FetchOne[User](ctx, db, Select("uuid", "name").From("users").Where(Eq("uuid", id)))
func FetchOne[T any](ctx context.Context, db *DB, b Builder) (T, error) {
	var zero T
	query, args := b.Build()

	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return zero, fmt.Errorf("goqdsl: query failed: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return zero, fmt.Errorf("goqdsl: rows error: %w", err)
		}
		return zero, sql.ErrNoRows
	}

	result, err := scanStruct[T](rows)
	if err != nil {
		return zero, err
	}

	return result, nil
}

// FetchAll executes the builder's query and scans all rows into a slice
// of structs of type T. See FetchOne for struct tag conventions.
//
// Usage:
//
//	users, err := goqdsl.FetchAll[User](ctx, db, Select("uuid", "name").From("users"))
func FetchAll[T any](ctx context.Context, db *DB, b Builder) ([]T, error) {
	query, args := b.Build()

	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("goqdsl: query failed: %w", err)
	}
	defer rows.Close()

	var results []T
	for rows.Next() {
		item, err := scanStruct[T](rows)
		if err != nil {
			return nil, err
		}
		results = append(results, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goqdsl: rows iteration error: %w", err)
	}

	return results, nil
}

// scanStruct scans the current row into a struct of type T by matching
// column names to struct fields via the `db` tag.
func scanStruct[T any](rows *sql.Rows) (T, error) {
	var result T

	columns, err := rows.Columns()
	if err != nil {
		return result, fmt.Errorf("goqdsl: failed to get columns: %w", err)
	}

	// Build a map from db tag (or lowercase field name) -> field index.
	t := reflect.TypeOf(result)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	fieldMap := make(map[string]int, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("db")
		if tag == "-" {
			continue
		}
		if tag == "" {
			tag = f.Name
		}
		fieldMap[tag] = i
	}

	// Create scan destinations aligned to the column order.
	val := reflect.ValueOf(&result).Elem()
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	dests := make([]any, len(columns))
	for i, col := range columns {
		if idx, ok := fieldMap[col]; ok {
			dests[i] = val.Field(idx).Addr().Interface()
		} else {
			// Column not mapped to a field — discard.
			var discard any
			dests[i] = &discard
		}
	}

	if err := rows.Scan(dests...); err != nil {
		return result, fmt.Errorf("goqdsl: scan failed: %w", err)
	}

	return result, nil
}
