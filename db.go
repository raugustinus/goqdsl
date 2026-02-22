package goqdsl

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"sort"
)

// DB wraps a *sql.DB and provides query execution helpers.
// Named parameters from Build() are automatically converted to positional
// parameters ($1, $2, ...) for database/sql compatibility.
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
	query, named := b.Build()
	positionalSQL, args := namedToPositional(query, named)
	return db.conn.ExecContext(ctx, positionalSQL, args...)
}

// QueryRow executes a builder's query and returns a single *sql.Row.
func (db *DB) QueryRow(ctx context.Context, b Builder) *sql.Row {
	query, named := b.Build()
	positionalSQL, args := namedToPositional(query, named)
	return db.conn.QueryRowContext(ctx, positionalSQL, args...)
}

// Query executes a builder's query and returns *sql.Rows.
func (db *DB) Query(ctx context.Context, b Builder) (*sql.Rows, error) {
	query, named := b.Build()
	positionalSQL, args := namedToPositional(query, named)
	return db.conn.QueryContext(ctx, positionalSQL, args...)
}

var namedParamRe = regexp.MustCompile(`@(\w+)`)

// namedToPositional converts a SQL string with @name placeholders to
// positional $1, $2, ... placeholders for database/sql compatibility.
// Repeated @name references map to the same $N.
func namedToPositional(query string, named map[string]any) (string, []any) {
	if len(named) == 0 {
		return query, nil
	}

	seen := make(map[string]int) // name -> $N position
	var args []any
	counter := 0

	result := namedParamRe.ReplaceAllStringFunc(query, func(match string) string {
		name := match[1:] // strip @
		if pos, ok := seen[name]; ok {
			return fmt.Sprintf("$%d", pos)
		}
		counter++
		seen[name] = counter
		args = append(args, named[name])
		return fmt.Sprintf("$%d", counter)
	})

	return result, args
}

// NamedToPositional is the exported version of namedToPositional for use
// by callers who need to convert named params to positional (e.g. for
// database/sql drivers that don't support named parameters).
func NamedToPositional(query string, named map[string]any) (string, []any) {
	return namedToPositional(query, named)
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
	query, named := b.Build()
	positionalSQL, args := namedToPositional(query, named)

	rows, err := db.conn.QueryContext(ctx, positionalSQL, args...)
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
	query, named := b.Build()
	positionalSQL, args := namedToPositional(query, named)

	rows, err := db.conn.QueryContext(ctx, positionalSQL, args...)
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

// sortedKeys returns map keys sorted by length descending, then alphabetically.
// Used by debug helpers to avoid partial replacements (e.g. @p10 before @p1).
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) != len(keys[j]) {
			return len(keys[i]) > len(keys[j])
		}
		return keys[i] < keys[j]
	})
	return keys
}
