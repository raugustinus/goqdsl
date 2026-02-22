package goqdsl

import (
	"fmt"
	"strings"
)

// InsertBuilder constructs INSERT statements using a fluent API.
type InsertBuilder struct {
	table      string
	columns    []string
	values     [][]any
	onConflict string
	returning  []string
}

// InsertInto starts building an INSERT statement for the given table.
func InsertInto(table string) *InsertBuilder {
	return &InsertBuilder{table: table}
}

// Columns sets the column names for the INSERT.
func (b *InsertBuilder) Columns(cols ...string) *InsertBuilder {
	b.columns = cols
	return b
}

// Values adds a row of values. Must match the number of columns.
// Can be called multiple times for multi-row inserts.
func (b *InsertBuilder) Values(vals ...any) *InsertBuilder {
	b.values = append(b.values, vals)
	return b
}

// OnConflict appends an ON CONFLICT clause after VALUES.
// Example: .OnConflict("DO NOTHING") or .OnConflict("(email) DO UPDATE SET name = EXCLUDED.name")
func (b *InsertBuilder) OnConflict(action string) *InsertBuilder {
	b.onConflict = action
	return b
}

// Returning sets the RETURNING clause (PostgreSQL extension).
func (b *InsertBuilder) Returning(cols ...string) *InsertBuilder {
	b.returning = cols
	return b
}

// Build generates the parameterized SQL string and its arguments.
func (b *InsertBuilder) Build() (string, []any) {
	var sb strings.Builder
	var args []any
	offset := 1

	fmt.Fprintf(&sb, "INSERT INTO %s", b.table)

	// columns
	if len(b.columns) > 0 {
		sb.WriteString(" (")
		sb.WriteString(strings.Join(b.columns, ", "))
		sb.WriteString(")")
	}

	// values
	sb.WriteString(" VALUES ")
	for i, row := range b.values {
		if i > 0 {
			sb.WriteString(", ")
		}
		placeholders := make([]string, len(row))
		for j, v := range row {
			placeholders[j] = fmt.Sprintf("$%d", offset)
			args = append(args, v)
			offset++
		}
		sb.WriteString("(")
		sb.WriteString(strings.Join(placeholders, ", "))
		sb.WriteString(")")
	}

	// ON CONFLICT
	if b.onConflict != "" {
		sb.WriteString(" ON CONFLICT ")
		sb.WriteString(b.onConflict)
	}

	// RETURNING
	if len(b.returning) > 0 {
		sb.WriteString(" RETURNING ")
		sb.WriteString(strings.Join(b.returning, ", "))
	}

	return sb.String(), args
}
