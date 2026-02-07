package goqdsl

import (
	"fmt"
	"strings"
)

// DeleteBuilder constructs DELETE statements using a fluent API.
type DeleteBuilder struct {
	table     string
	where     []Predicate
	returning []string
}

// DeleteFrom starts building a DELETE statement for the given table.
func DeleteFrom(table string) *DeleteBuilder {
	return &DeleteBuilder{table: table}
}

// Where appends one or more predicates to the WHERE clause (ANDed together).
func (b *DeleteBuilder) Where(preds ...Predicate) *DeleteBuilder {
	b.where = append(b.where, preds...)
	return b
}

// Returning sets the RETURNING clause (PostgreSQL extension).
func (b *DeleteBuilder) Returning(cols ...string) *DeleteBuilder {
	b.returning = cols
	return b
}

// Build generates the parameterized SQL string and its arguments.
func (b *DeleteBuilder) Build() (string, []any) {
	var sb strings.Builder
	var args []any
	offset := 1

	fmt.Fprintf(&sb, "DELETE FROM %s", b.table)

	// WHERE
	if len(b.where) > 0 {
		sb.WriteString(" WHERE ")
		offset, args = writePredicates(&sb, b.where, offset, args)
	}

	// RETURNING
	if len(b.returning) > 0 {
		sb.WriteString(" RETURNING ")
		sb.WriteString(strings.Join(b.returning, ", "))
	}

	return sb.String(), args
}
