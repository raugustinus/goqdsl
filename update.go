package goqdsl

import (
	"fmt"
	"strings"
)

// setClause holds a column = value pair for UPDATE SET.
type setClause struct {
	col string
	val any
}

// UpdateBuilder constructs UPDATE statements using a fluent API.
type UpdateBuilder struct {
	table     string
	sets      []setClause
	where     []Predicate
	returning []string
}

// Update starts building an UPDATE statement for the given table.
func Update(table string) *UpdateBuilder {
	return &UpdateBuilder{table: table}
}

// Set adds a column = value assignment to the SET clause.
func (b *UpdateBuilder) Set(col string, val any) *UpdateBuilder {
	b.sets = append(b.sets, setClause{col, val})
	return b
}

// Where appends one or more predicates to the WHERE clause (ANDed together).
func (b *UpdateBuilder) Where(preds ...Predicate) *UpdateBuilder {
	b.where = append(b.where, preds...)
	return b
}

// Returning sets the RETURNING clause (PostgreSQL extension).
func (b *UpdateBuilder) Returning(cols ...string) *UpdateBuilder {
	b.returning = cols
	return b
}

// Build generates the parameterized SQL string and its arguments.
func (b *UpdateBuilder) Build() (string, []any) {
	var sb strings.Builder
	var args []any
	offset := 1

	fmt.Fprintf(&sb, "UPDATE %s SET ", b.table)

	// SET clauses
	setParts := make([]string, len(b.sets))
	for i, s := range b.sets {
		setParts[i] = fmt.Sprintf("%s = $%d", s.col, offset)
		args = append(args, s.val)
		offset++
	}
	sb.WriteString(strings.Join(setParts, ", "))

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
