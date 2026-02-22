package goqdsl

import (
	"fmt"
	"strings"
)

// JoinType represents the type of SQL JOIN.
type JoinType int

const (
	InnerJoinType JoinType = iota
	LeftJoinType
	RightJoinType
	FullJoinType
)

func (jt JoinType) String() string {
	switch jt {
	case LeftJoinType:
		return "LEFT JOIN"
	case RightJoinType:
		return "RIGHT JOIN"
	case FullJoinType:
		return "FULL JOIN"
	default:
		return "INNER JOIN"
	}
}

// OrderDir represents sort direction.
type OrderDir int

const (
	Asc OrderDir = iota
	Desc
)

func (d OrderDir) String() string {
	if d == Desc {
		return "DESC"
	}
	return "ASC"
}

// join holds a parsed JOIN clause.
type join struct {
	joinType JoinType
	table    string
	left     string
	right    string
}

// orderBy holds a parsed ORDER BY clause.
type orderBy struct {
	col string
	dir OrderDir
}

// SelectBuilder constructs SELECT queries using a fluent API.
// Use Select() to create one, then chain From, Where, etc.
type SelectBuilder struct {
	distinct bool
	fields   []string
	from     string
	joins    []join
	where    []Predicate
	groupBy  []string
	having   []Predicate
	orderBy  []orderBy
	limit    *int
	offset   *int
}

// Select starts building a SELECT query with the given columns.
func Select(fields ...string) *SelectBuilder {
	return &SelectBuilder{fields: fields}
}

// Distinct marks the query as SELECT DISTINCT.
func (b *SelectBuilder) Distinct() *SelectBuilder {
	b.distinct = true
	return b
}

// From sets the target table (may include alias, e.g. "users u").
func (b *SelectBuilder) From(table string) *SelectBuilder {
	b.from = table
	return b
}

// InnerJoin adds an INNER JOIN clause.
func (b *SelectBuilder) InnerJoin(table, left, right string) *SelectBuilder {
	b.joins = append(b.joins, join{InnerJoinType, table, left, right})
	return b
}

// LeftJoin adds a LEFT JOIN clause.
func (b *SelectBuilder) LeftJoin(table, left, right string) *SelectBuilder {
	b.joins = append(b.joins, join{LeftJoinType, table, left, right})
	return b
}

// RightJoin adds a RIGHT JOIN clause.
func (b *SelectBuilder) RightJoin(table, left, right string) *SelectBuilder {
	b.joins = append(b.joins, join{RightJoinType, table, left, right})
	return b
}

// FullJoin adds a FULL OUTER JOIN clause.
func (b *SelectBuilder) FullJoin(table, left, right string) *SelectBuilder {
	b.joins = append(b.joins, join{FullJoinType, table, left, right})
	return b
}

// Where appends one or more predicates to the WHERE clause (ANDed together).
func (b *SelectBuilder) Where(preds ...Predicate) *SelectBuilder {
	b.where = append(b.where, preds...)
	return b
}

// GroupBy sets the GROUP BY columns.
func (b *SelectBuilder) GroupBy(cols ...string) *SelectBuilder {
	b.groupBy = append(b.groupBy, cols...)
	return b
}

// Having appends predicates to the HAVING clause (ANDed together).
func (b *SelectBuilder) Having(preds ...Predicate) *SelectBuilder {
	b.having = append(b.having, preds...)
	return b
}

// OrderBy adds an ORDER BY clause for the given column and direction.
func (b *SelectBuilder) OrderBy(col string, dir OrderDir) *SelectBuilder {
	b.orderBy = append(b.orderBy, orderBy{col, dir})
	return b
}

// Limit sets the LIMIT value.
func (b *SelectBuilder) Limit(n int) *SelectBuilder {
	b.limit = &n
	return b
}

// Offset sets the OFFSET value.
func (b *SelectBuilder) Offset(n int) *SelectBuilder {
	b.offset = &n
	return b
}

// Build generates the SQL string with @name placeholders and a map of named arguments.
func (b *SelectBuilder) Build() (string, map[string]any) {
	var sb strings.Builder
	args := make(map[string]any)
	counter := 0

	// SELECT
	if b.distinct {
		sb.WriteString("SELECT DISTINCT ")
	} else {
		sb.WriteString("SELECT ")
	}
	sb.WriteString(strings.Join(b.fields, ", "))

	// FROM
	sb.WriteString(" FROM ")
	sb.WriteString(b.from)

	// JOINs
	for _, j := range b.joins {
		fmt.Fprintf(&sb, " %s %s ON %s = %s", j.joinType, j.table, j.left, j.right)
	}

	// WHERE
	if len(b.where) > 0 {
		sb.WriteString(" WHERE ")
		writePredicates(&sb, b.where, &counter, args)
	}

	// GROUP BY
	if len(b.groupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(b.groupBy, ", "))
	}

	// HAVING
	if len(b.having) > 0 {
		sb.WriteString(" HAVING ")
		writePredicates(&sb, b.having, &counter, args)
	}

	// ORDER BY
	if len(b.orderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		parts := make([]string, len(b.orderBy))
		for i, o := range b.orderBy {
			parts[i] = fmt.Sprintf("%s %s", o.col, o.dir)
		}
		sb.WriteString(strings.Join(parts, ", "))
	}

	// LIMIT
	if b.limit != nil {
		name := nextParam(&counter)
		fmt.Fprintf(&sb, " LIMIT @%s", name)
		args[name] = *b.limit
	}

	// OFFSET
	if b.offset != nil {
		name := nextParam(&counter)
		fmt.Fprintf(&sb, " OFFSET @%s", name)
		args[name] = *b.offset
	}

	return sb.String(), args
}

// writePredicates renders a slice of predicates joined by AND into the
// string builder, merging arguments into the provided map.
func writePredicates(sb *strings.Builder, preds []Predicate, counter *int, args map[string]any) {
	for i, p := range preds {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		sql, pArgs := p.ToSQL(counter)
		sb.WriteString(sql)
		mergeArgs(args, pArgs)
	}
}
