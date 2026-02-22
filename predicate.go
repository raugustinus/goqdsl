package goqdsl

import (
	"fmt"
	"strings"
)

// Predicate represents a SQL condition that can render itself as a
// parameterized SQL fragment using named parameters (@name).
// The counter is used to generate unique parameter names (p1, p2, ...).
type Predicate interface {
	ToSQL(counter *int) (sql string, args map[string]any)
}

// mergeArgs copies all entries from src into dst.
func mergeArgs(dst, src map[string]any) {
	for k, v := range src {
		dst[k] = v
	}
}

// nextParam increments the counter and returns the next parameter name.
func nextParam(counter *int) string {
	*counter++
	return fmt.Sprintf("p%d", *counter)
}

// --- raw predicate ---

type rawPred struct {
	sql  string
	args map[string]any
}

// Raw creates a predicate from a raw SQL fragment with named parameters.
// Example: Raw("EXISTS(SELECT 1 FROM follows WHERE follower_id = @uid AND following_id = @tid)", map[string]any{"uid": userID, "tid": targetID})
func Raw(sql string, args map[string]any) Predicate { return rawPred{sql, args} }

func (p rawPred) ToSQL(counter *int) (string, map[string]any) {
	if p.args == nil {
		return p.sql, nil
	}
	return p.sql, p.args
}

// --- comparison predicates ---

type eqPred struct {
	col string
	val any
}

// Eq creates a column = @name predicate.
func Eq(col string, val any) Predicate { return eqPred{col, val} }

func (p eqPred) ToSQL(counter *int) (string, map[string]any) {
	name := nextParam(counter)
	return fmt.Sprintf("%s = @%s", p.col, name), map[string]any{name: p.val}
}

type neqPred struct {
	col string
	val any
}

// Neq creates a column != @name predicate.
func Neq(col string, val any) Predicate { return neqPred{col, val} }

func (p neqPred) ToSQL(counter *int) (string, map[string]any) {
	name := nextParam(counter)
	return fmt.Sprintf("%s != @%s", p.col, name), map[string]any{name: p.val}
}

type gtPred struct {
	col string
	val any
}

// Gt creates a column > @name predicate.
func Gt(col string, val any) Predicate { return gtPred{col, val} }

func (p gtPred) ToSQL(counter *int) (string, map[string]any) {
	name := nextParam(counter)
	return fmt.Sprintf("%s > @%s", p.col, name), map[string]any{name: p.val}
}

type gtePred struct {
	col string
	val any
}

// Gte creates a column >= @name predicate.
func Gte(col string, val any) Predicate { return gtePred{col, val} }

func (p gtePred) ToSQL(counter *int) (string, map[string]any) {
	name := nextParam(counter)
	return fmt.Sprintf("%s >= @%s", p.col, name), map[string]any{name: p.val}
}

type ltPred struct {
	col string
	val any
}

// Lt creates a column < @name predicate.
func Lt(col string, val any) Predicate { return ltPred{col, val} }

func (p ltPred) ToSQL(counter *int) (string, map[string]any) {
	name := nextParam(counter)
	return fmt.Sprintf("%s < @%s", p.col, name), map[string]any{name: p.val}
}

type ltePred struct {
	col string
	val any
}

// Lte creates a column <= @name predicate.
func Lte(col string, val any) Predicate { return ltePred{col, val} }

func (p ltePred) ToSQL(counter *int) (string, map[string]any) {
	name := nextParam(counter)
	return fmt.Sprintf("%s <= @%s", p.col, name), map[string]any{name: p.val}
}

// --- pattern predicates ---

type likePred struct {
	col     string
	pattern string
}

// Like creates a column LIKE @name predicate.
func Like(col string, pattern string) Predicate { return likePred{col, pattern} }

func (p likePred) ToSQL(counter *int) (string, map[string]any) {
	name := nextParam(counter)
	return fmt.Sprintf("%s LIKE @%s", p.col, name), map[string]any{name: p.pattern}
}

type ilikePred struct {
	col     string
	pattern string
}

// ILike creates a column ILIKE @name predicate (case-insensitive, PostgreSQL extension).
func ILike(col string, pattern string) Predicate { return ilikePred{col, pattern} }

func (p ilikePred) ToSQL(counter *int) (string, map[string]any) {
	name := nextParam(counter)
	return fmt.Sprintf("%s ILIKE @%s", p.col, name), map[string]any{name: p.pattern}
}

// --- set predicates ---

type inPred struct {
	col  string
	vals []any
}

// In creates a column IN (@name1, @name2, ...) predicate.
func In(col string, vals ...any) Predicate { return inPred{col, vals} }

func (p inPred) ToSQL(counter *int) (string, map[string]any) {
	args := make(map[string]any, len(p.vals))
	placeholders := make([]string, len(p.vals))
	for i, v := range p.vals {
		name := nextParam(counter)
		placeholders[i] = "@" + name
		args[name] = v
	}
	sql := fmt.Sprintf("%s IN (%s)", p.col, strings.Join(placeholders, ", "))
	return sql, args
}

type betweenPred struct {
	col  string
	low  any
	high any
}

// Between creates a column BETWEEN @low AND @high predicate.
func Between(col string, low, high any) Predicate { return betweenPred{col, low, high} }

func (p betweenPred) ToSQL(counter *int) (string, map[string]any) {
	lowName := nextParam(counter)
	highName := nextParam(counter)
	sql := fmt.Sprintf("%s BETWEEN @%s AND @%s", p.col, lowName, highName)
	return sql, map[string]any{lowName: p.low, highName: p.high}
}

// --- null predicates ---

type isNullPred struct {
	col string
}

// IsNull creates a column IS NULL predicate.
func IsNull(col string) Predicate { return isNullPred{col} }

func (p isNullPred) ToSQL(counter *int) (string, map[string]any) {
	return fmt.Sprintf("%s IS NULL", p.col), nil
}

type isNotNullPred struct {
	col string
}

// IsNotNull creates a column IS NOT NULL predicate.
func IsNotNull(col string) Predicate { return isNotNullPred{col} }

func (p isNotNullPred) ToSQL(counter *int) (string, map[string]any) {
	return fmt.Sprintf("%s IS NOT NULL", p.col), nil
}

// --- logical combinators ---

type andPred struct {
	preds []Predicate
}

// And combines multiple predicates with AND.
func And(preds ...Predicate) Predicate { return andPred{preds} }

func (p andPred) ToSQL(counter *int) (string, map[string]any) {
	return combinePredicates(p.preds, "AND", counter)
}

type orPred struct {
	preds []Predicate
}

// Or combines multiple predicates with OR. The result is wrapped in parentheses.
func Or(preds ...Predicate) Predicate { return orPred{preds} }

func (p orPred) ToSQL(counter *int) (string, map[string]any) {
	return combinePredicates(p.preds, "OR", counter)
}

type notPred struct {
	pred Predicate
}

// Not negates a predicate.
func Not(pred Predicate) Predicate { return notPred{pred} }

func (p notPred) ToSQL(counter *int) (string, map[string]any) {
	sql, args := p.pred.ToSQL(counter)
	return fmt.Sprintf("NOT (%s)", sql), args
}

// combinePredicates joins a slice of predicates with the given operator.
func combinePredicates(preds []Predicate, op string, counter *int) (string, map[string]any) {
	if len(preds) == 0 {
		return "", nil
	}
	if len(preds) == 1 {
		return preds[0].ToSQL(counter)
	}

	parts := make([]string, 0, len(preds))
	allArgs := make(map[string]any)

	for _, p := range preds {
		sql, args := p.ToSQL(counter)
		parts = append(parts, sql)
		mergeArgs(allArgs, args)
	}

	joined := strings.Join(parts, " "+op+" ")
	return "(" + joined + ")", allArgs
}
