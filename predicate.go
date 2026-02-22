package goqdsl

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Predicate represents a SQL condition that can render itself as a
// parameterized SQL fragment. The offset parameter indicates the next
// available placeholder number ($1, $2, ...) and the method returns the
// SQL fragment, the arguments consumed, and the new offset.
type Predicate interface {
	ToSQL(offset int) (sql string, args []any, newOffset int)
}

// --- raw predicate ---

var placeholderRe = regexp.MustCompile(`\$(\d+)`)

type rawPred struct {
	sql  string
	args []any
}

// Raw creates a predicate from a raw SQL fragment with positional parameters.
// The $1, $2, ... placeholders are re-numbered to fit the current offset.
// Example: Raw("EXISTS(SELECT 1 FROM follows WHERE follower_id = $1 AND following_id = $2)", userID, targetID)
func Raw(sql string, args ...any) Predicate { return rawPred{sql, args} }

func (p rawPred) ToSQL(offset int) (string, []any, int) {
	result := placeholderRe.ReplaceAllStringFunc(p.sql, func(match string) string {
		n, _ := strconv.Atoi(match[1:])
		return fmt.Sprintf("$%d", n+offset-1)
	})
	return result, p.args, offset + len(p.args)
}

// --- comparison predicates ---

type eqPred struct {
	col string
	val any
}

// Eq creates a column = $N predicate.
func Eq(col string, val any) Predicate { return eqPred{col, val} }

func (p eqPred) ToSQL(offset int) (string, []any, int) {
	return fmt.Sprintf("%s = $%d", p.col, offset), []any{p.val}, offset + 1
}

type neqPred struct {
	col string
	val any
}

// Neq creates a column != $N predicate.
func Neq(col string, val any) Predicate { return neqPred{col, val} }

func (p neqPred) ToSQL(offset int) (string, []any, int) {
	return fmt.Sprintf("%s != $%d", p.col, offset), []any{p.val}, offset + 1
}

type gtPred struct {
	col string
	val any
}

// Gt creates a column > $N predicate.
func Gt(col string, val any) Predicate { return gtPred{col, val} }

func (p gtPred) ToSQL(offset int) (string, []any, int) {
	return fmt.Sprintf("%s > $%d", p.col, offset), []any{p.val}, offset + 1
}

type gtePred struct {
	col string
	val any
}

// Gte creates a column >= $N predicate.
func Gte(col string, val any) Predicate { return gtePred{col, val} }

func (p gtePred) ToSQL(offset int) (string, []any, int) {
	return fmt.Sprintf("%s >= $%d", p.col, offset), []any{p.val}, offset + 1
}

type ltPred struct {
	col string
	val any
}

// Lt creates a column < $N predicate.
func Lt(col string, val any) Predicate { return ltPred{col, val} }

func (p ltPred) ToSQL(offset int) (string, []any, int) {
	return fmt.Sprintf("%s < $%d", p.col, offset), []any{p.val}, offset + 1
}

type ltePred struct {
	col string
	val any
}

// Lte creates a column <= $N predicate.
func Lte(col string, val any) Predicate { return ltePred{col, val} }

func (p ltePred) ToSQL(offset int) (string, []any, int) {
	return fmt.Sprintf("%s <= $%d", p.col, offset), []any{p.val}, offset + 1
}

// --- pattern predicates ---

type likePred struct {
	col     string
	pattern string
}

// Like creates a column LIKE $N predicate.
func Like(col string, pattern string) Predicate { return likePred{col, pattern} }

func (p likePred) ToSQL(offset int) (string, []any, int) {
	return fmt.Sprintf("%s LIKE $%d", p.col, offset), []any{p.pattern}, offset + 1
}

type ilikePred struct {
	col     string
	pattern string
}

// ILike creates a column ILIKE $N predicate (case-insensitive, PostgreSQL extension).
func ILike(col string, pattern string) Predicate { return ilikePred{col, pattern} }

func (p ilikePred) ToSQL(offset int) (string, []any, int) {
	return fmt.Sprintf("%s ILIKE $%d", p.col, offset), []any{p.pattern}, offset + 1
}

// --- set predicates ---

type inPred struct {
	col  string
	vals []any
}

// In creates a column IN ($N, $N+1, ...) predicate.
func In(col string, vals ...any) Predicate { return inPred{col, vals} }

func (p inPred) ToSQL(offset int) (string, []any, int) {
	placeholders := make([]string, len(p.vals))
	for i := range p.vals {
		placeholders[i] = fmt.Sprintf("$%d", offset+i)
	}
	sql := fmt.Sprintf("%s IN (%s)", p.col, strings.Join(placeholders, ", "))
	return sql, p.vals, offset + len(p.vals)
}

type betweenPred struct {
	col  string
	low  any
	high any
}

// Between creates a column BETWEEN $N AND $N+1 predicate.
func Between(col string, low, high any) Predicate { return betweenPred{col, low, high} }

func (p betweenPred) ToSQL(offset int) (string, []any, int) {
	sql := fmt.Sprintf("%s BETWEEN $%d AND $%d", p.col, offset, offset+1)
	return sql, []any{p.low, p.high}, offset + 2
}

// --- null predicates ---

type isNullPred struct {
	col string
}

// IsNull creates a column IS NULL predicate.
func IsNull(col string) Predicate { return isNullPred{col} }

func (p isNullPred) ToSQL(offset int) (string, []any, int) {
	return fmt.Sprintf("%s IS NULL", p.col), nil, offset
}

type isNotNullPred struct {
	col string
}

// IsNotNull creates a column IS NOT NULL predicate.
func IsNotNull(col string) Predicate { return isNotNullPred{col} }

func (p isNotNullPred) ToSQL(offset int) (string, []any, int) {
	return fmt.Sprintf("%s IS NOT NULL", p.col), nil, offset
}

// --- logical combinators ---

type andPred struct {
	preds []Predicate
}

// And combines multiple predicates with AND.
func And(preds ...Predicate) Predicate { return andPred{preds} }

func (p andPred) ToSQL(offset int) (string, []any, int) {
	return combinePredicates(p.preds, "AND", offset)
}

type orPred struct {
	preds []Predicate
}

// Or combines multiple predicates with OR. The result is wrapped in parentheses.
func Or(preds ...Predicate) Predicate { return orPred{preds} }

func (p orPred) ToSQL(offset int) (string, []any, int) {
	return combinePredicates(p.preds, "OR", offset)
}

type notPred struct {
	pred Predicate
}

// Not negates a predicate.
func Not(pred Predicate) Predicate { return notPred{pred} }

func (p notPred) ToSQL(offset int) (string, []any, int) {
	sql, args, newOffset := p.pred.ToSQL(offset)
	return fmt.Sprintf("NOT (%s)", sql), args, newOffset
}

// combinePredicates joins a slice of predicates with the given operator.
func combinePredicates(preds []Predicate, op string, offset int) (string, []any, int) {
	if len(preds) == 0 {
		return "", nil, offset
	}
	if len(preds) == 1 {
		return preds[0].ToSQL(offset)
	}

	parts := make([]string, 0, len(preds))
	var allArgs []any
	cur := offset

	for _, p := range preds {
		sql, args, next := p.ToSQL(cur)
		parts = append(parts, sql)
		allArgs = append(allArgs, args...)
		cur = next
	}

	joined := strings.Join(parts, " "+op+" ")
	return "(" + joined + ")", allArgs, cur
}
