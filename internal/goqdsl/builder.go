// Package goqdsl provides a fluent query builder DSL for constructing SQL
// statements targeting PostgreSQL, with parameterized query support and
// a database/sql execution layer.
package goqdsl

// Builder is the common interface for all query builders.
// Build returns the parameterized SQL string and its argument values.
type Builder interface {
	Build() (string, []any)
}
