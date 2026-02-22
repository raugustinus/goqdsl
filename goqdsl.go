// Package goqdsl provides a fluent query builder DSL for constructing SQL
// statements targeting PostgreSQL, with named parameter support and
// a database/sql execution layer.
package goqdsl

// Builder is the common interface for all query builders.
// Build returns the SQL string with @name placeholders and a map of named arguments.
type Builder interface {
	Build() (string, map[string]any)
}
