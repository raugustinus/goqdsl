package goqdsl

import (
	"fmt"
	"strings"
)

// ToSQL is a debug helper that returns the SQL with parameter placeholders
// replaced by their inlined values. The output is NOT safe for execution
// against a database — use Build() for that. This is intended for logging
// and debugging only.
func ToSQL(b Builder) string {
	query, args := b.Build()
	result := query

	// Replace placeholders in reverse order so that $10 is replaced before $1.
	for i := len(args) - 1; i >= 0; i-- {
		placeholder := fmt.Sprintf("$%d", i+1)
		val := formatValue(args[i])
		result = strings.Replace(result, placeholder, val, 1)
	}

	return result
}

// formatValue converts a value to its SQL literal representation for debug output.
func formatValue(v any) string {
	switch val := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case nil:
		return "NULL"
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("%v", val)
	}
}
