package goqdsl

import (
	"fmt"
	"regexp"
	"strings"
)

var debugParamRe = regexp.MustCompile(`@(\w+)`)

// ToSQL is a debug helper that returns the SQL with named parameter placeholders
// replaced by their inlined values. The output is NOT safe for execution
// against a database — use Build() for that. This is intended for logging
// and debugging only.
func ToSQL(b Builder) string {
	query, args := b.Build()

	result := debugParamRe.ReplaceAllStringFunc(query, func(match string) string {
		name := match[1:] // strip @
		if val, ok := args[name]; ok {
			return formatValue(val)
		}
		return match
	})

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
