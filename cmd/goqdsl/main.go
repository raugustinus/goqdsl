package main

import (
	"fmt"

	q "github.com/raugustinus/goqdsl"
)

func main() {
	fmt.Println("=== GoQDSL Demo ===")
	fmt.Println()

	// --- SELECT ---
	selectQuery := q.Select("uuid", "name", "created").
		From("foo").
		Where(q.Eq("uuid", "d3b2aa81-348d-4727-af3f-81eaa9433962")).
		OrderBy("created", q.Desc).
		Limit(1)

	sql, args := selectQuery.Build()
	fmt.Println("SELECT (parameterized):")
	fmt.Printf("  SQL:  %s\n", sql)
	fmt.Printf("  Args: %v\n", args)
	fmt.Printf("  Debug: %s\n", q.ToSQL(selectQuery))
	fmt.Println()

	// --- SELECT with JOIN and OR ---
	joinQuery := q.Select("f.uuid", "f.name", "b.label").
		From("foo f").
		InnerJoin("bar b", "f.uuid", "b.foo_uuid").
		Where(
			q.Or(q.Eq("f.name", "alice"), q.Eq("f.name", "bob")),
			q.IsNotNull("b.label"),
		).
		OrderBy("f.name", q.Asc).
		Limit(10).
		Offset(0)

	sql, args = joinQuery.Build()
	fmt.Println("SELECT with JOIN + OR:")
	fmt.Printf("  SQL:  %s\n", sql)
	fmt.Printf("  Args: %v\n", args)
	fmt.Printf("  Debug: %s\n", q.ToSQL(joinQuery))
	fmt.Println()

	// --- INSERT ---
	insertQuery := q.InsertInto("foo").
		Columns("name").
		Values("alice").
		Values("bob").
		Returning("uuid", "created")

	sql, args = insertQuery.Build()
	fmt.Println("INSERT:")
	fmt.Printf("  SQL:  %s\n", sql)
	fmt.Printf("  Args: %v\n", args)
	fmt.Printf("  Debug: %s\n", q.ToSQL(insertQuery))
	fmt.Println()

	// --- UPDATE ---
	updateQuery := q.Update("foo").
		Set("name", "charlie").
		Where(q.Eq("uuid", "d3b2aa81-348d-4727-af3f-81eaa9433962")).
		Returning("uuid", "name")

	sql, args = updateQuery.Build()
	fmt.Println("UPDATE:")
	fmt.Printf("  SQL:  %s\n", sql)
	fmt.Printf("  Args: %v\n", args)
	fmt.Printf("  Debug: %s\n", q.ToSQL(updateQuery))
	fmt.Println()

	// --- DELETE ---
	deleteQuery := q.DeleteFrom("foo").
		Where(q.Eq("uuid", "d3b2aa81-348d-4727-af3f-81eaa9433962")).
		Returning("uuid")

	sql, args = deleteQuery.Build()
	fmt.Println("DELETE:")
	fmt.Printf("  SQL:  %s\n", sql)
	fmt.Printf("  Args: %v\n", args)
	fmt.Printf("  Debug: %s\n", q.ToSQL(deleteQuery))
}
