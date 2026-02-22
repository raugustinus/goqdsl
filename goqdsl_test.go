package goqdsl

import (
	"reflect"
	"testing"
)

// ---------- SELECT ----------

func TestSelectSimple(t *testing.T) {
	sql, args := Select("uuid", "name").
		From("alerts").
		Build()

	wantSQL := "SELECT uuid, name FROM alerts"
	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if len(args) != 0 {
		t.Errorf("args = %v, want empty", args)
	}
}

func TestSelectWithWhere(t *testing.T) {
	sql, args := Select("uuid", "name").
		From("alerts").
		Where(Eq("uuid", "abc-123")).
		Build()

	wantSQL := "SELECT uuid, name FROM alerts WHERE uuid = $1"
	wantArgs := []any{"abc-123"}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestSelectDistinct(t *testing.T) {
	sql, _ := Select("name").
		From("users").
		Distinct().
		Build()

	want := "SELECT DISTINCT name FROM users"
	if sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
}

func TestSelectMultipleWhere(t *testing.T) {
	sql, args := Select("*").
		From("users").
		Where(Eq("active", true), Gt("age", 18)).
		Build()

	wantSQL := "SELECT * FROM users WHERE active = $1 AND age > $2"
	wantArgs := []any{true, 18}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestSelectWithInnerJoin(t *testing.T) {
	sql, args := Select("a.uuid", "b.name").
		From("alerts a").
		InnerJoin("users b", "a.user_id", "b.uuid").
		Where(Eq("a.active", true)).
		Build()

	wantSQL := "SELECT a.uuid, b.name FROM alerts a INNER JOIN users b ON a.user_id = b.uuid WHERE a.active = $1"
	wantArgs := []any{true}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestSelectWithLeftJoin(t *testing.T) {
	sql, _ := Select("a.*", "b.email").
		From("users a").
		LeftJoin("emails b", "a.id", "b.user_id").
		Build()

	want := "SELECT a.*, b.email FROM users a LEFT JOIN emails b ON a.id = b.user_id"
	if sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
}

func TestSelectWithRightJoin(t *testing.T) {
	sql, _ := Select("*").
		From("orders").
		RightJoin("customers", "orders.customer_id", "customers.id").
		Build()

	want := "SELECT * FROM orders RIGHT JOIN customers ON orders.customer_id = customers.id"
	if sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
}

func TestSelectWithFullJoin(t *testing.T) {
	sql, _ := Select("*").
		From("a").
		FullJoin("b", "a.id", "b.a_id").
		Build()

	want := "SELECT * FROM a FULL JOIN b ON a.id = b.a_id"
	if sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
}

func TestSelectWithMultipleJoins(t *testing.T) {
	sql, _ := Select("u.name", "o.total", "p.method").
		From("users u").
		InnerJoin("orders o", "u.id", "o.user_id").
		LeftJoin("payments p", "o.id", "p.order_id").
		Build()

	want := "SELECT u.name, o.total, p.method FROM users u INNER JOIN orders o ON u.id = o.user_id LEFT JOIN payments p ON o.id = p.order_id"
	if sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
}

func TestSelectGroupBy(t *testing.T) {
	sql, args := Select("department", "COUNT(*)").
		From("employees").
		GroupBy("department").
		Having(Gt("COUNT(*)", 5)).
		Build()

	wantSQL := "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > $1"
	wantArgs := []any{5}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestSelectOrderBy(t *testing.T) {
	sql, _ := Select("name", "age").
		From("users").
		OrderBy("age", Desc).
		OrderBy("name", Asc).
		Build()

	want := "SELECT name, age FROM users ORDER BY age DESC, name ASC"
	if sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
}

func TestSelectLimitOffset(t *testing.T) {
	sql, args := Select("*").
		From("logs").
		OrderBy("created", Desc).
		Limit(20).
		Offset(40).
		Build()

	wantSQL := "SELECT * FROM logs ORDER BY created DESC LIMIT $1 OFFSET $2"
	wantArgs := []any{20, 40}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestSelectComplex(t *testing.T) {
	sql, args := Select("u.name", "COUNT(o.id) AS order_count").
		From("users u").
		InnerJoin("orders o", "u.id", "o.user_id").
		Where(Eq("u.active", true), Gte("o.total", 100)).
		GroupBy("u.name").
		Having(Gt("COUNT(o.id)", 2)).
		OrderBy("order_count", Desc).
		Limit(10).
		Build()

	wantSQL := "SELECT u.name, COUNT(o.id) AS order_count FROM users u " +
		"INNER JOIN orders o ON u.id = o.user_id " +
		"WHERE u.active = $1 AND o.total >= $2 " +
		"GROUP BY u.name " +
		"HAVING COUNT(o.id) > $3 " +
		"ORDER BY order_count DESC " +
		"LIMIT $4"
	wantArgs := []any{true, 100, 2, 10}

	if sql != wantSQL {
		t.Errorf("sql =\n  %q\nwant\n  %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

// ---------- SELECT implements Builder ----------

func TestSelectImplementsBuilder(t *testing.T) {
	var _ Builder = Select("1").From("dual")
}

// ---------- INSERT ----------

func TestInsertSingleRow(t *testing.T) {
	sql, args := InsertInto("users").
		Columns("name", "email").
		Values("Alice", "alice@example.com").
		Build()

	wantSQL := "INSERT INTO users (name, email) VALUES ($1, $2)"
	wantArgs := []any{"Alice", "alice@example.com"}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestInsertMultipleRows(t *testing.T) {
	sql, args := InsertInto("users").
		Columns("name", "email").
		Values("Alice", "alice@example.com").
		Values("Bob", "bob@example.com").
		Build()

	wantSQL := "INSERT INTO users (name, email) VALUES ($1, $2), ($3, $4)"
	wantArgs := []any{"Alice", "alice@example.com", "Bob", "bob@example.com"}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestInsertReturning(t *testing.T) {
	sql, args := InsertInto("users").
		Columns("name").
		Values("Alice").
		Returning("uuid", "created").
		Build()

	wantSQL := "INSERT INTO users (name) VALUES ($1) RETURNING uuid, created"
	wantArgs := []any{"Alice"}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestInsertImplementsBuilder(t *testing.T) {
	var _ Builder = InsertInto("t").Columns("a").Values(1)
}

// ---------- UPDATE ----------

func TestUpdateSimple(t *testing.T) {
	sql, args := Update("users").
		Set("name", "Bob").
		Set("email", "bob@new.com").
		Where(Eq("uuid", "abc-123")).
		Build()

	wantSQL := "UPDATE users SET name = $1, email = $2 WHERE uuid = $3"
	wantArgs := []any{"Bob", "bob@new.com", "abc-123"}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestUpdateReturning(t *testing.T) {
	sql, args := Update("users").
		Set("active", false).
		Where(Eq("uuid", "abc")).
		Returning("uuid", "active").
		Build()

	wantSQL := "UPDATE users SET active = $1 WHERE uuid = $2 RETURNING uuid, active"
	wantArgs := []any{false, "abc"}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestUpdateImplementsBuilder(t *testing.T) {
	var _ Builder = Update("t").Set("a", 1)
}

// ---------- DELETE ----------

func TestDeleteSimple(t *testing.T) {
	sql, args := DeleteFrom("users").
		Where(Eq("uuid", "abc-123")).
		Build()

	wantSQL := "DELETE FROM users WHERE uuid = $1"
	wantArgs := []any{"abc-123"}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestDeleteReturning(t *testing.T) {
	sql, _ := DeleteFrom("sessions").
		Where(Lt("expires_at", "now()")).
		Returning("user_id").
		Build()

	wantSQL := "DELETE FROM sessions WHERE expires_at < $1 RETURNING user_id"
	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
}

func TestDeleteNoWhere(t *testing.T) {
	sql, args := DeleteFrom("temp_data").Build()

	wantSQL := "DELETE FROM temp_data"
	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if len(args) != 0 {
		t.Errorf("args = %v, want empty", args)
	}
}

func TestDeleteImplementsBuilder(t *testing.T) {
	var _ Builder = DeleteFrom("t")
}

// ---------- Predicates ----------

func TestPredicateEq(t *testing.T) {
	sql, args, off := Eq("name", "Alice").ToSQL(1)
	if sql != "name = $1" || args[0] != "Alice" || off != 2 {
		t.Errorf("Eq: sql=%q args=%v off=%d", sql, args, off)
	}
}

func TestPredicateNeq(t *testing.T) {
	sql, args, off := Neq("status", "deleted").ToSQL(3)
	if sql != "status != $3" || args[0] != "deleted" || off != 4 {
		t.Errorf("Neq: sql=%q args=%v off=%d", sql, args, off)
	}
}

func TestPredicateGt(t *testing.T) {
	sql, args, off := Gt("age", 18).ToSQL(1)
	if sql != "age > $1" || args[0] != 18 || off != 2 {
		t.Errorf("Gt: sql=%q args=%v off=%d", sql, args, off)
	}
}

func TestPredicateGte(t *testing.T) {
	sql, _, _ := Gte("score", 90).ToSQL(1)
	if sql != "score >= $1" {
		t.Errorf("Gte: sql=%q", sql)
	}
}

func TestPredicateLt(t *testing.T) {
	sql, _, _ := Lt("price", 50).ToSQL(1)
	if sql != "price < $1" {
		t.Errorf("Lt: sql=%q", sql)
	}
}

func TestPredicateLte(t *testing.T) {
	sql, _, _ := Lte("qty", 0).ToSQL(1)
	if sql != "qty <= $1" {
		t.Errorf("Lte: sql=%q", sql)
	}
}

func TestPredicateLike(t *testing.T) {
	sql, args, _ := Like("name", "%alice%").ToSQL(1)
	if sql != "name LIKE $1" || args[0] != "%alice%" {
		t.Errorf("Like: sql=%q args=%v", sql, args)
	}
}

func TestPredicateILike(t *testing.T) {
	sql, _, _ := ILike("name", "%bob%").ToSQL(1)
	if sql != "name ILIKE $1" {
		t.Errorf("ILike: sql=%q", sql)
	}
}

func TestPredicateIn(t *testing.T) {
	sql, args, off := In("status", "active", "pending", "review").ToSQL(1)
	wantSQL := "status IN ($1, $2, $3)"
	wantArgs := []any{"active", "pending", "review"}

	if sql != wantSQL {
		t.Errorf("In: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("In: args=%v, want %v", args, wantArgs)
	}
	if off != 4 {
		t.Errorf("In: off=%d, want 4", off)
	}
}

func TestPredicateBetween(t *testing.T) {
	sql, args, off := Between("age", 18, 65).ToSQL(1)
	if sql != "age BETWEEN $1 AND $2" {
		t.Errorf("Between: sql=%q", sql)
	}
	if !reflect.DeepEqual(args, []any{18, 65}) {
		t.Errorf("Between: args=%v", args)
	}
	if off != 3 {
		t.Errorf("Between: off=%d", off)
	}
}

func TestPredicateIsNull(t *testing.T) {
	sql, args, off := IsNull("deleted_at").ToSQL(5)
	if sql != "deleted_at IS NULL" || args != nil || off != 5 {
		t.Errorf("IsNull: sql=%q args=%v off=%d", sql, args, off)
	}
}

func TestPredicateIsNotNull(t *testing.T) {
	sql, args, off := IsNotNull("email").ToSQL(1)
	if sql != "email IS NOT NULL" || args != nil || off != 1 {
		t.Errorf("IsNotNull: sql=%q args=%v off=%d", sql, args, off)
	}
}

func TestPredicateAnd(t *testing.T) {
	sql, args, off := And(Eq("a", 1), Eq("b", 2)).ToSQL(1)
	wantSQL := "(a = $1 AND b = $2)"
	wantArgs := []any{1, 2}

	if sql != wantSQL {
		t.Errorf("And: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("And: args=%v, want %v", args, wantArgs)
	}
	if off != 3 {
		t.Errorf("And: off=%d, want 3", off)
	}
}

func TestPredicateOr(t *testing.T) {
	sql, args, off := Or(Eq("status", "active"), Eq("status", "pending")).ToSQL(1)
	wantSQL := "(status = $1 OR status = $2)"
	wantArgs := []any{"active", "pending"}

	if sql != wantSQL {
		t.Errorf("Or: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("Or: args=%v, want %v", args, wantArgs)
	}
	if off != 3 {
		t.Errorf("Or: off=%d, want 3", off)
	}
}

func TestPredicateNot(t *testing.T) {
	sql, args, off := Not(Eq("deleted", true)).ToSQL(1)
	wantSQL := "NOT (deleted = $1)"
	wantArgs := []any{true}

	if sql != wantSQL {
		t.Errorf("Not: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("Not: args=%v, want %v", args, wantArgs)
	}
	if off != 2 {
		t.Errorf("Not: off=%d, want 2", off)
	}
}

func TestPredicateNestedOrAnd(t *testing.T) {
	// WHERE (a = $1 AND (b = $2 OR c = $3))
	pred := And(Eq("a", 1), Or(Eq("b", 2), Eq("c", 3)))
	sql, args, off := pred.ToSQL(1)

	wantSQL := "(a = $1 AND (b = $2 OR c = $3))"
	wantArgs := []any{1, 2, 3}

	if sql != wantSQL {
		t.Errorf("Nested: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("Nested: args=%v, want %v", args, wantArgs)
	}
	if off != 4 {
		t.Errorf("Nested: off=%d, want 4", off)
	}
}

// ---------- ToSQL debug ----------

func TestToSQLDebug(t *testing.T) {
	b := Select("uuid", "name").
		From("users").
		Where(Eq("name", "Alice"), Gt("age", 18)).
		Limit(10)

	got := ToSQL(b)
	want := "SELECT uuid, name FROM users WHERE name = 'Alice' AND age > 18 LIMIT 10"

	if got != want {
		t.Errorf("ToSQL =\n  %q\nwant\n  %q", got, want)
	}
}

func TestToSQLDebugWithNull(t *testing.T) {
	b := Select("*").From("t").Where(Eq("x", nil))
	got := ToSQL(b)
	want := "SELECT * FROM t WHERE x = NULL"

	if got != want {
		t.Errorf("ToSQL = %q, want %q", got, want)
	}
}

func TestToSQLDebugWithBool(t *testing.T) {
	b := Update("users").Set("active", true).Where(Eq("uuid", "abc"))
	got := ToSQL(b)
	want := "UPDATE users SET active = TRUE WHERE uuid = 'abc'"

	if got != want {
		t.Errorf("ToSQL = %q, want %q", got, want)
	}
}

func TestToSQLDebugInsert(t *testing.T) {
	b := InsertInto("users").Columns("name", "age").Values("Alice", 30)
	got := ToSQL(b)
	want := "INSERT INTO users (name, age) VALUES ('Alice', 30)"

	if got != want {
		t.Errorf("ToSQL = %q, want %q", got, want)
	}
}

func TestToSQLDebugEscapeQuotes(t *testing.T) {
	b := Select("*").From("t").Where(Eq("name", "O'Brien"))
	got := ToSQL(b)
	want := "SELECT * FROM t WHERE name = 'O''Brien'"

	if got != want {
		t.Errorf("ToSQL = %q, want %q", got, want)
	}
}

// ---------- Where with Or in SELECT ----------

func TestSelectWhereWithOr(t *testing.T) {
	sql, args := Select("*").
		From("users").
		Where(
			Or(Eq("role", "admin"), Eq("role", "superadmin")),
			Eq("active", true),
		).
		Build()

	wantSQL := "SELECT * FROM users WHERE (role = $1 OR role = $2) AND active = $3"
	wantArgs := []any{"admin", "superadmin", true}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

// ---------- INSERT ON CONFLICT ----------

func TestInsertOnConflictDoNothing(t *testing.T) {
	sql, args := InsertInto("follows").
		Columns("follower_id", "following_id").
		Values(1, 2).
		OnConflict("DO NOTHING").
		Build()

	wantSQL := "INSERT INTO follows (follower_id, following_id) VALUES ($1, $2) ON CONFLICT DO NOTHING"
	wantArgs := []any{1, 2}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestInsertOnConflictWithReturning(t *testing.T) {
	sql, args := InsertInto("users").
		Columns("email", "name").
		Values("a@b.com", "Alice").
		OnConflict("(email) DO UPDATE SET name = EXCLUDED.name").
		Returning("id").
		Build()

	wantSQL := "INSERT INTO users (email, name) VALUES ($1, $2) ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name RETURNING id"
	wantArgs := []any{"a@b.com", "Alice"}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

// ---------- Raw predicate ----------

func TestRawPredicateSimple(t *testing.T) {
	sql, args, off := Raw("status = $1", "active").ToSQL(1)
	if sql != "status = $1" || args[0] != "active" || off != 2 {
		t.Errorf("Raw: sql=%q args=%v off=%d", sql, args, off)
	}
}

func TestRawPredicateWithOffset(t *testing.T) {
	sql, args, off := Raw("follower_id = $1 AND following_id = $2", 10, 20).ToSQL(5)
	wantSQL := "follower_id = $5 AND following_id = $6"
	wantArgs := []any{10, 20}

	if sql != wantSQL {
		t.Errorf("Raw: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("Raw: args=%v, want %v", args, wantArgs)
	}
	if off != 7 {
		t.Errorf("Raw: off=%d, want 7", off)
	}
}

func TestRawPredicateReusedPlaceholder(t *testing.T) {
	// PostgreSQL allows using $1 multiple times
	sql, args, off := Raw("(sender_id = $1 OR receiver_id = $1)", 42).ToSQL(3)
	wantSQL := "(sender_id = $3 OR receiver_id = $3)"
	wantArgs := []any{42}

	if sql != wantSQL {
		t.Errorf("Raw: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("Raw: args=%v, want %v", args, wantArgs)
	}
	if off != 4 {
		t.Errorf("Raw: off=%d, want 4", off)
	}
}

func TestRawPredicateNoArgs(t *testing.T) {
	sql, args, off := Raw("created_at > now() - interval '1 day'").ToSQL(1)
	if sql != "created_at > now() - interval '1 day'" || len(args) != 0 || off != 1 {
		t.Errorf("Raw: sql=%q args=%v off=%d", sql, args, off)
	}
}

func TestRawPredicateInWhere(t *testing.T) {
	sql, args := Select("id", "name").
		From("users").
		Where(
			Eq("active", true),
			Raw("age BETWEEN $1 AND $2", 18, 65),
		).
		Build()

	wantSQL := "SELECT id, name FROM users WHERE active = $1 AND age BETWEEN $2 AND $3"
	wantArgs := []any{true, 18, 65}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}
