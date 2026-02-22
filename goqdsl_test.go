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

	wantSQL := "SELECT uuid, name FROM alerts WHERE uuid = @p1"
	wantArgs := map[string]any{"p1": "abc-123"}

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

	wantSQL := "SELECT * FROM users WHERE active = @p1 AND age > @p2"
	wantArgs := map[string]any{"p1": true, "p2": 18}

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

	wantSQL := "SELECT a.uuid, b.name FROM alerts a INNER JOIN users b ON a.user_id = b.uuid WHERE a.active = @p1"
	wantArgs := map[string]any{"p1": true}

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

	wantSQL := "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > @p1"
	wantArgs := map[string]any{"p1": 5}

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

	wantSQL := "SELECT * FROM logs ORDER BY created DESC LIMIT @p1 OFFSET @p2"
	wantArgs := map[string]any{"p1": 20, "p2": 40}

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
		"WHERE u.active = @p1 AND o.total >= @p2 " +
		"GROUP BY u.name " +
		"HAVING COUNT(o.id) > @p3 " +
		"ORDER BY order_count DESC " +
		"LIMIT @p4"
	wantArgs := map[string]any{"p1": true, "p2": 100, "p3": 2, "p4": 10}

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

	wantSQL := "INSERT INTO users (name, email) VALUES (@p1, @p2)"
	wantArgs := map[string]any{"p1": "Alice", "p2": "alice@example.com"}

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

	wantSQL := "INSERT INTO users (name, email) VALUES (@p1, @p2), (@p3, @p4)"
	wantArgs := map[string]any{"p1": "Alice", "p2": "alice@example.com", "p3": "Bob", "p4": "bob@example.com"}

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

	wantSQL := "INSERT INTO users (name) VALUES (@p1) RETURNING uuid, created"
	wantArgs := map[string]any{"p1": "Alice"}

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

	wantSQL := "UPDATE users SET name = @p1, email = @p2 WHERE uuid = @p3"
	wantArgs := map[string]any{"p1": "Bob", "p2": "bob@new.com", "p3": "abc-123"}

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

	wantSQL := "UPDATE users SET active = @p1 WHERE uuid = @p2 RETURNING uuid, active"
	wantArgs := map[string]any{"p1": false, "p2": "abc"}

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

	wantSQL := "DELETE FROM users WHERE uuid = @p1"
	wantArgs := map[string]any{"p1": "abc-123"}

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

	wantSQL := "DELETE FROM sessions WHERE expires_at < @p1 RETURNING user_id"
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
	c := 0
	sql, args := Eq("name", "Alice").ToSQL(&c)
	if sql != "name = @p1" || args["p1"] != "Alice" || c != 1 {
		t.Errorf("Eq: sql=%q args=%v c=%d", sql, args, c)
	}
}

func TestPredicateNeq(t *testing.T) {
	c := 2
	sql, args := Neq("status", "deleted").ToSQL(&c)
	if sql != "status != @p3" || args["p3"] != "deleted" || c != 3 {
		t.Errorf("Neq: sql=%q args=%v c=%d", sql, args, c)
	}
}

func TestPredicateGt(t *testing.T) {
	c := 0
	sql, args := Gt("age", 18).ToSQL(&c)
	if sql != "age > @p1" || args["p1"] != 18 || c != 1 {
		t.Errorf("Gt: sql=%q args=%v c=%d", sql, args, c)
	}
}

func TestPredicateGte(t *testing.T) {
	c := 0
	sql, _ := Gte("score", 90).ToSQL(&c)
	if sql != "score >= @p1" {
		t.Errorf("Gte: sql=%q", sql)
	}
}

func TestPredicateLt(t *testing.T) {
	c := 0
	sql, _ := Lt("price", 50).ToSQL(&c)
	if sql != "price < @p1" {
		t.Errorf("Lt: sql=%q", sql)
	}
}

func TestPredicateLte(t *testing.T) {
	c := 0
	sql, _ := Lte("qty", 0).ToSQL(&c)
	if sql != "qty <= @p1" {
		t.Errorf("Lte: sql=%q", sql)
	}
}

func TestPredicateLike(t *testing.T) {
	c := 0
	sql, args := Like("name", "%alice%").ToSQL(&c)
	if sql != "name LIKE @p1" || args["p1"] != "%alice%" {
		t.Errorf("Like: sql=%q args=%v", sql, args)
	}
}

func TestPredicateILike(t *testing.T) {
	c := 0
	sql, _ := ILike("name", "%bob%").ToSQL(&c)
	if sql != "name ILIKE @p1" {
		t.Errorf("ILike: sql=%q", sql)
	}
}

func TestPredicateIn(t *testing.T) {
	c := 0
	sql, args := In("status", "active", "pending", "review").ToSQL(&c)
	wantSQL := "status IN (@p1, @p2, @p3)"
	wantArgs := map[string]any{"p1": "active", "p2": "pending", "p3": "review"}

	if sql != wantSQL {
		t.Errorf("In: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("In: args=%v, want %v", args, wantArgs)
	}
	if c != 3 {
		t.Errorf("In: c=%d, want 3", c)
	}
}

func TestPredicateBetween(t *testing.T) {
	c := 0
	sql, args := Between("age", 18, 65).ToSQL(&c)
	if sql != "age BETWEEN @p1 AND @p2" {
		t.Errorf("Between: sql=%q", sql)
	}
	if !reflect.DeepEqual(args, map[string]any{"p1": 18, "p2": 65}) {
		t.Errorf("Between: args=%v", args)
	}
	if c != 2 {
		t.Errorf("Between: c=%d", c)
	}
}

func TestPredicateIsNull(t *testing.T) {
	c := 4
	sql, args := IsNull("deleted_at").ToSQL(&c)
	if sql != "deleted_at IS NULL" || args != nil || c != 4 {
		t.Errorf("IsNull: sql=%q args=%v c=%d", sql, args, c)
	}
}

func TestPredicateIsNotNull(t *testing.T) {
	c := 0
	sql, args := IsNotNull("email").ToSQL(&c)
	if sql != "email IS NOT NULL" || args != nil || c != 0 {
		t.Errorf("IsNotNull: sql=%q args=%v c=%d", sql, args, c)
	}
}

func TestPredicateAnd(t *testing.T) {
	c := 0
	sql, args := And(Eq("a", 1), Eq("b", 2)).ToSQL(&c)
	wantSQL := "(a = @p1 AND b = @p2)"
	wantArgs := map[string]any{"p1": 1, "p2": 2}

	if sql != wantSQL {
		t.Errorf("And: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("And: args=%v, want %v", args, wantArgs)
	}
	if c != 2 {
		t.Errorf("And: c=%d, want 2", c)
	}
}

func TestPredicateOr(t *testing.T) {
	c := 0
	sql, args := Or(Eq("status", "active"), Eq("status", "pending")).ToSQL(&c)
	wantSQL := "(status = @p1 OR status = @p2)"
	wantArgs := map[string]any{"p1": "active", "p2": "pending"}

	if sql != wantSQL {
		t.Errorf("Or: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("Or: args=%v, want %v", args, wantArgs)
	}
	if c != 2 {
		t.Errorf("Or: c=%d, want 2", c)
	}
}

func TestPredicateNot(t *testing.T) {
	c := 0
	sql, args := Not(Eq("deleted", true)).ToSQL(&c)
	wantSQL := "NOT (deleted = @p1)"
	wantArgs := map[string]any{"p1": true}

	if sql != wantSQL {
		t.Errorf("Not: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("Not: args=%v, want %v", args, wantArgs)
	}
	if c != 1 {
		t.Errorf("Not: c=%d, want 1", c)
	}
}

func TestPredicateNestedOrAnd(t *testing.T) {
	// WHERE (a = @p1 AND (b = @p2 OR c = @p3))
	c := 0
	pred := And(Eq("a", 1), Or(Eq("b", 2), Eq("c", 3)))
	sql, args := pred.ToSQL(&c)

	wantSQL := "(a = @p1 AND (b = @p2 OR c = @p3))"
	wantArgs := map[string]any{"p1": 1, "p2": 2, "p3": 3}

	if sql != wantSQL {
		t.Errorf("Nested: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("Nested: args=%v, want %v", args, wantArgs)
	}
	if c != 3 {
		t.Errorf("Nested: c=%d, want 3", c)
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

	wantSQL := "SELECT * FROM users WHERE (role = @p1 OR role = @p2) AND active = @p3"
	wantArgs := map[string]any{"p1": "admin", "p2": "superadmin", "p3": true}

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

	wantSQL := "INSERT INTO follows (follower_id, following_id) VALUES (@p1, @p2) ON CONFLICT DO NOTHING"
	wantArgs := map[string]any{"p1": 1, "p2": 2}

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

	wantSQL := "INSERT INTO users (email, name) VALUES (@p1, @p2) ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name RETURNING id"
	wantArgs := map[string]any{"p1": "a@b.com", "p2": "Alice"}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

// ---------- Raw predicate ----------

func TestRawPredicateSimple(t *testing.T) {
	c := 0
	sql, args := Raw("status = @status", map[string]any{"status": "active"}).ToSQL(&c)
	if sql != "status = @status" || args["status"] != "active" {
		t.Errorf("Raw: sql=%q args=%v", sql, args)
	}
}

func TestRawPredicateNamedParams(t *testing.T) {
	c := 0
	sql, args := Raw("follower_id = @uid AND following_id = @tid", map[string]any{"uid": 10, "tid": 20}).ToSQL(&c)
	wantSQL := "follower_id = @uid AND following_id = @tid"
	wantArgs := map[string]any{"uid": 10, "tid": 20}

	if sql != wantSQL {
		t.Errorf("Raw: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("Raw: args=%v, want %v", args, wantArgs)
	}
}

func TestRawPredicateReusedParam(t *testing.T) {
	// Same @id used twice — no re-numbering needed with named params
	c := 0
	sql, args := Raw("(sender_id = @id OR receiver_id = @id)", map[string]any{"id": 42}).ToSQL(&c)
	wantSQL := "(sender_id = @id OR receiver_id = @id)"
	wantArgs := map[string]any{"id": 42}

	if sql != wantSQL {
		t.Errorf("Raw: sql=%q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("Raw: args=%v, want %v", args, wantArgs)
	}
}

func TestRawPredicateNoArgs(t *testing.T) {
	c := 0
	sql, args := Raw("created_at > now() - interval '1 day'", nil).ToSQL(&c)
	if sql != "created_at > now() - interval '1 day'" || args != nil {
		t.Errorf("Raw: sql=%q args=%v", sql, args)
	}
}

func TestRawPredicateInWhere(t *testing.T) {
	sql, args := Select("id", "name").
		From("users").
		Where(
			Eq("active", true),
			Raw("age BETWEEN @low AND @high", map[string]any{"low": 18, "high": 65}),
		).
		Build()

	wantSQL := "SELECT id, name FROM users WHERE active = @p1 AND age BETWEEN @low AND @high"
	wantArgs := map[string]any{"p1": true, "low": 18, "high": 65}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

// ---------- NamedToPositional ----------

func TestNamedToPositional(t *testing.T) {
	sql, args := NamedToPositional(
		"SELECT * FROM users WHERE name = @name AND age > @age",
		map[string]any{"name": "Alice", "age": 18},
	)

	wantSQL := "SELECT * FROM users WHERE name = $1 AND age > $2"
	wantArgs := []any{"Alice", 18}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}

func TestNamedToPositionalReused(t *testing.T) {
	sql, args := NamedToPositional(
		"SELECT * FROM t WHERE a = @id OR b = @id",
		map[string]any{"id": 42},
	)

	wantSQL := "SELECT * FROM t WHERE a = $1 OR b = $1"
	wantArgs := []any{42}

	if sql != wantSQL {
		t.Errorf("sql = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}
