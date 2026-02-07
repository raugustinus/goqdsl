# GoQDSL

A fluent, type-safe SQL query builder DSL for Go, targeting PostgreSQL.

Build parameterized queries programmatically using a chainable API instead of
concatenating raw strings. Every query produces `$1, $2, ...` placeholders
safe for execution via `database/sql`.

## Features

- **SELECT** — DISTINCT, JOINs (INNER/LEFT/RIGHT/FULL), WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
- **INSERT** — single and multi-row, RETURNING
- **UPDATE** — fluent SET assignments, WHERE, RETURNING
- **DELETE** — WHERE, RETURNING
- **15 composable predicates** — Eq, Neq, Gt, Gte, Lt, Lte, Like, ILike, In, Between, IsNull, IsNotNull, And, Or, Not
- **Execution layer** — `database/sql` wrapper with generic `FetchOne[T]` and `FetchAll[T]` for scanning rows into structs
- **Debug helper** — `ToSQL()` inlines parameter values for logging

## Quick Start

```go
import q "github.com/raugustinus/goqdsl"
```

### SELECT

```go
query := q.Select("uuid", "name").
    From("users").
    Where(q.Eq("active", true), q.Gt("age", 18)).
    OrderBy("name", q.Asc).
    Limit(10)

sql, args := query.Build()
// sql:  "SELECT uuid, name FROM users WHERE active = $1 AND age > $2 ORDER BY name ASC LIMIT $3"
// args: [true, 18, 10]
```

### SELECT with JOIN and OR

```go
query := q.Select("u.name", "o.total").
    From("users u").
    InnerJoin("orders o", "u.id", "o.user_id").
    Where(
        q.Or(q.Eq("u.role", "admin"), q.Eq("u.role", "manager")),
        q.Gte("o.total", 100),
    ).
    OrderBy("o.total", q.Desc).
    Limit(20)

sql, args := query.Build()
// sql:  "SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id
//        WHERE (u.role = $1 OR u.role = $2) AND o.total >= $3 ORDER BY o.total DESC LIMIT $4"
// args: ["admin", "manager", 100, 20]
```

### INSERT

```go
query := q.InsertInto("users").
    Columns("name", "email").
    Values("Alice", "alice@example.com").
    Values("Bob", "bob@example.com").
    Returning("uuid", "created")

sql, args := query.Build()
// sql:  "INSERT INTO users (name, email) VALUES ($1, $2), ($3, $4) RETURNING uuid, created"
// args: ["Alice", "alice@example.com", "Bob", "bob@example.com"]
```

### UPDATE

```go
query := q.Update("users").
    Set("name", "Charlie").
    Set("active", false).
    Where(q.Eq("uuid", "abc-123")).
    Returning("uuid", "name")

sql, args := query.Build()
// sql:  "UPDATE users SET name = $1, active = $2 WHERE uuid = $3 RETURNING uuid, name"
// args: ["Charlie", false, "abc-123"]
```

### DELETE

```go
query := q.DeleteFrom("sessions").
    Where(q.Lt("expires_at", "2024-01-01"))

sql, args := query.Build()
// sql:  "DELETE FROM sessions WHERE expires_at < $1"
// args: ["2024-01-01"]
```

### Executing Queries

```go
type User struct {
    UUID string `db:"uuid"`
    Name string `db:"name"`
}

sqlDB, _ := sql.Open("postgres", connStr)
db := q.NewDB(sqlDB)

// Fetch one row into a struct
user, err := q.FetchOne[User](ctx, db,
    q.Select("uuid", "name").From("users").Where(q.Eq("uuid", id)),
)

// Fetch all matching rows
users, err := q.FetchAll[User](ctx, db,
    q.Select("uuid", "name").From("users").Where(q.Eq("active", true)),
)

// Execute without scanning (INSERT/UPDATE/DELETE)
_, err = db.Exec(ctx, q.DeleteFrom("sessions").Where(q.Lt("expires_at", "now()")))
```

### Debug Output

```go
query := q.Select("*").From("users").Where(q.Eq("name", "Alice")).Limit(5)
fmt.Println(q.ToSQL(query))
// SELECT * FROM users WHERE name = 'Alice' LIMIT 5
```

> **Note:** `ToSQL()` output is for logging only — always use `Build()` for actual execution.

## Building

```sh
make all        # fmt + vet + test + build
make build      # compile to bin/goqdsl
make test       # run all tests
make run        # build and run the demo
make clean      # remove bin/
```

## Documentation

Full API documentation is available in [`docs/html/`](docs/html/index.html).

## License

BSD 3-Clause — see [LICENSE](LICENSE).
