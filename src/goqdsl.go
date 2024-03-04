package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
  Conn *pgxpool.Pool
)

type Join struct {
  table string
  left string
  right string
}

type Criteria struct {
  Key string
  Value string
}

type Q struct {
  from string
  fields []string
  joins []Join
  criteria map[string]any
}

func NewQ() *Q {
  return new(Q)
}

func (q *Q) Select(fields ...string) *Q {
  q.fields = fields
  return q
}

func (q *Q) From(t string) *Q {
  q.from = t
  return q
}

func (q *Q) InnerJoin(joins []Join) *Q {
  q.joins = joins
  return q
}

func (q *Q) Where(criteria map[string]string) *Q {
  q.criteria = criteria
  return q
}

// require generics
func FetchOne[T any](q *Query) (T, error) {
  var value T
  rows, _ := Conn.Query(context.Background(), q.Query(), pgx.NamedArgs(q.Criteria)) 
  value, err = pgx.CollectOneRow[T](rows, pgx.RowToStructByName[T any])
  return value
}

func (q *Q) Query() string {
  
  var sql string
  sql = 
  "SELECT " + strings.Join(q.fields, ", ") + " " +
  "FROM " + q.from + " "
 
  idx := 0
  for k, v := range q.criteria {
    if idx == 0 {
       sql += "WHERE " + k + " = " + v + " "
    } else {
       sql += "AND   " + k + " = " + v + " "
    }
    idx++
  }

  return sql
}

func main() {
  NewQ().
  q := NewQ().Select("uuid", "name").From("alerts").Where(map[string]string{"uuid": "d3b2aa81-348d-4727-af3f-81eaa9433962"})
  sql := q.Query()
  fmt.Printf("sql: \n%s\n", sql)
}

// end
