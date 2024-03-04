package goqdsl

import (
	"fmt"
	"testing"
)

func TestQueryBuild(t *testing.T) {
  q := NewQ().Select("uuid", "name").From("alerts").Where(map[string]string{"uuid": "d3b2aa81-348d-4727-af3f-81eaa9433962"})
  sql := q.Query()

  fmt.Printf("sql: \n%s", sql)
}
