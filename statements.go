package sqlast

import (
	"bytes"
	"strings"
)

type Statement interface {
	String() string
}

// SELECT
//     [ALL | DISTINCT | DISTINCTROW ]
//       [SQL_CACHE | SQL_NO_CACHE] [SQL_CALC_FOUND_ROWS]
//     select_expr [, select_expr ...]
//     [FROM table_references
//       [PARTITION partition_list]
//     [WHERE where_condition]
//     [GROUP BY {col_name | expr | position}
//       [ASC | DESC], ... [WITH ROLLUP]]
//     [HAVING where_condition]
//     [ORDER BY {col_name | expr | position}
//       [ASC | DESC], ...]
//     [LIMIT {[offset,] row_count | row_count OFFSET offset}]
//     [FOR UPDATE | LOCK IN SHARE MODE]]
type SelectStatement struct {
	Distinct  bool
	Fields    []string
	TableName string
	Where     Expression
	// GroupBy
	// Having Expression
	// OrderBy
	// Limit
	ForUpdate bool
}

func (s *SelectStatement) String() string {
	out := &bytes.Buffer{}
	out.WriteString("SELECT " + strings.Join(s.Fields, ", "))
	if s.TableName != "" {
		out.WriteString("\n")
		out.WriteString("FROM " + s.TableName)
	}
	if s.Where != nil {
		out.WriteString("\nWHERE\n\t")
		out.WriteString(s.Where.String())
	}
	return out.String()
}
