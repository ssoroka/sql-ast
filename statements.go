package sqlast

import (
	"bytes"
	"fmt"
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
	Distinct   bool
	Fields     []string
	TableName  string
	Where      Expression
	Joins      []JoinTables
	Aggregates []Aggregate
	GroupBy    []string
	Having     Expression
	OrderBy    []SortField
	// GroupBy
	// Having Expression
	// OrderBy
	// Limit
	ForUpdate bool
}
type Aggregate struct {
	AggregateType string
	FieldName     string
}
type SortField struct {
	Field string
	Sort  string
}
type SelectAlias struct {
	Field string
	Alias string
}
type TableAlias struct {
	Table string
	Alias string
}

func (a *Aggregate) String() string {
	return fmt.Sprintf("%s(%s)", a.AggregateType, a.FieldName)
}

type JoinTables struct {
	JoinType    string
	TableName   string
	OnCondition Expression
}

func (s *SelectStatement) String() string {
	out := &bytes.Buffer{}
	out.WriteString("SELECT " + strings.Join(s.Fields, ", "))
	if s.TableName != "" {
		out.WriteString("\n")
		out.WriteString("FROM " + s.TableName)
		if len(s.Joins) >= 0 {
			for _, j := range s.Joins {
				out.WriteString("\n\t" + j.JoinType + " " + j.TableName + " ON " + j.OnCondition.String())
			}

		}
	}
	if s.Where != nil {
		out.WriteString("\nWHERE\n\t")
		out.WriteString(s.Where.String())
	}
	if s.GroupBy != nil {
		out.WriteString("\nGROUP BY\n\t")
		out.WriteString(strings.Join(s.GroupBy, ","))
	}
	if s.Having != nil {
		out.WriteString("\nHaving\n\t")
		out.WriteString(s.Having.String())
	}
	return out.String()
}
