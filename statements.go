package sqlast

import (
	"bytes"
	"fmt"
	"strings"
)

type Statement interface {
	String() string
}

func NewSelectStatement() *SelectStatement {
	return &SelectStatement{}
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
	Distinct       bool
	Fields         []string
	SelectAl       []SelectAlias
	ComplexSelects []ComplexSelect
	TableName      string
	ComplexFrom    ComplexTable
	Where          Expression
	Joins          []JoinTables
	TableAl        []TableAlias
	Aggregates     []Aggregate
	GroupBy        []string
	Having         Expression
	OrderBy        []SortField
	CaseFields     []CaseField
	// GroupBy
	// Having Expression
	// OrderBy
	// Limit
	ForUpdate bool
}
type ComplexSelect struct {
	Alias          string
	FieldName      string
	AggregateField *Aggregate
	CaseStatement  *CaseField
	StaticValue    string
}

func (c *ComplexSelect) String() string {
	buff := bytes.Buffer{}
	if c.FieldName != "" {
		buff.WriteString(c.FieldName)
	} else if c.AggregateField != nil {
		buff.WriteString(c.AggregateField.String())
	} else if c.CaseStatement != nil {
		buff.WriteString(c.CaseStatement.String())
	} else if c.StaticValue != "" {
		buff.WriteString(c.StaticValue)
	}
	if c.Alias != "" {
		buff.WriteString(" AS " + c.Alias)
	}
	return buff.String()
}

type ComplexTable struct {
	Alias     string
	SubSelect *SelectStatement
	TableName string
}

func (t *ComplexTable) String() string {
	buff := bytes.Buffer{}
	if t.TableName != "" {
		buff.WriteString(t.TableName)
	} else {
		buff.WriteString(fmt.Sprintf("(%s)", t.SubSelect.String()))
	}
	if t.Alias != "" {
		buff.WriteString(" AS " + t.Alias)
	}
	return buff.String()
}

type CaseField struct {
	Alias           string
	FieldIdentifier string
	WhenCond        []WhenCond
	ElseCond        string
}

func (c *CaseField) String() string {
	buffer := bytes.NewBuffer([]byte{})
	buffer.WriteString("CASE\n")
	if c.FieldIdentifier != "" {
		buffer.WriteString(c.FieldIdentifier + "\n")
	}
	for _, w := range c.WhenCond {
		buffer.WriteString("\t" + w.String() + "\n")
	}
	if c.ElseCond != "" {
		buffer.WriteString("\t ELSE " + c.ElseCond + "\n")
	}
	buffer.WriteString("END")
	if c.Alias != "" {
		buffer.WriteString(" AS " + c.Alias)
	}
	buffer.WriteString("\n")
	return buffer.String()
}

type WhenCond struct {
	WhenCond Expression
	ThenCond string
}

func (w *WhenCond) String() string {
	return "WHEN " + w.WhenCond.String() + " THEN " + w.ThenCond
}

type Aggregate struct {
	AggregateType string
	FieldName     string
	Params        []Item
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
	buff := bytes.Buffer{}
	buff.WriteString(a.AggregateType)
	for _, par := range a.Params {
		Lit := LiteralExpression{par.Token, par.Val}
		buff.WriteString(Lit.String())
	}

	return buff.String() //fmt.Sprintf("%s(%s)", a.AggregateType, a.FieldName)
}

type JoinTables struct {
	JoinType    string
	TableName   string
	SubSelect   *SelectStatement
	Alias       string
	OnCondition Expression
}

func (j *JoinTables) String() string {
	buff := bytes.Buffer{}
	buff.WriteString(j.JoinType)
	if j.TableName != "" {
		buff.WriteString(" " + j.TableName)
	} else {
		buff.WriteString(" (" + j.SubSelect.String() + ")")
	}
	if j.Alias != "" {
		buff.WriteString(" AS " + j.Alias)
	}
	if j.JoinType != "," {
		buff.WriteString(" ON " + j.OnCondition.String())
	}

	return buff.String()
}
func (s *SelectStatement) String() string {
	out := &bytes.Buffer{}
	selFields := []string{}
	for _, cc := range s.ComplexSelects {
		selFields = append(selFields, cc.String())
	}
	out.WriteString("SELECT " + strings.Join(selFields, ", "))
	if s.ComplexFrom.TableName != "" || s.ComplexFrom.SubSelect != nil {
		out.WriteString("\n")
		out.WriteString("FROM " + s.ComplexFrom.String()) //s.TableName)
		if len(s.Joins) >= 0 {
			for _, j := range s.Joins {
				out.WriteString("\n\t" + j.String())
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
	if len(s.OrderBy) > 0 {
		oo := []string{}
		for _, i := range s.OrderBy {
			oo = append(oo, i.Field+" "+i.Sort)
		}
		out.WriteString("\nOrder By\n\t")
		out.WriteString(strings.Join(oo, ","))
	}
	return out.String()
}
