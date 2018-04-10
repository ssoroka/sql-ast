package sqlast

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	source := strings.NewReader("select table1.field1,table2.field2 from table1 left join table2 on table1.field3=table2.field4 inner join table3 on table3.fieldA=table1.field1 where table1.field1>=20")
	parser := NewParser(source)
	//var selectStatement ast.Statement
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	pp := selectStatement.(*SelectStatement)
	if len(pp.Fields) != 2 {
		t.Fail()
		t.Error("Failed to parse Field")
		return
	}
	if pp.TableName != "table1" {
		t.Fail()
		t.Error("Failed to parse table name")
		return
	}
	if len(pp.Joins) == 0 {
		t.Fail()
		t.Error("Failed to parse joins")
		return
	}
	t.Log(pp.Joins[0])
	t.Log(pp.Joins[1])
	//t.Log(pp)
}
func TestAggregate(t *testing.T) {
	source := strings.NewReader("select sum(table1.field1),avg(table2.field2) from table1 where table1.field1>=20")
	parser := NewParser(source)
	//var selectStatement ast.Statement
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	pp := selectStatement.(*SelectStatement)
	fmt.Println(pp)
}
func TestGroupByHaving(t *testing.T) {
	source := strings.NewReader("select sum(table1.field1),avg(table2.field2) from table1 where table1.field1>=20 Group by table1.field12,table1.field10 having table1.field2<=10")
	parser := NewParser(source)
	//var selectStatement ast.Statement
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	pp := selectStatement.(*SelectStatement)
	if len(pp.GroupBy) != 2 {
		t.Fail()
		t.Errorf("Failed Parsing Group by")
	}
	if pp.Having.String() != "table1.field2 <= 10" {
		t.Fail()
		t.Errorf("Expecting %s got %s instead", "table1.field2 <= 10", pp.Having.String())
	}
}
func TestAliasField(t *testing.T) {
	source := strings.NewReader("select sum(table1.field1) as v1,avg(table2.field2) v2,table2.field2 b2 from table1 where table1.field1>=20 Group by table1.field12,table1.field10 having table1.field2<=10")
	parser := NewParser(source)
	//var selectStatement ast.Statement
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	pp := selectStatement.(*SelectStatement)
	if len(pp.SelectAl) == 0 {
		t.Fail()
		t.Errorf("Failed to parse alias field ")
	}
	if len(pp.SelectAl) != 3 {
		t.Fail()
		t.Errorf("Failed to parse alias field Content of SelectAl %s content of Aggregates %s", pp.SelectAl, pp.Aggregates)

	}
	t.Log(pp.SelectAl)
}
func TestAliasTable(t *testing.T) {
	source := strings.NewReader("select table1.field1,table2.field2 from table1 t1 left join table2 t2 on table1.field3=table2.field4 inner join table3 as t3 on table3.fieldA=table1.field1 where table1.field1>=20")
	parser := NewParser(source)
	//var selectStatement ast.Statement
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	pp := selectStatement.(*SelectStatement)
	if len(pp.TableAl) != 3 {
		t.Fail()
		t.Errorf("Failed to parse alias table")
	}
	t.Log(pp.TableAl)
}
func TestCaseSelect1(t *testing.T) {
	query := `SELECT
	player_name,
	year,
	"is_a_senior" = CASE WHEN year = "SR" THEN "yes" ELSE NULL END
  FROM
	benn.college_football_players
  `
	source := strings.NewReader(query)
	parser := NewParser(source)
	//var selectStatement ast.Statement
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	pp := selectStatement.(*SelectStatement)
	if len(pp.CaseFields) == 0 {
		t.Fail()
		t.Errorf("Failed to parse CASE condition")
	}
	t.Log(pp.CaseFields)
	if len(pp.Fields) != 3 {
		t.Fail()
		t.Errorf("Failed to correctly detect fields")
	}
	fmt.Println(pp.Fields)
}
func TestCaseSelect2(t *testing.T) {
	query := `SELECT
	player_name,
	year,
	CASE WHEN year = "SR" THEN "yes" ELSE NULL END is_a_senior
  FROM
	benn.college_football_players
  `
	source := strings.NewReader(query)
	parser := NewParser(source)
	//var selectStatement ast.Statement
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	pp := selectStatement.(*SelectStatement)
	if len(pp.CaseFields) == 0 {
		t.Fail()
		t.Errorf("Failed to parse CASE condition")
	}
	t.Log(pp.CaseFields)
	if len(pp.Fields) != 3 {
		t.Fail()
		t.Errorf("Failed to correctly detect fields")
	}
	fmt.Println(pp.CaseFields[0].String())
}
