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
	output := `SELECT sum(table1.field1) AS v1, avg(table2.field2) AS v2, table2.field2 AS b2
FROM table1
WHERE
	table1.field1 >= 20
GROUP BY
	table1.field12,table1.field10
Having
	table1.field2 <= 10`
	pp := selectStatement.(*SelectStatement)
	if len(pp.ComplexSelects) == 0 {
		t.Fail()
		t.Errorf("Failed to parse alias field ")
	}
	if len(pp.ComplexSelects) != 3 {
		t.Fail()
		t.Errorf("Failed to parse alias field Content of SelectAl %s content of Aggregates %s", pp.SelectAl, pp.Aggregates)

	}
	if pp.String() != output {
		t.Errorf("Output is Different, Expecting %s but got %s instead", output, pp.String())
		t.Fail()
		return
	}
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
	if len(pp.ComplexSelects) != 3 {
		t.Fail()
		t.Errorf("Failed to correctly detect fields")
	}
	fmt.Println(pp.ComplexSelects)
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
	output := `SELECT player_name, year, CASE
	WHEN year = "SR" THEN "yes"
	 ELSE NULL
END
 AS is_a_senior
FROM benn.college_football_players`
	pp := selectStatement.(*SelectStatement)
	if len(pp.CaseFields) == 0 {
		t.Fail()
		t.Errorf("Failed to parse CASE condition")
	}
	t.Log(pp.CaseFields)
	if len(pp.ComplexSelects) != 3 {
		t.Fail()
		t.Errorf("Failed to correctly detect fields")
	}
	if pp.String() != output {
		t.Errorf("Output is Different, Expecting %s but got %s instead", output, pp.String())
		t.Fail()
		return
	}
}
func TestSelectStringSingleQuote(t *testing.T) {
	source := strings.NewReader("select '' AS f1,\"pp\" LU from MM")
	parser := NewParser(source)
	//var selectStatement ast.Statement
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	pp := selectStatement.(*SelectStatement)
	if len(pp.ComplexSelects) != 2 {
		t.Error("Error Parsing field")
		t.Fail()
	}
	output := `SELECT '' AS f1, "pp" AS LU
FROM MM`
	if pp.String() != output {
		t.Errorf("Output is Different, Expecting %s but got %s instead", output, pp.String())
		t.Fail()
		return
	}

}
func TestSelectFromComplex(t *testing.T) {
	source := strings.NewReader("select table1.Field1 from (select ii as Field1,subfield2 as Field2 from mainTable where subfield2!='OO') as table1 ")
	parser := NewParser(source)
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	pp := selectStatement.(*SelectStatement)
	if len(pp.ComplexSelects) != 1 {
		t.Error("Failed to parse select")
		t.Fail()
		return
	}
	if pp.ComplexFrom.SubSelect == nil {
		t.Error("Failed to parse subQuery")
		t.Fail()
		return
	}

	outputSubString := `SELECT ii AS Field1, subfield2 AS Field2
FROM mainTable
WHERE
	subfield2 != 'OO'`
	if outputSubString != pp.ComplexFrom.SubSelect.String() {
		t.Errorf("Output is Different, Expecting %s but got %s instead", outputSubString, pp.ComplexFrom.SubSelect.String())
		t.Fail()
		return
	}
	t.Log(pp.String())
}
func TestSelectJoinComplex(t *testing.T) {
	source := strings.NewReader("select table1.Field1 from table1 right join (select ii as Field1,subfield2 as Field2 from mainTable where subfield2!='OO') as k on table1.field1=k.field4")
	parser := NewParser(source)
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	pp := selectStatement.(*SelectStatement)
	if len(pp.Joins) != 1 {
		t.Fail()
		t.Error("Parsing Join Error")
	}
	t.Log(pp)
}
func TestSelectOrderBy(t *testing.T) {
	source := strings.NewReader("select table1.Field1 from table1 order by field3 asc,field4 desc")
	parser := NewParser(source)
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	pp := selectStatement.(*SelectStatement)
	if len(pp.OrderBy) != 2 {
		t.Error("Failed to parse Order By")
		t.Fail()
	}
	t.Log(pp.OrderBy)
	t.Log(pp)
}
func TestSelectConcat(t *testing.T) {
	source := strings.NewReader("select Concat('PP',\"AFK\") from table1 order by field3 asc,field4 desc")
	parser := NewParser(source)
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	pp := selectStatement.(*SelectStatement)
	if len(pp.ComplexSelects) != 1 {
		t.Error("Failed to parse Select")
		t.Fail()
		return
	}
	t.Log(pp)
	output := `SELECT Concat('PP',"AFK")
FROM table1
Order By
	field3 asc,field4 desc`
	if output != pp.String() {
		t.Errorf("Output is Different, Expecting %s but got %s instead", output, pp.String())
		t.Fail()
		return
	}
}
