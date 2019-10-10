package sqlast

import (
	"fmt"
	"os"
	"strings"
	"testing"

	tk "github.com/eaciit/toolkit"
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
	source := strings.NewReader("select table1.field1, sum(table1.field1) as v1,avg(table2.field2) v2,table2.field2 b2 from table1 where table1.field1>=20 Group by table1.field12,table1.field10 having table1.field2<=10")
	parser := NewParser(source)
	//var selectStatement ast.Statement
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	output := `SELECT table1.field1, sum(table1.field1) AS v1, avg(table2.field2) AS v2, table2.field2 AS b2
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
	if len(pp.ComplexSelects) != 4 {
		t.Fail()
		t.Errorf("Failed to parse alias field Content of SelectAl %s content of Aggregates %s", pp.SelectAl, pp.Aggregates)

	}
	//t.Log("AAA1", tk.JsonString(pp.ComplexSelects[0]))
	//t.Log("AAA2", tk.JsonString(pp.ComplexSelects[1]))
	//t.Log("AAA3", tk.JsonString(pp.ComplexSelects[2]))
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
	t.Log("AA", tk.JsonString(pp))
	t.Log(pp.TableAl)
}
func TestCaseSelect1(t *testing.T) {
	query := `SELECT
	player_name,
	years,
	"is_a_senior" = CASE WHEN years = "SR" THEN "yes" ELSE NULL END
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
	t.Log(tk.JsonString(pp.CaseFields))
	t.Log(tk.JsonString(pp.ComplexSelects))
	if len(pp.ComplexSelects) != 3 {
		t.Fail()
		t.Errorf("Failed to correctly detect fields")
	}
	fmt.Println(pp.ComplexSelects)
}
func TestOverParX(t *testing.T) {
	query := `SELECT a, SUM(b) OVER (PARTITION BY c, d ORDER BY e, f) as BB
	FROM T`
	source := strings.NewReader(query)
	parser := NewParser(source)
	//var selectStatement ast.Statement
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	output := `SELECT a, SUM(b) OVER (PARTITION BY c,d ORDER BY e,f) AS BB
FROM T`
	pp := selectStatement.(*SelectStatement)
	if pp.String() != output {
		t.Errorf("Output is Different, Expecting %s but got %s instead", output, pp.String())
		t.Fail()
		return
	}
}
func TestOverPartition(t *testing.T) {
	query := `SELECT a, SUM(b) OVER (PARTITION BY c, d ORDER BY e, f)
	FROM T
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
	output := `SELECT a, SUM(b) OVER (PARTITION BY c,d ORDER BY e,f)
FROM T`
	pp := selectStatement.(*SelectStatement)
	if pp.String() != output {
		t.Errorf("Output is Different, Expecting %s but got %s instead", output, pp.String())
		t.Fail()
		return
	}
}
func TestCaseSelect2(t *testing.T) {
	query := `SELECT
	player_name,
	years,
	CASE WHEN years = "SR" AND years<50 THEN "yes" ELSE NULL END is_a_senior
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
	output := `SELECT player_name, years, CASE
	WHEN years = "SR"
	AND years < 50 THEN "yes"
	 ELSE NULL
END
 AS is_a_senior
FROM benn.college_football_players`
	pp := selectStatement.(*SelectStatement)
	if len(pp.CaseFields) == 0 {
		t.Fail()
		t.Errorf("Failed to parse CASE condition")
	}
	t.Log(tk.JsonString(pp.ComplexSelects[2].CaseStatement))
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
func TestParseExpression(t *testing.T) {
	source := strings.NewReader(`((fieldX = "1"	AND OT.fieldF > 9) OR ODS='@ODS')`)
	parser3 := NewParser(source)
	var exprWithODS Expression
	err2 := parser3.ParseExpression(&exprWithODS)
	if err2 != nil {
		t.Fail()
		t.Log(err2.Error())
	}
	t.Log(exprWithODS.String())
	outputExpected := `((fieldX = "1"
	AND OT.fieldF > 9)
	OR ODS = '@ODS')`
	if outputExpected != exprWithODS.String() {
		t.Fail()
		return
	}
}
func TestParseExpression2(t *testing.T) {
	// t.Skip()
	source := strings.NewReader(`((fieldX = "1"	AND OT.fieldF > 9) OR (ODS='@ODS'))`)
	parser3 := NewParser(source)
	var exprWithODS Expression
	err2 := parser3.ParseExpression(&exprWithODS)
	if err2 != nil {
		t.Fail()
		t.Log(err2.Error())
	}
	t.Log(exprWithODS.String())
	outputExpected := `((fieldX = "1"
	AND OT.fieldF > 9)
	OR (ODS = '@ODS'))`
	if outputExpected != exprWithODS.String() {
		t.Fail()
		return
	}
}
func TestOuterJoin(t *testing.T) {
	//t.Skip()
	source := strings.NewReader(`SELECT CURRENCYCODE,COUNTRYCODE,CITYNAME FROM EBBSPRD_BN_SYSTEM
	LEFT OUTER JOIN EBBSPRD_LK_RELADDR ON ((EBBSPRD_LK_MLITDEL.DEALNO = '1') AND ('2' = '2'))
	RIGHT OUTER JOIN EBBSPRD_BN_SYSTEM ON (EBBSPRD_LK_MLITDEL.DEALNO = EBBSPRD_LK_MLITDEL.EXPIRYDATE) WHERE (EBBSPRD_LK_MLITDEL.ODACCOUNTNO = EBBSPRD_LK_MLITDEL.ODACCOUNTNO)`)
	parser := NewParser(source)
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		fmt.Println(err.Error())
		t.Fail()
		return
	}
	pp := selectStatement.(*SelectStatement)
	expected := `SELECT CURRENCYCODE, COUNTRYCODE, CITYNAME
FROM EBBSPRD_BN_SYSTEM
	LEFT OUTER JOIN EBBSPRD_LK_RELADDR ON ((EBBSPRD_LK_MLITDEL.DEALNO = '1')
	AND (2 = '2'))
	RIGHT OUTER JOIN EBBSPRD_BN_SYSTEM ON (EBBSPRD_LK_MLITDEL.DEALNO = EBBSPRD_LK_MLITDEL.EXPIRYDATE)
WHERE
	(EBBSPRD_LK_MLITDEL.ODACCOUNTNO = EBBSPRD_LK_MLITDEL.ODACCOUNTNO)`
	if pp.String() != expected {
		t.Error("Not Same, Found ", pp.String())
		t.Fail()
		return
	}
}
func TestFullOuterJoin(t *testing.T) {
	//t.Skip()
	source := strings.NewReader(`SELECT CURRENCYCODE,COUNTRYCODE,CITYNAME FROM EBBSPRD_BN_SYSTEM
	FULL OUTER JOIN EBBSPRD_LK_RELADDR ON ((EBBSPRD_LK_MLITDEL.DEALNO = '1') AND ('2' = '2'))
	FULL INNER JOIN EBBSPRD_BN_SYSTEM ON (EBBSPRD_LK_MLITDEL.DEALNO = EBBSPRD_LK_MLITDEL.EXPIRYDATE) WHERE (EBBSPRD_LK_MLITDEL.ODACCOUNTNO = EBBSPRD_LK_MLITDEL.ODACCOUNTNO)`)
	parser := NewParser(source)
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		fmt.Println(err.Error())
		t.Fail()
		return
	}
	pp := selectStatement.(*SelectStatement)
	expected := `SELECT CURRENCYCODE, COUNTRYCODE, CITYNAME
FROM EBBSPRD_BN_SYSTEM
	FULL OUTER JOIN EBBSPRD_LK_RELADDR ON ((EBBSPRD_LK_MLITDEL.DEALNO = '1')
	AND (2 = '2'))
	FULL INNER JOIN EBBSPRD_BN_SYSTEM ON (EBBSPRD_LK_MLITDEL.DEALNO = EBBSPRD_LK_MLITDEL.EXPIRYDATE)
WHERE
	(EBBSPRD_LK_MLITDEL.ODACCOUNTNO = EBBSPRD_LK_MLITDEL.ODACCOUNTNO)`
	if pp.String() != expected {
		t.Error("Not Same, Found ", pp.String())
		t.Fail()
		return
	}
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
	t.Log("concat", tk.JsonString(pp.ComplexSelects))
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
func TestQuoteSS(t *testing.T) {
	source := strings.NewReader(`TARGET_ATTR IS NOT IN ('-1')`)
	parser := NewParser(source)
	//selectStatement := Statement(&SelectStatement{})
	var JJ Expression
	err := parser.ParseExpression(&JJ)
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	expectedOutput := `TARGET_ATTR IS NOT IN ('-1')`
	if JJ.String() != expectedOutput {
		t.Fail()
	}
	//t.Log(JJ.String())
}
func TestIsNull(t *testing.T) {
	source := strings.NewReader(`select * from patient where (x IS NULL OR y IS NULL) AND g>10`)
	parser := NewParser(source)
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	expectedOutput := `SELECT *
FROM patient
WHERE
	(x IS NULL
	OR y IS NULL)
	AND g > 10`
	pp := selectStatement.(*SelectStatement)
	if pp.String() != expectedOutput {
		t.Error("Unexpected output, got", pp.String(), "expected", expectedOutput)
		t.Fail()

	}
}
func TestSomething2(t *testing.T) {
	source := strings.NewReader(`SELECT
	CMT_ALL_BCA_LARGEEXPOSURE_DETAILS.*
	FROM
	CMT_ALL_BCA_LARGEEXPOSURE_DETAILS
	LEFT OUTER JOIN PeTe ON PeTe.PeKa = CMT_ALL_BCA_LARGEEXPOSURE_DETAILS.ATTR_A
	AND PeTe.PROCESS_DATE = '@Process_Date'
	AND CMT_ALL_BCA_LARGEEXPOSURE_DETAILS.PROCESS_DATE = '@Process_Date'
	LEFT OUTER JOIN PeTeX ON PeTeX.PeKaX = CMT_ALL_BCA_LARGEEXPOSURE_DETAILS.ATTR_C
	AND PeTeX.PROCESS_DATE = '@Process_Date'
	AND CMT_ALL_BCA_LARGEEXPOSURE_DETAILS.PROCESS_DATE = '@Process_Date'
	WHERE
	PeTe.PeKa IS NULL
	AND PeTeX.PeKaX IS NULL`)
	parser := NewParser(source)
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	t.Log(selectStatement.String())
}
func TestMultiOver(t *testing.T) {
	source := strings.NewReader(`select pat_id, 
	dept_id, 
	ins_amt, 
	row_number() over (order by ins_amt) as rn, 
	rank() over (order by ins_amt ) as rk, 
	dense_rank() over (order by ins_amt ) as dense_rk 
	from patient`)
	parser := NewParser(source)
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	expectedOutput := `SELECT pat_id, dept_id, ins_amt, row_number() OVER (ORDER BY ins_amt) AS rn, rank() OVER (ORDER BY ins_amt) AS rk, dense_rank() OVER (ORDER BY ins_amt) AS dense_rk
FROM patient`
	pp := selectStatement.(*SelectStatement)
	if pp.String() != expectedOutput {
		t.Error("Unexpected output, got", pp.String(), "expected", expectedOutput)
		t.Fail()

	}
}
func TestCountDistinct(t *testing.T) {
	source := strings.NewReader(`SELECT COUNT (DISTINCT columns) FROM table`)
	parser := NewParser(source)
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	pp := selectStatement.(*SelectStatement)
	expectedOutput := `SELECT COUNT(DISTINCT columns)
FROM table`
	if pp.String() != expectedOutput {
		t.Error("Unexpected output, got", pp.String(), "expected", expectedOutput)
		t.Fail()

	}
}
func TestPartialCase(t *testing.T) {
	source := strings.NewReader(`CASE ProductLine
	WHEN 'R' THEN 'Road'
	WHEN 'M' THEN 'Mountain'
	WHEN 'T' THEN 'Touring'
	WHEN 'S' THEN 'Other sale items'
	ELSE 'Not for sale'
 END as Category`)
	parser := NewParser(source)
	selectStatement := SelectStatement{}
	err := parser.ParseCase(&selectStatement, "")
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	t.Log(selectStatement.ComplexSelects[0].String())
}
func TestParseAggr(t *testing.T) {
	source := strings.NewReader("regex_replace(\"(AS)\",\"SS\",\"II\")")
	p := NewParser(source)
	ag := Aggregate{}
	e := p.ParseAggregate(&ag)
	if e != nil {
		t.Fail()
	}
	t.Log(ag.String())
}
func TestNewAggregate1(t *testing.T) {
	source := strings.NewReader("select explode(riskcode), cast(masterno as int), LPAD('LL','oo',3), split(aa,'b'),Substr(aa,bb,4)," +
		"Lpad(a,b,c),regex_replace(\"(AS)\",\"SS\",\"II\"),Substring(aa,bb,4),UPPER('SS') from PREFIX2_RSKIND RSKND")
	parser := NewParser(source)
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	pp := selectStatement.(*SelectStatement)

	//t.Log(pp)
	expectedOutput := `SELECT explode(riskcode), cast(masterno As int), LPAD('LL','oo',3), split(aa,'b'), Substr(aa,bb,4), Lpad(a,b,c), regex_replace("(AS)","SS","II"), Substring(aa,bb,4), UPPER('SS')
FROM PREFIX2_RSKIND AS RSKND`
	if pp.String() != expectedOutput {
		t.Error("Unexpected output, got", pp.String(), "expected", expectedOutput)
		t.Fail()
	}
}
func TestRowNumOver(t *testing.T) {
	source := strings.NewReader("select riskcode, masterno , row_number() over (partition by masterno order by seqno desc) as max_row from PREFIX2_RSKIND RSKND")
	parser := NewParser(source)
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	pp := selectStatement.(*SelectStatement)
	if len(pp.ComplexSelects) != 3 {
		t.Fail()
		fmt.Println(err.Error())
		return
	}
	//t.Log(pp)
	expectedOutput := `SELECT riskcode, masterno, row_number() OVER (PARTITION BY masterno ORDER BY seqno) AS max_row
FROM PREFIX2_RSKIND AS RSKND`
	if pp.String() != expectedOutput {
		t.Error("Unexpected output, got", pp.String(), "expected", expectedOutput)
		t.Fail()

	}
}
