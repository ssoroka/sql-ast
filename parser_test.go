package sqlast

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	source := strings.NewReader("select table1.field1,table2.field2 from table1 left join table2 on table1.field3=table2.field4 where table1.field1>20")
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
}
