package sqlast

import (
	"testing"
)

func TestSimpleSelect(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `select *
    from  some_table`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if ast == nil {
		t.Error("Expected AST to be set but it was empty")
		t.FailNow()
	}

	if ast.String() != "SELECT *\nFROM some_table" {
		t.Error("Unexpected output", ast.String())
	}
}

func TestSimpleSelectFields(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `select
    one, two,  three,four
    FROM
    some_other_table
  `)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if ast.String() != "SELECT one, two, three, four\nFROM some_other_table" {
		t.Error("Unexpected output", ast.String())
	}
}

func TestSimpleSelectWhere(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `select 1 FROM some_other_table
  where a = 1
`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	expectedOutput := `SELECT 1
FROM some_other_table
WHERE
	a = 1`
	if ast.String() != expectedOutput {
		t.Error("Unexpected output, got", ast.String(), "expected", expectedOutput)
	}
}

func TestSimpleSelectWhereQuotedStringEquality(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `select 1 FROM some_other_table where a = "giraffe"`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	expectedOutput := `SELECT 1
FROM some_other_table
WHERE
	a = "giraffe"`
	if ast.String() != expectedOutput {
		t.Error("Unexpected output, got", ast.String(), "expected", expectedOutput)
	}
}

func TestSimpleSelectWhereQuotedStringAndBooleanEquality(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `select 1 FROM some_other_table where a = "giraffe" and b = true`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	expectedOutput := `SELECT 1
FROM some_other_table
WHERE
	a = "giraffe"
	AND b = TRUE`
	if ast.String() != expectedOutput {
		t.Error("Unexpected output, got", ast.String(), "expected", expectedOutput)
	}
}

func TestSimpleSelectWhereMultipleAndWithSubExpression(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `select 1 FROM some_other_table where a = "giraffe" and b = true AND (q is not null or q >= 3)`)
	// and last_known_date > "2015-05-02"
	// and created_at <= NOW()
	// `SELECT 1 FROM some_other_table WHERE a = 1 AND b = e.something AND (q IS NOT NULL OR q >= 3) AND last_known_date > "2015-05-02" AND created_at <= NOW()`
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	expectedOutput := `SELECT 1
FROM some_other_table
WHERE
	a = "giraffe"
	AND b = TRUE
	AND (
		q IS NOT NULL
		OR q >= 3
	)`
	if ast.String() != expectedOutput {
		t.Error("Unexpected output, got", ast.String(), "expected", expectedOutput)
	}
}

// func TestScannerCanReadStuff(t *testing.T) {
// 	str := `1234567890 FRoM water SElect`
// 	scanner := NewScanner(bufio.NewReader(strings.NewReader(str)))

// 	item := scanner.Scan()
// 	if item.Token != Number {
// 		t.Error("Expected scanner to read number, got", item.Inspect())
// 	}
// 	if item.Val != "1234567890" {
// 		t.Error("Expected scanner to read number, got", item.Inspect())
// 	}

// 	if scanner.Scan().Token != Whitespace {
// 		t.Error("Expected whitespace")
// 	}

// 	item = scanner.Scan()
// 	if item.Token != From {
// 		t.Error("Expected scanner to read FROM keyword, got", item.Inspect())
// 	}
// 	if item.Val != "FRoM" {
// 		t.Error("Expected scanner to read FROM keyword, got", item.Inspect())
// 	}

// 	if scanner.Scan().Token != Whitespace {
// 		t.Error("Expected whitespace")
// 	}

// 	item = scanner.Scan()
// 	if item.Token != Identifier {
// 		t.Error("Expected scanner to read \"water\" identifier, got", item.Inspect())
// 	}
// 	if item.Val != "water" {
// 		t.Error("Expected scanner to read \"water\" identifier, got", item.Inspect())
// 	}

// 	if scanner.Scan().Token != Whitespace {
// 		t.Error("Expected whitespace")
// 	}

// 	item = scanner.Scan()
// 	if item.Token != Select {
// 		t.Error("Expected scanner to read SELECT keyword, got", item.Inspect())
// 	}
// 	if item.Val != "SElect" {
// 		t.Error("Expected scanner to read SELECT keyword, got", item.Inspect())
// 	}

// }

// func TestPeek(t *testing.T) {
// 	str := `1234567890 FRoM water SElect`
// 	s := NewScanner(bufio.NewReader(strings.NewReader(str)))
// 	log.Println("peek 1")
// 	if s.peek() != '1' {
// 		t.Error("1. peek failed")
// 	}
// 	log.Println("peek 1")
// 	if s.peek() != '1' {
// 		t.Error("2. peek failed")
// 	}
// 	log.Println("read 1")
// 	if s.read() != '1' {
// 		t.Error("3. read failed")
// 	}
// 	log.Println("peek 2")
// 	if r := s.peek(); r != '2' {
// 		t.Error("4. peek failed, expected 2, got", string(r))
// 	}
// 	log.Println("peek 2")
// 	if r := s.peek(); r != '2' {
// 		t.Error("5. peek failed, expected 2, got", string(r))
// 	}
// 	log.Println("read 2")
// 	if r := s.read(); r != '2' {
// 		t.Error("6. read failed, expected 2, got", string(r))
// 	}
// }
