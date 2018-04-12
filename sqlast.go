package sqlast

import "strings"

type Token int

const (
	Illegal Token = iota
	EOF
	Whitespace

	// literals
	Identifier // fields, table name
	Number
	Date // As a string in either 'YYYY-MM-DD' or 'YY-MM-DD' format. A “relaxed” syntax is permitted: Any punctuation character may be used as the delimiter between date parts. For example, '2012-12-31', '2012/12/31', '2012^12^31', and '2012@12@31' are equivalent.
	Time
	Boolean
	QuotedString
	SinglQuotedString

	// Operators
	Equals
	NotEqual
	LessThan
	LessThanEquals
	GreaterThan
	GreaterThanEquals
	Or
	And
	BinaryOr
	BinaryAnd
	Xor
	Is
	Not
	IsNot
	Like
	Regexp
	In
	Multiply
	Divide
	Add
	Subtract
	Modulus // %
	ShiftLeft
	ShiftRight

	// misc characters
	Asterisk
	Comma
	Placeholder // '?', etc.
	ParenOpen
	ParenClose

	// keywords
	Select
	From
	Where
	GroupBy
	Having
	OrderBy
	Limit
	ForUpdate
	Join
	LeftJoin
	LeftOuterJoin
	RightJoin
	RightOuterJoin
	InnerJoin
	On
	Null
	Case
	When
	Then
	Else
	End
	Asc
	Desc
	As

	//aggregate
	Sum
	Avg
	Count
	Max
	Min
	Concat

	// SqlServer specific keyword, for now
	RowNum
	Over
	PartitionBy
)

var literals = []Token{Identifier, Number, Date, Time, Boolean, QuotedString}

// Parse parses statements
func Parse(result *Statement, sql string) error {
	parser := NewParser(strings.NewReader(sql))
	return parser.Parse(result)
}
