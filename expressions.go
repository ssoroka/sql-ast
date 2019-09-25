package sqlast

import (
	"fmt"
	"strconv"
	"strings"
)

// expr:
//     expr OR expr
//   | expr || expr
//   | expr XOR expr
//   | expr AND expr
//   | expr && expr
// //   | NOT expr
// //   | ! expr
// //   | boolean_primary IS [NOT] {TRUE | FALSE | UNKNOWN}
// //   | boolean_primary
type LogicalExpression struct {
	Left     Expression
	Operator LogicalOperator
	Right    Expression
}

func (le *LogicalExpression) String() string {
	leftStr := le.Left.String()
	rightStr := le.Right.String()
	// switch le.Left.(type) {
	// case *LogicalExpression:
	// 	leftStr = "(" + leftStr + ")"
	// }
	// switch le.Right.(type) {
	// case *LogicalExpression:
	// 	rightStr = "(" + rightStr + ")"
	// }
	return leftStr + "\n\t" + strings.ToUpper(le.Operator.Val) + " " + rightStr
}

// OR, ||, AND, && (not?)
type LogicalOperator struct {
	Token Token
	Val   string
}

// this
type ANDLogicalExpression struct {
	Expressions []Expression
}

func (a *ANDLogicalExpression) String() string {
	output := []string{}
	for _, val := range a.Expressions {
		output = append(output, val.String())
	}
	return strings.Join(output, " AND ")

}

// bit_expr:
//     bit_expr | bit_expr
//   | bit_expr & bit_expr
//   | bit_expr << bit_expr
//   | bit_expr >> bit_expr
//   | bit_expr + bit_expr
//   | bit_expr - bit_expr
//   | bit_expr * bit_expr
//   | bit_expr / bit_expr
//   | bit_expr DIV bit_expr
//   | bit_expr MOD bit_expr
//   | bit_expr % bit_expr
//   | bit_expr ^ bit_expr
//   | bit_expr + interval_expr
//   | bit_expr - interval_expr
//   | simple_expr

type BitwiseExpression struct {
	Left     Expression
	Operator BitwiseOperator
	Right    Expression
}

// |, &
type BitwiseOperator struct {
	Token Token
	Val   string
}

// boolean_primary:
//     boolean_primary IS [NOT] NULL
//   | boolean_primary <=> predicate
//   | boolean_primary comparison_operator predicate
//   | boolean_primary comparison_operator {ALL | ANY} (subquery)
//   | predicate
//
// comparison_operator: = | >= | > | <= | < | <> | !=
//
// predicate:
//     bit_expr [NOT] IN (subquery)
//   | bit_expr [NOT] IN (expr [, expr] ...)
//   | bit_expr [NOT] BETWEEN bit_expr AND predicate
//   | bit_expr SOUNDS LIKE bit_expr
//   | bit_expr [NOT] LIKE simple_expr [ESCAPE simple_expr]
//   | bit_expr [NOT] REGEXP bit_expr
//   | bit_expr
//
//
// simple_expr:
//     literal
//   | identifier
//   | function_call
//   | simple_expr COLLATE collation_name
//   | param_marker
//   | variable
//   | simple_expr || simple_expr
//   | + simple_expr
//   | - simple_expr
//   | ~ simple_expr
//   | ! simple_expr
//   | BINARY simple_expr
//   | (expr [, expr] ...)
//   | ROW (expr, expr [, expr] ...)
//   | (subquery)
//   | EXISTS (subquery)
//   | {identifier expr}
//   | case_expr
//   | match_expr
//   | interval_expr

type Expression interface {
	// Items() []Item
	String() string //
}

type DummmyExpression struct {
}

func (d *DummmyExpression) String() string {
	return "DUMMY"
}

// // ParseExpression finds the relevant expression type and creates it and assigns it to the receiver
// func ParseExpression(receiver interface{}, text string) {

// }

type LiteralExpression struct {
	Token Token
	Val   string
}

func (le *LiteralExpression) String() string {
	switch le.Token {
	case Number:
		return le.Val
	case QuotedString:
		return strconv.QuoteToASCII(le.Val)
	case SinglQuotedString:
		return fmt.Sprintf("'%s'", le.Val)
	case Comma:
		return ","
	case ParenOpen:
		return "("
	case ParenClose:
		return ")"
	case Boolean:
		return strings.ToUpper(le.Val)
	case Null:
		return "NULL"
	case Identifier:
		return le.Val
	case Over:
		return "Over "
	case OrderBy:
		return "Order By "
	case PartitionBy:
		return "Partition By "
	case Asc:
		return "Asc "
	case Desc:
		return "Desc "
	case Unix_timestamp:
		return "Unix_timestamp"
	case From_unixtime:
		return "From_unixtime"
	case As:
		return "As"
	case Nvl:
		return "NVL"
	case Concat:
		return "Concat"
	case Trim:
		return "Trim"
	case Cast:
		return "Cast"
	case Explode:
		return "Explode"
	case Lpad:
		return "Lpad"
	case Asterisk:
		return "*"
	default:
		//panic("can't handle literal expression token type: " + fmt.Sprintf("%d %s", le.Token, le.Val))
		return le.Val
	}
}

// ParenExpression is a '(' + expression + ')'
type ParenExpression struct {
	Expression Expression
}

func (pe *ParenExpression) String() string {
	return "(" + pe.Expression.String() + ")"
}

type BooleanExpression struct {
	Left     Expression
	Operator Comparison
	Right    Expression
}

func (be *BooleanExpression) String() string {
	if be.Right == nil {
		return be.Left.String() + " " + be.Operator.String() + " nil!!!"
	}
	return be.Left.String() + " " + be.Operator.String() + " " + be.Right.String()
}

type IdentifierExpression struct {
	Name  string
	Table string
	Alias string
}

func (ie *IdentifierExpression) String() string {
	if ie == nil {
		return "nil!"
	}
	return ie.Name
}

type FunctionExpression struct {
	FunctionName string
	Parameters   []Item
}

func (f *FunctionExpression) String() string {
	outString := f.FunctionName + "("
	for _, p := range f.Parameters {
		outString += p.Val
	}
	outString += ")"
	return outString
}

type NotExpression struct{}
type SubqueryExpression struct{}
type ExistsExpression struct{}
