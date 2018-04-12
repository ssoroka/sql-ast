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
	return le.Left.String() + "\n\t" + strings.ToUpper(le.Operator.Val) + " " + le.Right.String()
}

// OR, ||, AND, && (not?)
type LogicalOperator struct {
	Token Token
	Val   string
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
	default:
		panic("can't handle literal expression token type: " + fmt.Sprintf("%d %s", le.Token, le.Val))
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
	Left     IdentifierExpression
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

type FunctionExpression struct{}
type NotExpression struct{}
type SubqueryExpression struct{}
type ExistsExpression struct{}
