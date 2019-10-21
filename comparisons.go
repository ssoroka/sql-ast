package sqlast

import "fmt"

type Comparison interface {
	String() string
	Compare(a, b *Expression) bool
}

// = | >= | > | <= | < | <> | !=
type ComparisonOperator struct {
	Token Token
	Val   string
}

func (co *ComparisonOperator) String() string {
	switch co.Token {
	case NotEqualSynonim:
		return "<>"
	case Equals:
		return "="
	case GreaterThan:
		return ">"
	case GreaterThanEquals:
		return ">="
	case LessThan:
		return "<"
	case LessThanEquals:
		return "<="
	case Is:
		return "IS"
	case Not:
		return "NOT"
	case IsNot:
		return "IS NOT"
	case IsNotIn:
		return "IS NOT IN"
	case NotEqual:
		return "!="
	case EqualNull:
		return "<=>"
	case Like:
		return " LIKE "
	case NotLike:
		return " NOT LIKE "
	case IsNotLike:
		return " IS NOT LIKE "
	case RLike:
		return " RLIKE "
	case NotRLike:
		return " NOT RLIKE "
	case Between:
		return " BETWEEN "
	case NotBetween:
		return " NOT BETWEEN "
	default:
		panic("unhandled comparison operator token type: " + fmt.Sprintf("%d,%s", co.Token, co.Val))
	}
}

func (co *ComparisonOperator) Compare(a, b *Expression) bool {
	return true
}
