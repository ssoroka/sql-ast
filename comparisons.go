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
	case Equals:
		return "="
	default:
		panic("unhandled comparison operator token type: " + fmt.Sprintf("%d", co.Token))
	}
}

func (co *ComparisonOperator) Compare(a, b *Expression) bool {
	return true
}
