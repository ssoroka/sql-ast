package sqlast

import (
	"fmt"
	"io"
	"strconv"

	"github.com/pkg/errors"
)

// Parser represents a parser.
type Parser struct {
	s        *Scanner
	itemBuf  []Item
	lastItem Item
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: NewScanner(r)}
}

// scan returns the next token from the underlying scanner.
// If a token has been unscanned then read that instead.
func (p *Parser) scan() Item {
	// If we have a token on the buffer, then return it.
	if len(p.itemBuf) > 0 {
		item := p.itemBuf[0]
		p.itemBuf = p.itemBuf[1:]
		return item
	}

	// Otherwise read the next token from the scanner.
	item := p.s.Scan()

	// Save it to the buffer in case we unscan later.
	p.lastItem = item

	return item
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() {
	p.itemBuf = append([]Item{p.lastItem}, p.itemBuf...)
}

// nextItem scans the next non-whitespace token.
func (p *Parser) nextItem() Item {
	item := p.scan()
	if item.Token == Whitespace {
		item = p.scan()
	}
	return item
}

// Parse parses the tokens provided by a scanner (lexer) into an AST
func (p *Parser) Parse(result *Statement) error {
	statement := &SelectStatement{}
	_ = "breakpoint"
	if item := p.nextItem(); item.Token != Select {
		return fmt.Errorf("found %v, expected SELECT", item.Inspect())
	}

	for {
		// Read a field.
		item := p.nextItem()
		switch item.Token {
		case Identifier, Asterisk, Number:
			statement.Fields = append(statement.Fields, item.Val)
		case Multiply: // special case for now.
			statement.Fields = append(statement.Fields, "*")
		default:
			return fmt.Errorf("found %v, expected field", item.Inspect())
		}

		// If the next token is not a comma then break the loop.
		if item := p.nextItem(); item.Token != Comma {
			p.unscan()
			break
		}
	}

	// Next we should see the "FROM" keyword.
	if item := p.nextItem(); item.Token != From {
		return fmt.Errorf("found %v, expected FROM", item.Inspect())
	}

	item := p.nextItem()
	if item.Token != Identifier {
		return fmt.Errorf("found %v, expected table name", item.Inspect())
	}
	statement.TableName = item.Val

	var err error
	if err = p.parseConditional(&statement.Where); err != nil {
		return err
	}

	*result = Statement(statement)
	return nil
}

// parseConditional detects the "where" clause and processes it, if any.
func (p *Parser) parseConditional(result *Expression) error {
	if item := p.nextItem(); item.Token != Where {
		p.unscan()
		return nil
	}

	// ok, we have a where statement.
	return p.parseExpression(result)
}

func (p *Parser) parseExpression(result *Expression) error {

	items := []Item{}
	done := false
	// depth := 0
	for !done {
		// see if we're done and we hit a border token.
		item := p.scan()
		switch item.Token {
		// case ParenClose
		// case ParenOpen:
		// case Select:
		// case From:
		// case Where:
		case Illegal:
			if len(items) > 0 {
				return errors.New("Error, unexpected token " + item.Inspect() + " after " + items[len(items)-1].Inspect())
			} else {
				return errors.New("Error, unexpected token " + item.Inspect() + " after WHERE")
			}
		case GroupBy, Having, OrderBy, Limit, ForUpdate, EOF:
			p.unscan()
			done = true
			break
		case Whitespace:

		default:
			fmt.Println("Parser Warning: Unhandled token", item.Inspect())
		}
		items = append(items, item)
	}
	fmt.Println(items)

	//todo: write expression
	if len(items) > 0 {
		if err := parseSubExpression(result, items); err != nil {
			return errors.Wrap(err, "Error parsing expression: "+itemsString(items))
		}
	}

	return nil
	// switch item.Token {
	// case LeftParen:
	//   p.State.Push(LeftParen)
	//   p.parseExpression()
	// case Identifier, Number, Date, Time, Boolean, QuotedString:
	// 	return item, nil
	// }

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
	//   | match_expr
	//   | case_expr
	//   | interval_expr
}

// parseSubExpression is called when we know we have an expression.
func parseSubExpression(result *Expression, items []Item) error {
	// strip parens if start and ends with parens
	if len(items) >= 3 && items[0].Token == ParenOpen && items[len(items)-1].Token == ParenClose {
		var expression *Expression
		if err := parseSubExpression(expression, items[1:len(items)-2]); err != nil {
			return errors.Wrapf(err, "error parsing paren expression: %s", itemsString(items[1:len(items)-2]))
		}
		*result = &ParenExpression{
			Expression: *expression,
		}
		return nil
	}

	items = withoutWhitespace(items)
	if len(items) == 0 {
		panic("parsing subexpression with no items")
	}

	if items[0].Token == ParenOpen {
		// find the close of the parenthesis so we can process it together.
		parenCount := 1
		i := 1
		for i < len(items) {
			switch items[i].Token {
			case ParenOpen:
				parenCount++
			case ParenClose:
				parenCount--
				if parenCount == 0 {
					break
				}
			}
			i++
		}
		if items[i-1].Token != ParenClose || parenCount > 0 {
			return errors.New("Opening parenthesis without matching closing parenthesis: " + itemsString(items))
		}
	}

	// detect the type of expression.
	if len(items) == 1 { // handle the simple case where we only have one element.
		switch items[0].Token {
		case Number, QuotedString, Boolean:
			*result = &LiteralExpression{Token: items[0].Token, Val: items[0].Val}
			return nil
		}
	}
	//	if we only have 2 elements, we probably have a NOT or something.

	// otherwise start breaking it up by order of operator precedence.
	logicalOperators := []Token{And, Xor, Or}
	for _, op := range logicalOperators {
		if idx := tokenIndex(items, op); idx > 0 {
			leftItems := items[0:idx]
			rightItems := items[idx+1 : len(items)]

			var leftExpression, rightExpression Expression
			if err := parseSubExpression(&leftExpression, leftItems); err != nil {
				return errors.Wrap(err, "Error parsing sub expression(1): "+itemsString(items))
			}
			if err := parseSubExpression(&rightExpression, rightItems); err != nil {
				return errors.Wrap(err, "Error parsing sub expression(2): "+itemsString(items))
			}
			*result = &LogicalExpression{
				Left:     leftExpression,
				Operator: LogicalOperator{Token: items[idx].Token, Val: items[idx].Val},
				Right:    rightExpression,
			}
			return nil
		}
	}

	comparisonOperators := []Token{Equals, GreaterThanEquals, GreaterThan, LessThanEquals, LessThan, NotEqual, Is, Like, Regexp, In}
	for _, op := range comparisonOperators {
		if idx := tokenIndex(items, op); idx > 0 {
			leftItems := items[0:idx]
			rightItems := items[idx+1 : len(items)]
			if len(leftItems) == 1 && len(rightItems) > 0 {
				var rightExpression Expression
				if err := parseSubExpression(&rightExpression, rightItems); err != nil {
					return errors.Wrap(err, "Error parsing sub expression(3): "+itemsString(items))
				}
				*result = &BooleanExpression{
					Left:     IdentifierExpression{Name: leftItems[0].Val},
					Operator: &ComparisonOperator{Token: items[idx].Token, Val: items[idx].Val},
					Right:    rightExpression,
				}
				return nil
			}
			// var leftExpression, rightExpression Expression
			// if err := parseSubExpression(&leftExpression, leftItems); err != nil {
			// 	return errors.Wrap(err, "Error parsing sub expression: "+itemsString(items))
			// }
			// if err := parseSubExpression(&rightExpression, rightItems); err != nil {
			// 	return errors.Wrap(err, "Error parsing sub expression: "+itemsString(items))
			// }
			// *result = &BooleanExpression{
			// 	Left:     leftExpression,
			// 	Operator: &ComparisonOperator{Token: items[idx].Token, Val: items[idx].Val},
			// 	Right:    rightExpression,
			// }
			panic("left side is too large. too many items to the left of the boolean expression? " + itemsString(leftItems))
			return nil
		}
	}

	return errors.New("Couldn't detect expression type: " + fmt.Sprintf("%s", items))
}

func tokenIndex(items []Item, eq Token) int {
	for i := range items {
		if items[i].Token == eq {
			return i
		}
	}
	return -1
}

func withoutWhitespace(items []Item) []Item {
	result := []Item{}
	for i := range items {
		switch items[i].Token {
		case Whitespace, EOF:
		default:
			result = append(result, items[i])
		}
	}
	return result
}

func itemsString(items []Item) string {
	result := ""
	for i := range items {
		switch items[i].Token {
		case Whitespace, EOF:
			continue
		}
		if i > 0 {
			result = result + " "
		}
		result = result + items[i].Val
	}
	return strconv.QuoteToASCII(result)
}
