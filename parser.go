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
func (p *Parser) DetectFieldAlias(result *SelectStatement, item Item) bool {
	var nextItem Item
detectAliasLoop:
	for {
		nextItem = p.nextItem()
		if nextItem.Token != Whitespace {
			break detectAliasLoop
		}
	}
	switch nextItem.Token {
	case Comma:
		p.unscan()
		return false
	case Identifier, As: // we found indication of alias
		if nextItem.Token == Identifier {
			newAlias := SelectAlias{item.Val, nextItem.Val}
			result.SelectAl = append(result.SelectAl, newAlias)
		} else {
		identifierLookup:
			for {
				ii := p.nextItem()
				switch ii.Token {
				case Whitespace:
					continue
				case Identifier:
					newAlias := SelectAlias{item.Val, ii.Val}
					result.SelectAl = append(result.SelectAl, newAlias)
					break identifierLookup
				default:
					p.unscan()
					break identifierLookup
				}
			}
		}
		return false
	default:
		p.unscan()
		return false
	}
	return false
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
			//fmt.Println("FoundIdentifier")
			statement.Fields = append(statement.Fields, item.Val)
			p.DetectFieldAlias(statement, item)
		case Multiply: // special case for now.
			statement.Fields = append(statement.Fields, "*")
		case Count, Avg, Min, Max, Sum:
			p.unscan()
			ag := Aggregate{}
			e := p.parseAggregate(&ag)
			if e != nil {
				return e
			}
			statement.Aggregates = append(statement.Aggregates, ag)
			statement.Fields = append(statement.Fields, ag.String())
			pItem := Item{item.Token, ag.String()}
			p.DetectFieldAlias(statement, pItem)
		default:
			return fmt.Errorf("found %v, expected field", item.Inspect())
		}

		// If the next token is not a comma then break the loop.
		if item := p.nextItem(); item.Token != Comma {
			fmt.Println(item)
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

	if item := p.nextItem(); item.Token == Join || item.Token == LeftJoin || item.Token == RightJoin || item.Token == InnerJoin {
		//fmt.Println("Join Found")
		p.unscan()
		ll := 0
	JoinLoop:
		for {
			ll++
			if ll == 30 {
				break
			}
			item := p.nextItem()
			switch item.Token {
			case Where:
				p.unscan()
				break JoinLoop
			case Join, LeftJoin, RightJoin, InnerJoin:
				newJoinStatement := &JoinTables{}
				newJoinStatement.JoinType = item.String()
				e := p.parseJoin(newJoinStatement)
				if e != nil {
					return e
				}
				if newJoinStatement.TableName != "" {
					statement.Joins = append(statement.Joins, *newJoinStatement)
				}
			}
		}
	} else {
		p.unscan()
	}

	var err error
	if err = p.parseConditional(&statement.Where); err != nil {
		return err
	}
nextOption:
	for {
		item := p.nextItem()
		switch item.Token {
		case Whitespace:
			continue
		case GroupBy:
			p.parseGroupBy(&(statement.GroupBy))
		case Having:
			p.parseExpression(&statement.Having)
		case OrderBy:

		case EOF:
			break nextOption
		}
	}

	*result = Statement(statement)
	return nil
}
func (p *Parser) parseOrderBy(result *[]SortField) error {
	var curField SortField
	for {
		item := p.nextItem()
		switch item.Token {
		case Whitespace:
			continue
		case Limit, EOF:
			return nil
		case Identifier:
			if curField.Field != "" {
				return errors.New("Sort order not found after " + curField.Field)
			}
			curField.Field = item.Val
		case Asc, Desc:
			if curField.Sort != "" {
				return errors.New("Sort order duplicateFound, expected comma")
			}
		case Comma:
			*result = append(*result, curField)
			curField = SortField{}
		}
	}
}
func (p *Parser) parseGroupBy(result *[]string) error {
	for {
		v := p.nextItem()
		switch v.Token {
		case Whitespace:
			continue
		case Identifier:
			*result = append(*result, v.Val)
		case OrderBy, Having, EOF:
			p.unscan()
			return nil
		}
	}
}

// parse aggregate AVG,SUM,MAX,MIN,COUNT
func (p *Parser) parseAggregate(result *Aggregate) error {
	// retrieve aggregate function
	aggrFunc := p.nextItem()
	result.AggregateType = aggrFunc.Val
	parentOpenFound := false
	parentOpenNum := 0
	//parentCloseFound := false
AggrLoop:
	for {
		item := p.nextItem()
		switch item.Token {
		case Whitespace:
			continue
		case ParenOpen:
			parentOpenFound = true
			parentOpenNum++
		case ParenClose:
			if !parentOpenFound {
				return errors.New("Closing parenthesis found befor open parenthesis")
			}
			parentOpenNum--
			// parentCloseFound = true
			if parentOpenNum == 0 {
				break AggrLoop
			}
		case Identifier, Multiply:
			if !parentOpenFound {
				return errors.New("Identifier found befor open parenthesis")
			}
			if item.Token == Multiply && aggrFunc.Token != Count {
				return errors.New("Identifier * Can only be used on Count")
			}

			result.FieldName = item.Val
		case Comma:
			p.unscan()
			break AggrLoop
		}

	}
	if parentOpenNum != 0 {
		return errors.New("NO matching bracket")
	}
	// parOpen := p.nextItem()
	// if parOpen.Token != ParenOpen {
	// 	return errors.New(fmt.Sprintf("Expected '(' but found %s instead after %s", parOpen.Val, aggrFunc.Val))
	// }
	// result.AggregateType = aggrFunc.Val
	// fieldVal := p.nextItem()
	// if fieldVal.Token != Identifier && fieldVal.Token != Multiply { // we compare Multiply to allow count(*)
	// 	return errors.New(fmt.Sprintf("Expected Field Name but found %s instead after %s", parOpen.Val, aggrFunc.Val))
	// }
	// if fieldVal.Token != Multiply && aggrFunc.Token != Count {
	// 	return errors.New(fmt.Sprintf("Only Count allowed to use * as parameter"))
	// }
	// result.FieldName = fieldVal.Val
	// parenClose := p.nextItem()
	// fmt.Println("parenClose", parenClose)
	// if parOpen.Token != ParenOpen {
	// 	return errors.New(fmt.Sprintf("Expected ')' but found %s instead after %s", parenClose.Val, fieldVal.Val))
	// }
	return nil
}

// parseJoin detects the "JOIN" clause and processes it, if any.
func (p *Parser) parseJoin(result *JoinTables) error {
	// retrieve table name
	tableName := p.nextItem()
	result.TableName = tableName.Val
	// retrieve on field
	onCond := p.nextItem()
	fmt.Println("TableName", result.TableName, onCond.Val)
	if onCond.Token != On {
		fmt.Errorf("Expected on, but found %s instead", onCond)
		p.unscan()
		return errors.New(fmt.Sprintf("found %v, expected field", onCond.Inspect())) //fmt.Errorf("found %v, expected field", item.Inspect())
	} else {
		p.unscan()
	}
	//fmt.Println("Parsing Expression")
	// ok, we have a where statement.
	adad := &(result.OnCondition)
	e := p.parseExpression(adad)
	//fmt.Println("adad", *adad)
	// if e != nil {
	// 	fmt.Println("error parse join condition", e.Error())
	// }
	return e
}

// parseConditional detects the "where" or "on" clause and processes it, if any.
func (p *Parser) parseConditional(result *Expression) error {
	if item := p.nextItem(); item.Token != Where && item.Token != On {
		fmt.Println("Where or On not found", item)
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
		case GroupBy, Having, OrderBy, Limit, ForUpdate, EOF, Where, Join, LeftJoin, RightJoin, InnerJoin:
			p.unscan()
			done = true
			break
		case On:
			continue
		case Whitespace:

		default:
			fmt.Println("Parser Warning: Unhandled token", item.Inspect())
		}
		if item.Token != Where && item.Token != On && item.Token != Join &&
			item.Token != LeftJoin && item.Token != RightJoin &&
			item.Token != InnerJoin && item.Token != OrderBy && item.Token != GroupBy {
			items = append(items, item)
		}

	}
	// fmt.Println(items)
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
	items = withoutWhitespace(items)
	// fmt.Println("Processing this", items, len(items))
	// fmt.Println(items[0], items[len(items)-1])
	// strip parens if start and ends with parens
	if len(items) >= 3 && items[0].Token == ParenOpen && items[len(items)-1].Token == ParenClose {
		var expression Expression
		//pp := items[1 : len(items)-1]
		// fmt.Println("Removing parenthesis", items[1:len(items)-1], len(pp))
		if err := parseSubExpression(&expression, items[1:len(items)-1]); err != nil {
			return errors.Wrapf(err, "error parsing paren expression: %s", itemsString(items[1:len(items)-1]))
		}

		*result = &ParenExpression{
			Expression: expression,
		}
		if result == nil {
			fmt.Println("ParenExpresion is nil")
		}
		return nil
	}

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
		//fmt.Println("ParentCount", parenCount)
		if items[i-1].Token != ParenClose || parenCount > 0 {
			return errors.New("Opening parenthesis without matching closing parenthesis: " + itemsString(items))
		}
	}

	// detect the type of expression.
	if len(items) == 1 { // handle the simple case where we only have one element.
		switch items[0].Token {
		case Number, QuotedString, Boolean, Identifier, Null:
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
			leftExpression = &DummmyExpression{}
			rightExpression = &DummmyExpression{}
			if err := parseSubExpression(&leftExpression, leftItems); err != nil {
				return errors.Wrap(err, "Error parsing sub expression(1): "+itemsString(items))
			}
			if err := parseSubExpression(&rightExpression, rightItems); err != nil {
				return errors.Wrap(err, "Error parsing sub expression(2): "+itemsString(items))
			}
			// if leftExpression != nil {
			// 	fmt.Println("LeftExpression is ", leftExpression)
			// }
			// if rightExpression != nil {
			// 	fmt.Println("RightExpression is", rightExpression)
			// }
			//fmt.Println("items[idx]", items[idx].Inspect())
			rp := LogicalExpression{
				Left:     leftExpression,
				Operator: LogicalOperator{Token: items[idx].Token, Val: items[idx].Val},
				Right:    rightExpression,
			}
			//fmt.Println("Result", *result)
			*result = &rp
			return nil
		}
	}

	comparisonOperators := []Token{Equals, GreaterThanEquals, GreaterThan, LessThanEquals, LessThan, NotEqual, IsNot, Is, Like, Regexp, In}
	for _, op := range comparisonOperators {
		if idx := tokenIndex(items, op); idx > 0 {
			leftItems := items[0:idx]
			rightItems := items[idx+1 : len(items)]
			if len(leftItems) == 1 && len(rightItems) > 0 {
				var rightExpression Expression
				rightExpression = &DummmyExpression{}
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
