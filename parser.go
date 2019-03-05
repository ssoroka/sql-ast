package sqlast

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Parser represents a parser.
type Parser struct {
	s        *Scanner
	itemBuf  []Item
	lastItem Item
}

func init() {
	log.SetOutput(os.Stdout)
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
	var FoundAlias bool
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
		return FoundAlias
	case Identifier, As: // we found indication of alias
		if nextItem.Token == Identifier {
			FoundAlias = true
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
					FoundAlias = true
					newAlias := SelectAlias{item.Val, ii.Val}
					result.SelectAl = append(result.SelectAl, newAlias)
					break identifierLookup
				default:
					p.unscan()
					break identifierLookup
				}
			}
		}
		return FoundAlias
	default:
		p.unscan()
		return FoundAlias
	}
	return FoundAlias
}
func (p *Parser) DetectTableAlias(result *SelectStatement, item Item) bool {
	var nextItem Item
detectAliasLoop:
	for {
		nextItem = p.nextItem()
		if nextItem.Token != Whitespace {
			break detectAliasLoop
		}
	}
	switch nextItem.Token {
	case Identifier, As: // we found indication of alias
		if nextItem.Token == Identifier {
			newAlias := TableAlias{item.Val, nextItem.Val}
			result.TableAl = append(result.TableAl, newAlias)
			return true
		} else {
		identifierLookup:
			for {
				ii := p.nextItem()
				switch ii.Token {
				case Whitespace:
					continue
				case Identifier:
					newAlias := TableAlias{item.Val, ii.Val}
					result.TableAl = append(result.TableAl, newAlias)
					return true
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
func (p *Parser) parseCase(result *SelectStatement, alias string) error {
	var newCase *CaseField
	var newWhen *WhenCond
	for {
		item := p.nextItem()
		switch item.Token {
		case Whitespace:
			continue
		case End:
			newCase.WhenCond = append(newCase.WhenCond, *newWhen)
			newWhen = nil
			//newCase.WhenCond = append(newCase.WhenCond, *newWhen)
			newCase.Alias = alias
			newComplexSelect := ComplexSelect{}
			newComplexSelect.CaseStatement = newCase
			if alias != "" {
				newComplexSelect.Alias = alias
			}
			result.CaseFields = append(result.CaseFields, *newCase)
			result.ComplexSelects = append(result.ComplexSelects, newComplexSelect)
			return nil
		case Identifier:
			newCase.FieldIdentifier = item.Val
		case Case:
			if newCase != nil {
				result.CaseFields = append(result.CaseFields, *newCase)
			}
			newCase = &CaseField{}
		case When:
			if newWhen != nil {
				newCase.WhenCond = append(newCase.WhenCond, *newWhen)
			}
			newWhen = &WhenCond{}
			e := p.parseExpression(&(newWhen.WhenCond))
			if e != nil {
				return e
			}
		case Then, Else:
			if newWhen == nil {
				return errors.New("Then found without preceeding When <condition>")
			}
			var thenItem Item
			thenItems := []Item{}
		thenLookup:
			for {
				thenItem = p.nextItem()
				switch thenItem.Token {
				case Whitespace:
					continue
				case Else, When, End, From:
					p.unscan()
					break thenLookup
				default:
					thenItems = append(thenItems, thenItem)
				}
			}
			if item.Token == Then {
				if len(thenItems) == 1 {
					le := LiteralExpression{thenItems[0].Token, thenItems[0].Val}
					newWhen.ThenCond = le.String()
				} else {
					var expression Expression
					e := parseSubExpression(&expression, thenItems)
					if e != nil {
						return e
					}
					newWhen.ThenCond = expression.String()
				}
			} else { // ELSE statement
				if len(thenItems) == 1 {
					le := LiteralExpression{thenItems[0].Token, thenItems[0].Val}
					newCase.ElseCond = le.String()
				} else {
					var expression Expression
					e := parseSubExpression(&expression, thenItems)
					if e != nil {
						return e
					}
					newCase.ElseCond = expression.String()
				}
			}
		default:
			return errors.New("Unknown Token " + item.String() + " Expected WHEN,ELSE, or THEN")
		}
	}
}

type SubParser struct {
	items    []Item
	curIndex int
}

func (s *SubParser) nextItem() Item {
	i := s.items[s.curIndex]
	s.curIndex++
	return i
}
func (s *SubParser) unScan() {
	if s.curIndex > 0 {
		s.curIndex--
	}
}

// Parse parses the tokens provided by a scanner (lexer) into an AST
func (p *Parser) Parse(result *Statement) error {
	statement := &SelectStatement{}
	_ = "breakpoint"
	parentCountSub := 0
	isASubQuery := false
	if item := p.nextItem(); item.Token == ParenOpen {
		parentCountSub++
		isASubQuery = true
	} else {
		p.unscan()
	}
	if item := p.nextItem(); item.Token != Select {
		return fmt.Errorf("found %v, expected SELECT", item.Inspect())
	}

	for {
		// Read a field.
		item := p.nextItem()
		//fmt.Println("item", item)
		switch item.Token {
		case ParenOpen:
			if isASubQuery {
				parentCountSub++
			}
		case ParenClose:
			if isASubQuery {
				parentCountSub--
				if parentCountSub == 0 {
					*result = Statement(statement)
					return nil
				}
			}
		case Identifier, Asterisk, Number:

			statement.Fields = append(statement.Fields, item.Val)
			newComplexSelect := ComplexSelect{}
			newComplexSelect.FieldName = item.Val
			//fmt.Println("FoundIdentifier", item.Val)
			if p.DetectFieldAlias(statement, item) {
				newComplexSelect.Alias = statement.SelectAl[len(statement.SelectAl)-1].Alias
			}
			statement.ComplexSelects = append(statement.ComplexSelects, newComplexSelect)
		case Multiply: // special case for now.
			statement.Fields = append(statement.Fields, "*")
			newComplexSelect := ComplexSelect{}
			newComplexSelect.FieldName = "*"
			statement.ComplexSelects = append(statement.ComplexSelects, newComplexSelect)
		case Count, Avg, Min, Max, Sum, Concat, RowNum, Nvl, Trim, From_unixtime:
			p.unscan()
			ag := Aggregate{}
			e := p.parseAggregate(&ag)
			if e != nil {
				return e
			}
			if item.Token == RowNum {
				nextToken := p.nextItem()
				if nextToken.Token == Over {
					ag.Params = append(ag.Params, nextToken)
				RowNumOverLoop:
					for {
						nextToken = p.nextItem()
						switch nextToken.Token {
						case ParenClose:
							ag.Params = append(ag.Params, nextToken)
							break RowNumOverLoop
						default:
							ag.Params = append(ag.Params, nextToken)
						}
					}
				} else {
					p.unscan()
				}
			}
			//fmt.Println(len(ag.Params), ag.Params)
			statement.Aggregates = append(statement.Aggregates, ag)
			statement.Fields = append(statement.Fields, ag.String())
			pItem := Item{item.Token, ag.String()}
			newComplexSelect := ComplexSelect{}
			newComplexSelect.AggregateField = &ag
			if p.DetectFieldAlias(statement, pItem) {
				newComplexSelect.Alias = statement.SelectAl[len(statement.SelectAl)-1].Alias
			}
			statement.ComplexSelects = append(statement.ComplexSelects, newComplexSelect)
		case QuotedString, SinglQuotedString:
			// check the following non Whitespace token
			statement.Fields = append(statement.Fields, strconv.QuoteToASCII(item.Val))
			var nextItem Item
		CaseWhenLoop1:
			for {
				nextItem = p.nextItem()
				//fmt.Println("NextItem", nextItem)
				switch nextItem.Token {
				case Whitespace:
					continue
				case Equals: //we found case...when...then...end
					e := p.parseCase(statement, item.Val)
					//fmt.Println("Parse Case Done")
					if e != nil {
						return e
					}
					break CaseWhenLoop1
				case Case:
					//fmt.Println(statement)
					return errors.New("Need = before Case in select field")
				case As, Identifier: //WeFoundAlias
					//fmt.Println("Alias Detected")
					p.unscan()
					newComplexSelect := ComplexSelect{}
					le := LiteralExpression{item.Token, item.Val}
					newComplexSelect.StaticValue = le.String()
					if p.DetectFieldAlias(statement, item) {
						//fmt.Println("Alias Found")
						newComplexSelect.Alias = statement.SelectAl[len(statement.SelectAl)-1].Alias
					}
					statement.ComplexSelects = append(statement.ComplexSelects, newComplexSelect)
					break CaseWhenLoop1
				default:
					p.unscan()
					break CaseWhenLoop1
				}
			}
		case Case:
			p.unscan()
			e := p.parseCase(statement, "")
			if e != nil {
				return e
			}
			//fmt.Println("Parse Case Done")
		detectCaseAlias:
			for {
				nitem := p.nextItem()
				switch nitem.Token {
				case Whitespace:

				case Identifier, As:
					if nitem.Token == Identifier {
						statement.CaseFields[len(statement.CaseFields)-1].Alias = nitem.Val
						statement.ComplexSelects[len(statement.ComplexSelects)-1].Alias = nitem.Val
					} else {
					detectAlias:
						for {
							n2 := p.nextItem()
							switch n2.Token {
							case Identifier:
								statement.CaseFields[len(statement.CaseFields)-1].Alias = n2.Val
								statement.ComplexSelects[len(statement.ComplexSelects)-1].Alias = n2.Val
								break detectAlias
							default:
								p.unscan()
								return errors.New("Syntax Error, Expecting Identifier after AS, got " + n2.String() + " instead")
							}
						}
					}
					statement.Fields = append(statement.Fields, statement.CaseFields[len(statement.CaseFields)-1].Alias)
				default:
					p.unscan()
					break detectCaseAlias
				}
			}

		default:
			return fmt.Errorf("found %v, expected field", item.Inspect())
		}

		// If the next token is not a comma then break the loop.
		if item := p.nextItem(); item.Token != Comma {
			//fmt.Println(item)
			p.unscan()
			break
		}
	}

	// Next we should see the "FROM" keyword.
	if item := p.nextItem(); item.Token != From {
		return fmt.Errorf("found %v, expected FROM", item.Inspect())
	}

	item := p.nextItem()
	if item.Token == Identifier {
		statement.TableName = item.Val
		pTable := Item{}
		pTable.Token = Identifier
		pTable.Val = item.Val
		complexTable := ComplexTable{}
		if p.DetectTableAlias(statement, pTable) {
			complexTable.Alias = statement.TableAl[len(statement.TableAl)-1].Alias
		}
		complexTable.TableName = item.Val
		statement.ComplexFrom = complexTable
	} else if item.Token == ParenOpen { //complexTable Found
		p.unscan()
		var newSubStatement Statement
		e := p.Parse(&newSubStatement)
		if e != nil {
			return e
		}
		pTable := Item{}
		pTable.Token = Identifier
		complexTable := ComplexTable{}
		if p.DetectTableAlias(statement, pTable) {
			complexTable.Alias = statement.TableAl[len(statement.TableAl)-1].Alias
		}

		complexTable.SubSelect = (newSubStatement.(*SelectStatement))
		statement.ComplexFrom = complexTable
	} else {
		return fmt.Errorf("found %v, expected table name", item.Inspect())
	}

	if item := p.nextItem(); item.Token == ParenClose {
		if isASubQuery {
			parentCountSub--
			if parentCountSub == 0 {
				*result = Statement(statement)
				fmt.Println(statement)
				fmt.Println("Return after Selecting Table")
				return nil
			}
		}
	} else {
		p.unscan()
	}

	if item := p.nextItem(); item.Token == Join || item.Token == LeftJoin || item.Token == RightJoin || item.Token == InnerJoin ||
		item.Token == RightOuterJoin || item.Token == LeftOuterJoin || item.Token == Comma {
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
			case Where, EOF:
				fmt.Println("Found", item.Inspect())
				p.unscan()
				break JoinLoop
			case Join, LeftJoin, RightJoin, InnerJoin, Comma:
				newJoinStatement := &JoinTables{}
				newJoinStatement.JoinType = item.Val
				e := p.parseJoin(newJoinStatement, statement)
				if e != nil {
					log.Debug(e)
					return e
				}
				if newJoinStatement.TableName != "" || newJoinStatement.SubSelect != nil {
					statement.Joins = append(statement.Joins, *newJoinStatement)
				}
			}
		}
	} else {
		p.unscan()
	}
	item2 := p.nextItem()
	if item2.Token == ParenClose {
		if isASubQuery {
			parentCountSub--
			if parentCountSub == 0 {
				*result = Statement(statement)
				return nil
			}
		}
	} else if item2.Token == EOF {
		p.unscan()
		*result = Statement(statement)
		return nil
	} else {
		fmt.Println(item2.Inspect())
		p.unscan()
	}

	var err error

	if err = p.parseConditional(&statement.Where); err != nil {
		return err
	}
	if item := p.nextItem(); item.Token == ParenClose {
		if isASubQuery {
			parentCountSub--
			if parentCountSub == 0 {
				*result = Statement(statement)
				return nil
			}
		}
	} else {
		p.unscan()
	}
nextOption:
	for {
		item := p.nextItem()
		fmt.Println(item)
		switch item.Token {
		case Whitespace:
			continue
		case GroupBy:
			p.parseGroupBy(&(statement.GroupBy))
		case Having:
			p.parseExpression(&statement.Having)
		case OrderBy:
			p.parseOrderBy(&statement.OrderBy)
		case ParenClose:
			if isASubQuery {
				parentCountSub--
				if parentCountSub == 0 {
					*result = Statement(statement)
					return nil
				}
			}
		case EOF:
			break nextOption
		}
	}

	*result = Statement(statement)
	return nil
}

func (p *Parser) parseOrderBy(result *[]SortField) error {
	var curField SortField
	fmt.Println("Parse Orderby")
	for {
		item := p.nextItem()
		fmt.Println(item)
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
			curField.Sort = item.Val
			*result = append(*result, curField)
		case Comma:
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
			result.Params = append(result.Params, item)
		case ParenClose:
			if !parentOpenFound {
				return errors.New("Closing parenthesis found befor open parenthesis")
			}
			parentOpenNum--
			// parentCloseFound = true
			if parentOpenNum == 0 {
				result.Params = append(result.Params, item)
				break AggrLoop
			}

		case Identifier, Multiply, Asterisk:
			if !parentOpenFound {
				return errors.New("Identifier found befor open parenthesis")
			}
			if (item.Token == Multiply || item.Token == Asterisk) && aggrFunc.Token != Count {
				return errors.New("Identifier * Can only be used on Count")
			}

			result.FieldName = item.Val
			result.Params = append(result.Params, item)
		case Comma:
			result.Params = append(result.Params, item)
		default:
			result.Params = append(result.Params, item)
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
func (p *Parser) parseJoin(result *JoinTables, statement *SelectStatement) error {
	// retrieve table name
	var e error
	tableName := p.nextItem()
	if tableName.Token == Identifier {
		pTable := Item{}
		pTable.Token = Identifier
		pTable.Val = tableName.Val
		if p.DetectTableAlias(statement, pTable) {
			result.Alias = statement.TableAl[len(statement.TableAl)-1].Alias
		}
		log.Debug(tableName.Val)
		result.TableName = tableName.Val
		if result.JoinType == "," {
			return nil
		}
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
		e = p.parseExpression(adad)
	} else if tableName.Token == ParenOpen {
		fmt.Println("Complex Join Found")
		p.unscan()
		var subStatement Statement
		e = p.Parse(&subStatement)
		if e != nil {
			return e
		}
		pTable := Item{}
		pTable.Token = Identifier
		pTable.Val = tableName.Val
		if p.DetectTableAlias(statement, pTable) {
			result.Alias = statement.TableAl[len(statement.TableAl)-1].Alias
		}
		result.SubSelect = subStatement.(*SelectStatement)
		if result.JoinType == "," {
			return nil
		}
		adad := &(result.OnCondition)
		e = p.parseExpression(adad)
	} else {
		fmt.Errorf("Expected table name, but found %s instead", tableName.Inspect())
		p.unscan()
		return errors.New(fmt.Sprintf("found %v, expected field", tableName.Inspect())) //fmt.Errorf("found %v, expected field", item.Inspect())
	}

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
	log.Debug("Parsing Expression")
	items := []Item{}
	done := false
	// depth := 0
	parentCount := 0 //parenthesis count
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
		case GroupBy, Having, OrderBy, Limit, ForUpdate, EOF, Where, Join, LeftJoin, RightJoin, InnerJoin, Then,
			LeftOuterJoin, RightOuterJoin:
			p.unscan()
			done = true
			break
		case On:
			continue
		case Whitespace:
		case ParenOpen:
			parentCount++
		case ParenClose:
			parentCount--
			if parentCount < 0 {
				p.unscan()
				done = true
				break
			}
		default:
			fmt.Println("Parser Warning: Unhandled token", item.Inspect())
		}
		if item.Token != Where && item.Token != On && item.Token != Join &&
			item.Token != LeftJoin && item.Token != RightJoin && item.Token != RightOuterJoin && item.Token != LeftOuterJoin && item.Token != Having &&
			item.Token != InnerJoin && item.Token != OrderBy && item.Token != GroupBy && item.Token != Then && !(item.Token == ParenClose && parentCount < 0) {
			items = append(items, item)
		}

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
	items = withoutWhitespace(items)
	//fmt.Println("Processing this", items, len(items))
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
	// we have a functional expression

	// detect the type of expression.
	if len(items) == 1 { // handle the simple case where we only have one element.
		switch items[0].Token {
		case Number, QuotedString, Boolean, Identifier, Null, SinglQuotedString:
			*result = &LiteralExpression{Token: items[0].Token, Val: items[0].Val}
			return nil
		}
	}
	fmt.Println(len(items) > 1, items[1].Token == ParenOpen, items[len(items)-1].Token == ParenClose)
	if len(items) > 1 && items[1].Token == ParenOpen && items[len(items)-1].Token == ParenClose {
		parameters := []Item{}
		for i, _ := range items {
			if i == 0 || i == len(items)-1 {
				continue
			}
			parameters = append(parameters, items[i])
		}
		*result = &FunctionExpression{
			FunctionName: items[0].Val,
			Parameters:   parameters,
		}
		return nil
	}
	//	if we only have 2 elements, we probably have a NOT or something.
	// if we have 3 element and the middle is either comparator operator
	// if len(items) == 3 {
	// 	if isOperator(rune(items[1].Val[0])) {
	// 		var rightExp LiteralExpression
	// 		var leftExp IdentifierExpression
	// 		switch items[0].Token {
	// 		case Identifier, Number, QuotedString:
	// 			a := IdentifierExpression{}
	// 			a.Name = items[0].Val
	// 			leftExp = a
	// 		default:
	// 			return errors.New("Invalid Type of operand " + items[0].Val + " in Comparasion")
	// 		}
	// 		switch items[2].Token {
	// 		case Identifier, Number, QuotedString:
	// 			a := LiteralExpression{}
	// 			a.Val = items[2].Val
	// 			a.Token = items[2].Token
	// 			rightExp = a
	// 		default:
	// 			return errors.New("Invalid Type of operand " + items[0].Val + " in Comparasion")
	// 		}
	// 		operator := &ComparisonOperator{}
	// 		operator.Token = items[1].Token
	// 		operator.Val = items[1].Val
	// 		*result = &BooleanExpression{
	// 			Left:     leftExp,
	// 			Right:    &rightExp,
	// 			Operator: operator,
	// 		}
	// 		return nil
	// 	}
	// }
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
