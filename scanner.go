package sqlast

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"strings"
	"unicode/utf8"
)

var eof = rune(0)

func init() {
	//log.SetOutput(ioutil.Discard)
}

// Scanner (aka lexer) takes a string of text and pulls tokens out of it.
// these are passed to the parser which turns them into an AST structure.
type Scanner struct {
	r             *bufio.Reader
	buf           *bytes.Buffer
	lastReadToken string
	lastReadRune  rune
	lastReadItem  Item
}

func NewScanner(r io.Reader) *Scanner {
	return &Scanner{
		r:   bufio.NewReader(r),
		buf: bytes.NewBufferString(""),
	}
}

// read reads the next rune from the bufferred reader.
// Returns the rune(0) if an error occurs (or io.EOF is returned).
func (s *Scanner) read() rune {
	if s.buf.Len() > 0 {
		r, _, err := s.buf.ReadRune()
		if err != nil {
			panic(err)
		}
		// log.Println("buf.ReadRune", string(r))
		s.lastReadRune = r
		return r
	}
	s.buf.Truncate(0) // need to get rid of back buffer
	r, _, err := s.r.ReadRune()
	//log.Println("s.r.ReadRune", string(r))
	s.lastReadRune = r
	if err == io.EOF {
		return eof
	}
	if err != nil {
		log.Println("Error reading rune:", err)
		panic(err)
	}
	return r
}

// peek reads the next rune from the bufferred reader and then returns it.
// Returns the rune(0) if an error occurs (or io.EOF is returned).
func (s *Scanner) peek() rune {
	defer s.unread()
	return s.read()
}

// unread places the previously read rune back on the reader.
func (s *Scanner) unread() {
	//log.Println("buf.UnreadRune (pre) is ", s.buf.String(), s.buf.Len())
	if err := s.buf.UnreadRune(); err == nil {
		//log.Println("buf.UnreadRune", string(s.lastReadRune))
		//log.Println("buf.UnreadRune is ", s.buf.String(), s.buf.Len())
		return
	} else if s.buf.Len() == 0 {
		//log.Println("buf.WriteRune", string(s.lastReadRune))
		s.buf.WriteRune(s.lastReadRune)
	} else {
		//log.Println("NewBuf", string(s.lastReadRune))
		// stuff in buffer and can't unread rune.
		newBuf := &bytes.Buffer{}
		newBuf.WriteRune(s.lastReadRune)
		newBuf.Write(s.buf.Bytes())
		s.buf = newBuf
	}
	// if err := s.r.UnreadRune(); err != nil {
	// 	log.Println("Error Unreading rune:", err)
	// 	panic(err)
	// }
}

// read token attempts to read "s" from the buffer.
// If the next few bytes are not equal to s, all read bytes are unread and readToken returns false.
// otherwise readToken returns true and saves the value to lastReadToken.
func (s *Scanner) tryReadToken(token string) bool {
	//log.Println("tryReadToken", token)
	// if token == "Current_date" {
	// 	fmt.Println("Try read Current_date", token)
	// }
	readRunes := &bytes.Buffer{}
	for i := 0; i < len(token); i++ {
		r := s.read()

		if utf8.RuneLen(r) != 1 || strings.ToUpper(string(r)) != string(token[i]) {
			s.unread()
			if readRunes.Len() != 0 {
				s.unreadString(readRunes.String())
			}
			return false
		}
		readRunes.WriteRune(r)
	}
	// check that we're at also a border character.
	r := s.peek()
	if (len(token) == 1 && isOperator(rune(token[0]))) || (len(token) == 2 && isOperator(rune(token[0])) && isOperator(rune(token[1]))) {
		if token == readRunes.String() {
			s.lastReadToken = readRunes.String()
			return true
		}
	}
	if !isWhitespace(r) && !isParenthesis(r) && !(r == ',') && r != eof {
		s.unreadString(readRunes.String())
		return false
	}
	s.lastReadToken = readRunes.String()
	return true
}

func (s *Scanner) unreadString(str string) {
	//log.Println("unreadString", str)
	if s.buf.Len() == 0 {
		s.buf.WriteString(str)
	} else {
		// stuff in buffer and can't unread rune.
		newBuf := &bytes.Buffer{}
		newBuf.WriteString(str)
		newBuf.Write(s.buf.Bytes())
		s.buf = newBuf
	}
}

// Scan returns the next token and literal value.
func (s *Scanner) Scan() Item {
	// Read the next rune.
	ch := s.read()

	// If we see whitespace then consume all contiguous whitespace.
	// If we see a letter then consume as an Identifier or reserved word.
	if isWhitespace(ch) {
		s.unread()
		return s.scanWhitespace()
	}

	if isDigit(ch) {
		s.unread()
		return s.scanNumber()
	}

	if isOperator(ch) {
		s.unread()
		if s.tryOperands() {
			return s.lastReadItem
		}
		return Item{Illegal, string(ch)}
	}

	if isLetter(ch) {
		s.unread()
		if s.tryKeywords() {
			return s.lastReadItem
		}
		if s.tryOperands() {
			return s.lastReadItem
		}
		return s.scanIdentifier()
	}

	if ch == '"' {
		s.unread()
		return s.scanQuotedString()
	}
	if ch == 39 {
		s.unread()
		return s.scanSingleQuotedString()
	}
	// Otherwise read the individual character.
	switch ch {
	case eof:
		return Item{EOF, ""}
	case '*':
		return Item{Asterisk, "*"}
	case ',':
		return Item{Comma, ","}
	case '(':
		return Item{ParenOpen, "("}
	case ')':
		return Item{ParenClose, ")"}
	}

	return Item{Illegal, string(ch)}
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanWhitespace() Item {
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent whitespace character into the buffer.
	// Non-whitespace characters and EOF will cause the loop to exit.
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isWhitespace(ch) {
			s.unread()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return Item{Whitespace, buf.String()}
}

// scanIdentifier consumes the current rune and all contiguous Identifier runes.
func (s *Scanner) scanIdentifier() Item {
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent Identifier character into the buffer.
	// Non-Identifier characters and EOF will cause the loop to exit.
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isLetter(ch) && !isDigit(ch) && ch != '_' && ch != '.' {
			s.unread()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	return Item{Identifier, buf.String()}
}

// scanNumber consumes the current rune and all contiguous numeric runes.
func (s *Scanner) scanNumber() Item {
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isDigit(ch) && ch != '.' {
			s.unread()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	return Item{Number, buf.String()}
}
func (s *Scanner) scanSingleQuotedString() Item {
	var buf bytes.Buffer
	quoteChr := s.read()
	_ = quoteChr
	for {
		ch := s.read()
		if ch == eof {
			return Item{Illegal, "EOF found before end of string"}
		}
		if ch == 39 {
			break
		}
		_, _ = buf.WriteRune(ch)
	}
	return Item{SinglQuotedString, buf.String()}
}
func (s *Scanner) scanQuotedString() Item {
	var buf bytes.Buffer
	quoteChr := s.read()
	_ = quoteChr

	for {
		ch := s.read()
		if ch == eof {
			return Item{Illegal, "EOF found before end of string"}
		}
		if ch == '"' {
			break
		}
		if ch == '\\' {
			ch = s.read()
		}
		_, _ = buf.WriteRune(ch)
	}

	return Item{QuotedString, buf.String()}
}

func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '@'
}

func isDigit(ch rune) bool {
	return ch >= '0' && ch <= '9'
}
func isParenthesis(ch rune) bool {
	return ch == '(' || ch == ')'
}
func isOperator(ch rune) bool {
	return ch == '=' ||
		ch == '<' ||
		ch == '>' ||
		ch == '/' ||
		ch == '%' ||
		ch == '&' ||
		ch == '!' ||
		ch == '|' ||
		ch == '+' ||
		ch == '-' ||
		ch == '*'
}

// tryKeywords returns true if it can read a keyword, placing it in lastReadItem if returning true
func (s *Scanner) tryKeywords() bool {
	if s.tryReadToken("SELECT") {
		s.lastReadItem = Item{Select, s.lastReadToken}
		return true
	} else if s.tryReadToken("FROM_UNIXTIME") {
		s.lastReadItem = Item{From_unixtime, s.lastReadToken}
		return true
	} else if s.tryReadToken("FROM") {
		s.lastReadItem = Item{From, s.lastReadToken}
		return true
	} else if s.tryReadToken("WHERE") {
		s.lastReadItem = Item{Where, s.lastReadToken}
		return true
	} else if s.tryReadToken("GROUP BY") {
		s.lastReadItem = Item{GroupBy, s.lastReadToken}
		return true
	} else if s.tryReadToken("SUM") {
		s.lastReadItem = Item{Sum, s.lastReadToken}
		return true
	} else if s.tryReadToken("AVG") {
		s.lastReadItem = Item{Avg, s.lastReadToken}
		return true
	} else if s.tryReadToken("NVL") {
		s.lastReadItem = Item{Nvl, s.lastReadToken}
		return true
	} else if s.tryReadToken("SPLIT") {
		s.lastReadItem = Item{Split, s.lastReadToken}
		return true
	} else if s.tryReadToken("SUBSTR") {
		s.lastReadItem = Item{Substr, s.lastReadToken}
		return true
	} else if s.tryReadToken("SUBSTRING") {
		s.lastReadItem = Item{Substr, s.lastReadToken}
		return true
	} else if s.tryReadToken("REGEX_REPLACE") {
		s.lastReadItem = Item{RegexReplace, s.lastReadToken}
		return true
	} else if s.tryReadToken("LPAD") {
		s.lastReadItem = Item{Lpad, s.lastReadToken}
		return true
	} else if s.tryReadToken("DATEDIFF") {
		s.lastReadItem = Item{DateDiff, s.lastReadToken}
		return true
	} else if s.tryReadToken("EXPLODE") {
		s.lastReadItem = Item{Explode, s.lastReadToken}
		return true
	} else if s.tryReadToken("LENGTH") {
		s.lastReadItem = Item{Length, s.lastReadToken}
		return true
	} else if s.tryReadToken("COALESCE") {
		s.lastReadItem = Item{COALESCE, s.lastReadToken}
		return true
	} else if s.tryReadToken("CAST") {
		s.lastReadItem = Item{Cast, s.lastReadToken}
		return true
	} else if s.tryReadToken("RANK") {
		s.lastReadItem = Item{Rank, s.lastReadToken}
		return true
	} else if s.tryReadToken("DENSE_RANK") {
		s.lastReadItem = Item{DenseRank, s.lastReadToken}
		return true
	} else if s.tryReadToken("TRIM") {
		s.lastReadItem = Item{Trim, s.lastReadToken}
		return true
	} else if s.tryReadToken("UNION") {
		s.lastReadItem = Item{Union, s.lastReadToken}
		return true
	} else if s.tryReadToken("UNIX_TIMESTAMP") {
		s.lastReadItem = Item{Unix_timestamp, s.lastReadToken}
		return true
	} else if s.tryReadToken("MIN") {
		s.lastReadItem = Item{Min, s.lastReadToken}
		return true
	} else if s.tryReadToken("MAX") {
		s.lastReadItem = Item{Max, s.lastReadToken}
		return true
	} else if s.tryReadToken("COUNT") {
		s.lastReadItem = Item{Count, s.lastReadToken}
		return true
	} else if s.tryReadToken("ROW_NUMBER") {
		s.lastReadItem = Item{RowNum, s.lastReadToken}
		return true
	} else if s.tryReadToken("TO_DATE") {
		s.lastReadItem = Item{ToDate, s.lastReadToken}
		return true
	} else if s.tryReadToken("YEAR") {
		s.lastReadItem = Item{Year, s.lastReadToken}
		return true
	} else if s.tryReadToken("QUARTER") {
		s.lastReadItem = Item{Quarter, s.lastReadToken}
		return true
	} else if s.tryReadToken("MONTH") {
		s.lastReadItem = Item{Month, s.lastReadToken}
		return true
	} else if s.tryReadToken("HOUR") {
		s.lastReadItem = Item{Hour, s.lastReadToken}
		return true
	} else if s.tryReadToken("MINUTE") {
		s.lastReadItem = Item{Minute, s.lastReadToken}
		return true
	} else if s.tryReadToken("LAST_DAY") {
		s.lastReadItem = Item{LastDay, s.lastReadToken}
		return true
	} else if s.tryReadToken("DATE_SUB") {
		s.lastReadItem = Item{DateSub, s.lastReadToken}
		return true
	} else if s.tryReadToken("CURRENT_DATE") {
		s.lastReadItem = Item{CurrentDate, s.lastReadToken}
		return true
	} else if s.tryReadToken("TRUNC") {
		s.lastReadItem = Item{Trunc, s.lastReadToken}
		return true
	} else if s.tryReadToken("OVER") {
		s.lastReadItem = Item{Over, s.lastReadToken}
		return true
	} else if s.tryReadToken("UPPER") {
		s.lastReadItem = Item{Upper, s.lastReadToken}
		return true
	} else if s.tryReadToken("PARTITION BY") {
		s.lastReadItem = Item{PartitionBy, s.lastReadToken}
		return true
	} else if s.tryReadToken("CONCAT") {
		s.lastReadItem = Item{Concat, s.lastReadToken}
		return true
	} else if s.tryReadToken("HAVING") {
		s.lastReadItem = Item{Having, s.lastReadToken}
		return true
	} else if s.tryReadToken("ORDER BY") {
		s.lastReadItem = Item{OrderBy, s.lastReadToken}
		return true
	} else if s.tryReadToken("LIMIT") {
		s.lastReadItem = Item{Limit, s.lastReadToken}
		return true
	} else if s.tryReadToken("JOIN") {
		s.lastReadItem = Item{Join, s.lastReadToken}
		return true
	} else if s.tryReadToken("LEFT JOIN") {
		s.lastReadItem = Item{LeftJoin, s.lastReadToken}
		return true
	} else if s.tryReadToken("LEFT OUTER JOIN") {
		s.lastReadItem = Item{LeftOuterJoin, s.lastReadToken}
		return true
	} else if s.tryReadToken("RIGHT JOIN") {
		s.lastReadItem = Item{RightJoin, s.lastReadToken}
		return true
	} else if s.tryReadToken("RIGHT OUTER JOIN") {
		s.lastReadItem = Item{RightOuterJoin, s.lastReadToken}
		return true
	} else if s.tryReadToken("INNER JOIN") {
		s.lastReadItem = Item{InnerJoin, s.lastReadToken}
		return true
	} else if s.tryReadToken("FULL INNER JOIN") {
		s.lastReadItem = Item{FullInnerJoin, s.lastReadToken}
		return true
	} else if s.tryReadToken("FULL OUTER JOIN") {
		s.lastReadItem = Item{FullOuterJoin, s.lastReadToken}
		return true
	} else if s.tryReadToken("ON") {
		s.lastReadItem = Item{On, s.lastReadToken}
		return true
	} else if s.tryReadToken("LIKE") {
		s.lastReadItem = Item{Like, s.lastReadToken}
		return true
	} else if s.tryReadToken("NOT BETWEEN") {
		s.lastReadItem = Item{NotBetween, s.lastReadToken}
		return true
	} else if s.tryReadToken("NOT") {
		s.lastReadItem = Item{Not, s.lastReadToken}
		return true
	} else if s.tryReadToken("BETWEEN") {
		s.lastReadItem = Item{Between, s.lastReadToken}
		return true
	} else if s.tryReadToken("FOR UPDATE") {
		s.lastReadItem = Item{ForUpdate, s.lastReadToken}
		return true
	} else if s.tryReadToken("TRUE") {
		s.lastReadItem = Item{Boolean, s.lastReadToken}
		return true
	} else if s.tryReadToken("FALSE") {
		s.lastReadItem = Item{Boolean, s.lastReadToken}
		return true
	} else if s.tryReadToken("NULL") {
		s.lastReadItem = Item{Null, s.lastReadToken}
		return true
	} else if s.tryReadToken("CASE") {
		s.lastReadItem = Item{Case, s.lastReadToken}
		return true
	} else if s.tryReadToken("WHEN") {
		s.lastReadItem = Item{When, s.lastReadToken}
		return true
	} else if s.tryReadToken("THEN") {
		s.lastReadItem = Item{Then, s.lastReadToken}
		return true
	} else if s.tryReadToken("ELSE") {
		s.lastReadItem = Item{Else, s.lastReadToken}
		return true
	} else if s.tryReadToken("END") {
		s.lastReadItem = Item{End, s.lastReadToken}
		return true
	} else if s.tryReadToken("ASC") {
		s.lastReadItem = Item{Asc, s.lastReadToken}
		return true
	} else if s.tryReadToken("DESC") {
		s.lastReadItem = Item{Desc, s.lastReadToken}
		return true
	} else if s.tryReadToken("AS") {
		s.lastReadItem = Item{As, s.lastReadToken}
		return true
	}

	return false
}

func (s *Scanner) tryOperands() bool {
	//fmt.Println("tryOperands")
	if s.tryReadToken("<=>") {
		//fmt.Println("MyOperandFound")
		s.lastReadItem = Item{EqualNull, s.lastReadToken}
		return true
	} else if s.tryReadToken("<=") {
		s.lastReadItem = Item{LessThanEquals, s.lastReadToken}
		return true
	} else if s.tryReadToken("<") {
		s.lastReadItem = Item{LessThan, s.lastReadToken}
		return true
	} else if s.tryReadToken(">=") {
		s.lastReadItem = Item{GreaterThanEquals, s.lastReadToken}
		return true
	} else if s.tryReadToken(">") {
		s.lastReadItem = Item{GreaterThan, s.lastReadToken}
		return true
	} else if s.tryReadToken("=") {
		s.lastReadItem = Item{Equals, s.lastReadToken}
		return true
	} else if s.tryReadToken("!=") {
		s.lastReadItem = Item{NotEqual, s.lastReadToken}
		return true
	} else if s.tryReadToken("!") {
		s.lastReadItem = Item{Not, s.lastReadToken}
		return true
	} else if s.tryReadToken("NOT BETWEEN") {
		s.lastReadItem = Item{NotBetween, s.lastReadToken}
		return true
	} else if s.tryReadToken("NOT") {
		s.lastReadItem = Item{Not, s.lastReadToken}
		return true
	} else if s.tryReadToken("||") {
		s.lastReadItem = Item{Or, s.lastReadToken}
		return true
	} else if s.tryReadToken("&&") {
		s.lastReadItem = Item{And, s.lastReadToken}
		return true
	} else if s.tryReadToken("OR") {
		s.lastReadItem = Item{Or, s.lastReadToken}
		return true
	} else if s.tryReadToken("AND") {
		s.lastReadItem = Item{And, s.lastReadToken}
		return true
	} else if s.tryReadToken("XOR") {
		s.lastReadItem = Item{Xor, s.lastReadToken}
		return true
	} else if s.tryReadToken("IS NOT IN") {
		s.lastReadItem = Item{IsNotIn, s.lastReadToken}
		return true
	} else if s.tryReadToken("IS NOT") {
		s.lastReadItem = Item{IsNot, s.lastReadToken}
		return true
	} else if s.tryReadToken("IS") {
		s.lastReadItem = Item{Is, s.lastReadToken}
		return true
	} else if s.tryReadToken("|") {
		s.lastReadItem = Item{BinaryOr, s.lastReadToken}
		return true
	} else if s.tryReadToken("&") {
		s.lastReadItem = Item{BinaryAnd, s.lastReadToken}
		return true
	} else if s.tryReadToken("*") {
		s.lastReadItem = Item{Multiply, s.lastReadToken}
		return true
	} else if s.tryReadToken("/") {
		s.lastReadItem = Item{Divide, s.lastReadToken}
		return true
	} else if s.tryReadToken("+") {
		s.lastReadItem = Item{Add, s.lastReadToken}
		return true
	} else if s.tryReadToken("-") {
		s.lastReadItem = Item{Subtract, s.lastReadToken}
		return true
	} else if s.tryReadToken("%") {
		s.lastReadItem = Item{Modulus, s.lastReadToken}
		return true
	} else if s.tryReadToken("BETWEEN") {
		s.lastReadItem = Item{Between, s.lastReadToken}
		return true
	}
	return false
}

// // 		if s.tryReadToken("||")
// // 			return Item{Or, s.lastReadToken}
// // 		else if s.tryReadToken("OR")
// // 			return Item{Or, s.lastReadToken}
// // 		// if preview[0:1] == "||" ||
// // 		//  preview[0:1] == "&&" ||
// // 		//  preview[0:1] == "IS" ||
// // 		//  preview[0:1] == "OR" ||
// // 		//  preview[0:2] == "AND" ||
// // 		//  preview[0:2] == "XOR" ||
