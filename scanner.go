package sqlast

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"strings"
	"unicode/utf8"
)

var eof = rune(0)

func init() {
	log.SetOutput(ioutil.Discard)
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
		log.Println("buf.ReadRune", string(r))
		s.lastReadRune = r
		return r
	}
	s.buf.Truncate(0) // need to get rid of back buffer
	r, _, err := s.r.ReadRune()
	log.Println("s.r.ReadRune", string(r))
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
	log.Println("buf.UnreadRune (pre) is ", s.buf.String(), s.buf.Len())
	if err := s.buf.UnreadRune(); err == nil {
		log.Println("buf.UnreadRune", string(s.lastReadRune))
		log.Println("buf.UnreadRune is ", s.buf.String(), s.buf.Len())
		return
	} else if s.buf.Len() == 0 {
		log.Println("buf.WriteRune", string(s.lastReadRune))
		s.buf.WriteRune(s.lastReadRune)
	} else {
		log.Println("NewBuf", string(s.lastReadRune))
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
	log.Println("tryReadToken", token)
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
	if !isWhitespace(r) && r != eof {
		s.unreadString(readRunes.String())
		return false
	}
	s.lastReadToken = readRunes.String()
	return true
}

func (s *Scanner) unreadString(str string) {
	log.Println("unreadString", str)
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
		} else if !isLetter(ch) && !isDigit(ch) && ch != '_' {
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
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch rune) bool {
	return ch >= '0' && ch <= '9'
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
	} else if s.tryReadToken("FROM") {
		s.lastReadItem = Item{From, s.lastReadToken}
		return true
	} else if s.tryReadToken("WHERE") {
		s.lastReadItem = Item{Where, s.lastReadToken}
		return true
	} else if s.tryReadToken("GROUP BY") {
		s.lastReadItem = Item{GroupBy, s.lastReadToken}
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
	} else if s.tryReadToken("FOR UPDATE") {
		s.lastReadItem = Item{ForUpdate, s.lastReadToken}
		return true
	} else if s.tryReadToken("TRUE") {
		s.lastReadItem = Item{Boolean, s.lastReadToken}
		return true
	} else if s.tryReadToken("FALSE") {
		s.lastReadItem = Item{Boolean, s.lastReadToken}
		return true
	}

	return false
}

func (s *Scanner) tryOperands() bool {
	if s.tryReadToken("<=") {
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
