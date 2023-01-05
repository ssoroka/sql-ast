package sqlast

import "fmt"

type Item struct {
	Token Token
	Val   string
}

func (i Item) String() string {
	return i.Inspect()
}

func (i Item) Inspect() string {
	tokenName := ""
	switch i.Token {
	case Illegal:
		tokenName = "Illegal"
	case EOF:
		tokenName = "EOF"
	case Whitespace:
		tokenName = "Whitespace"

	case Identifier:
		tokenName = "Identifier"
	case Number:
		tokenName = "Number"
	case Date:
		tokenName = "Date"
	case Time:
		tokenName = "Time"
	case Boolean:
		tokenName = "Boolean"
	case QuotedString:
		tokenName = "QuotedString"
	case LeftJoin:
		tokenName = "LeftJoin"
	case RightJoin:
		tokenName = "RightJoin"
	case LeftOuterJoin:
		tokenName = "LeftOuterJoin"
	case FullOuterJoin:
		tokenName = "FullOuterJoin"
	case FullInnerJoin:
		tokenName = "FullInnerJoin"
	case RightOuterJoin:
		tokenName = "RightOuterJoin"
	case Join:
		tokenName = "Join"
	case InnerJoin:
		tokenName = "InnerJoin"
	case On:
		tokenName = "On"
	case Null:
		tokenName = "Null"
	case Equals:
		tokenName = "Equals"
	case EqualNull:
		tokenName = "EqualNull"
	case LessThan:
		tokenName = "LessThan"
	case LessThanEquals:
		tokenName = "LessThanEquals"
	case GreaterThan:
		tokenName = "GreaterThan"
	case GreaterThanEquals:
		tokenName = "GreaterThanEquals"
	case Or:
		tokenName = "Or"
	case And:
		tokenName = "And"
	case Xor:
		tokenName = "Xor"
	case Is:
		tokenName = "Is"
	case Not:
		tokenName = "Not"
	case IsNot:
		tokenName = "IsNot"
	case IsNotIn:
		tokenName = "IsNotIn"
	case Like:
		tokenName = "Like"
	case NotLike:
		tokenName = "NotLike"
	case RLike:
		tokenName = "RLike"
	case NotRLike:
		tokenName = "NotRLike"
	case Asterisk:
		tokenName = "Asterisk"
	case Comma:
		tokenName = "Comma"
	case Placeholder:
		tokenName = "Placeholder"
	case ParenOpen:
		tokenName = "ParenOpen"
	case ParenClose:
		tokenName = "ParenClose"
	case Multiply:
		tokenName = "Multiply"

	case Select:
		tokenName = "Select"
	case Update:
		tokenName = "Update"
	case Insert:
		tokenName = "Insert"
	case Delete:
		tokenName = "Delete"
	case From_unixtime:
		tokenName = "From_unixtime"
	case Unix_timestamp:
		tokenName = "UNIX_Timestamp"
	case From:
		tokenName = "From"
	case Where:
		tokenName = "Where"
	case GroupBy:
		tokenName = "GroupBy"
	case Having:
		tokenName = "Having"
	case OrderBy:
		tokenName = "OrderBy"
	case Limit:
		tokenName = "Limit"
	case ForUpdate:
		tokenName = "ForUpdate"
	case Sum:
		tokenName = "Sum"
	case Avg:
		tokenName = "Avg"
	case Max:
		tokenName = "Max"
	case Min:
		tokenName = "Min"
	case Count:
		tokenName = "Count"
	case Nvl:
		tokenName = "NVL"
	case Trim:
		tokenName = "Trim"
	case ToDate:
		tokenName = "ToDate"
	case Year:
		tokenName = "Year"
	case Quarter:
		tokenName = "Quarter"
	case Month:
		tokenName = "Month"
	case Hour:
		tokenName = "Hour"
	case Minute:
		tokenName = "Minute"
	case LastDay:
		tokenName = "LastDay"
	case DateSub:
		tokenName = "DateSub"
	case Trunc:
		tokenName = "Trunc"
	case CurrentDate:
		tokenName = "Current_date"
	case Concat:
		tokenName = "Concat"
	case As:
		tokenName = "As"
	case Case:
		tokenName = "Case"
	case Then:
		tokenName = "Then"
	case When:
		tokenName = "When"
	case End:
		tokenName = "End"
	case NotEqualSynonim:
		tokenName = "NotEqualSynonim"
	case NotEqual:
		tokenName = "NotEqual"
	case SinglQuotedString:
		tokenName = "SingleQuotedStr"
	case Asc:
		tokenName = "Asc"
	case Desc:
		tokenName = "Desc"
	case RowNum:
		tokenName = "RowNumber"
	case In:
		tokenName = "In"
	case Union:
		tokenName = "Union"
	case All:
		tokenName = "Union"
	default:
		tokenName = "Unknwon Token"
	}
	return fmt.Sprintf("{%v, %q}", tokenName, i.Val)
}

// func (i Item) String() string {
// 	switch i.Token {
// 	case EOF:
// 		return "EOF"
// 	case Whitespace:
// 		return " "
// 	}
// 	if len(i.Val) > 10 {
// 		return fmt.Sprintf("%.10q...", i.Val)
// 	}
// 	return fmt.Sprintf("%q", i.Val)
// }
