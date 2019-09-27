package sqlast

import (
	"strings"
	"testing"
)

func TestSimpleSelect(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `SELECT * from some_table`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if ast == nil {
		t.Error("Expected AST to be set but it was empty")
		t.FailNow()
	}

	if ast.String() != "SELECT *\nFROM some_table" {
		t.Error("Unexpected output", ast.String())
	}
}
func TestWhereParenthesis(t *testing.T) {
	input := "SELECT * FROM ABC WHERE ( (EBBSPRD_AE_ACNOM.ACCOUNTNO = EBBSPRD_AE_ACNOM.ACCOUNTNO) AND EBBSPRD_AE_ACNOM.BLDGNAME > EBBSPRD_AE_ACNOM.TELEPHONENO AND 1=1)"
	parser := NewParser(strings.NewReader(input))
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
	}
	//pp := selectStatement.(*SelectStatement)
}
func TestEqualNotNull(t *testing.T) {
	input := "SELECT * FROM ABC WHERE  1<=> 1"
	parser := NewParser(strings.NewReader(input))
	selectStatement := Statement(&SelectStatement{})
	err := parser.Parse(&selectStatement)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
	}
	//pp := selectStatement.(*SelectStatement)
	output := selectStatement.String()
	t.Log(output)
	outputExpected := `SELECT *
FROM ABC
WHERE
	1 <=> 1`
	if output != outputExpected {
		t.Fail()
	}
}
func TestWeirdBoolExpression(t *testing.T) {
	input := "select * from HHH where TRIM ( RDM_ALL_CTRY_CD_VER1.ISO_IND ) = ''"
	var ast Statement
	err := Parse(&ast, input)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if ast == nil {
		t.Error("Expected AST to be set but it was empty")
		t.FailNow()
	}
	t.Log(ast.String())
	expected := `SELECT *
FROM HHH
WHERE
	TRIM(RDM_ALL_CTRY_CD_VER1.ISO_IND) = ''`
	if ast.String() != expected {
		t.Log([]byte(ast.String()))
		t.Log([]byte(expected))
		t.Fail()
	}
}
func TestIncompleteJoin(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `SELECT * from some_table JOIN AA`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}
func TestNewFunctionSelect(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `SELECT To_Date(ll),Year(),Quarter(),month(),hour(),minute(),LAST_DAY(),Hour(),Current_date() from some_table`)
	if err != nil {
		t.Log("Found Error")
		t.Error(err)
		return
		//t.FailNow()
	}
	if ast.String() != "SELECT To_Date(ll), Year(), Quarter(), month(), hour(), minute(), LAST_DAY(), Hour(), Current_date()\n"+
		"FROM some_table" {
		t.Error("Parsed Data not same")
		t.Log(ast)
	}
	//t.Log(ast)
}
func TestJoinComplexComma(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `SELECT * from some_table,(select * from table1) AS XX,(select aa,bb from table2) as XY `)
	if err != nil {
		t.Log("Found Error")
		t.Error(err)
		return
		//t.FailNow()
	}
	//t.Log()
	oo := strings.Replace(ast.String(), "\n", " ", -1)
	oo = strings.Replace(oo, "\t", " ", -1)
	if oo != `SELECT * FROM some_table  , (SELECT * FROM table1) AS XX  , (SELECT aa, bb FROM table2) AS XY` {
		t.Log("Difierent OUtput detected")
		t.Log(oo)
		t.FailNow()
	} else {

	}
}
func TestJoinComma(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `SELECT * from some_table,table1,table2`)
	if err != nil {
		t.Log("Found Error")
		t.Error(err)
		return
		//t.FailNow()
	}
	//t.Log()
	oo := strings.Replace(ast.String(), "\n", " ", -1)
	oo = strings.Replace(oo, "\t", " ", -1)
	if oo != `SELECT * FROM some_table  , table1  , table2` {
		t.Log("Difierent OUtput detected")
		t.Log(oo)
		t.FailNow()
	} else {

	}
}
func TestSTFSelect(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `SELECT   ProductNumber,
      CASE ProductLine
         WHEN 'R' THEN 'Road'
         WHEN 'M' THEN 'Mountain'
         WHEN 'T' THEN 'Touring'
         WHEN 'S' THEN 'Other sale items'
         ELSE 'Not for sale'
      END as Category,
   Name
FROM Production.Product
ORDER BY ProductNumber`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if ast == nil {
		t.Error("Expected AST to be set but it was empty")
		t.FailNow()
	}

	if ast.String() != `SELECT ProductNumber, CASE
ProductLine
	WHEN 'R' THEN 'Road'
	WHEN 'M' THEN 'Mountain'
	WHEN 'T' THEN 'Touring'
	WHEN 'S' THEN 'Other sale items'
	 ELSE 'Not for sale'
END
 AS Category, Name
FROM Production.Product` {
		t.Error("Unexpected output", ast.String())
	}
}

func TestSimpleSelectFields(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `select
    one, two,  three,four
    FROM
    some_other_table
  `)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if ast.String() != "SELECT one, two, three, four\nFROM some_other_table" {
		t.Error("Unexpected output", ast.String())
	}
}

func TestSimpleSelectWhere(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `select 1 FROM some_other_table
  where a = 1
`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	expectedOutput := `SELECT 1
FROM some_other_table
WHERE
	a = 1`
	if ast.String() != expectedOutput {
		t.Error("Unexpected output, got", ast.String(), "expected", expectedOutput)
	}
}
func TestSQL1(t *testing.T) {
	var ast Statement
	sql1 := `SELECT	
	CASE	
		WHEN b.party_id='' 
		OR b.bank_group_code='' 
		OR b.cty_code='' THEN '-1' 
		ELSE NVL(CONCAT('OTP','-','@cntryCode','-',trim(b.party_id),'-',
			trim(b.bank_group_code),'-',trim(b.cty_code)),'-1') 
	END	as PRTY_SROGT_ID,customer_type as PRTY_TYPE_CD,'' as PRTY_SUB_TYPE_CD,
			b.party_id as DOM_PRTY_HOST_NUM,'' as DOM_PRTY_HOST_NUM_TYPE,
			PROFILE_STATUS as STS_CD,b.party_name as LEGAL_NM,'' as LONG_NM,
			b.party_short_name as SHORT_NM,SCI_Pty_mst.legal_status_constitution as LEGAL_Constitution_CD,
			SCI_Pty_mst.incorporation_no as INC_NUM,b.cty_code as DMCLE_CTRY_CD,
			'' as BKRPT_DT,from_unixtime(unix_timestamp(cast(SCI_Pty_mst.incorporation_date as varchar(10)),
			'yyyy-MM-dd'),'yyyy-MM-dd') as INCORPORATION_DT,'' as BSL_SEGMT,
			business_division as BUSN_TYPE,industry_code as ISIC_CD,'' as ISIC_DESC,
			'' as ISIC_STRT_DT,from_unixtime(unix_timestamp(cast(open_date as varchar(10)),
			'yyyy-MM-dd'),'yyyy-MM-dd') as RL_STRT_DT,from_unixtime(unix_timestamp(cast(close_date as varchar(10)),
			'yyyy-MM-dd'),'yyyy-MM-dd') as RL_END_DT,'' as DSCLS_AGMT_FL,
			SCI_Pty_mst.scb_internal_crg as INTRNL_CR_GRADE,affiliate_code as AFFL_CD,
			CONCAT('SCI','-','@cntryCode','-',nvl(concat(XREF.LSX_LE_ID,
			'-',XREF.LSX_LSP_ID),SCI_Pty_mst.sci_le_id)) as ENTRP_PRTY_SROGT_ID,
			nvl(concat(XREF.LSX_LE_ID,'-',XREF.LSX_LSP_ID),SCI_Pty_mst.sci_le_id) as ENTRP_PRTY_HOST_NUM,
			sub_seg_code as SUB_SEGMT_CD,'' as SEGMT_CD_DESC,'' as SUB_SEGMT_CD_DESC,
			'' as BSL_SEGMT_DESC,SCI_Pty_mst.legal_status_constitution_desc as LEGAL_Constitution_DESC,
			'' as PRIM_PHN_NUM,'' as PRIM_EML_ADDR,'' as PRIM_FAX_NUM,'' as CDD_SYS_RISK_CD,
			'' as CDD_RCMND_RISK_CD,SCI_Pty_mst.scb_internal_crg_desc as INTRNL_CR_GRADE_DESC,
			'OTP_PRTY_01' as PRCS_ID,seg_code as SEGMT_CD,'' as CDD_SYS_RISK_DESC,
			'' as CDD_RCMND_RISK_DESC,'' as PRTY_TYPE_DESC,'' as PRTY_SUB_TYPE_DESC,
			'' as STS_DESC,'' as FM_Appropriateness_IND,SCI_Pty_mst.incorporation_cty_code as Incorporation_CTRY_CD,
			'' as SCB_GRP_ENT_FL,'OTP-PARTY' as PRTY_CLAS,'' as BUSN_TYPE_DESC,
			'' as AFFL_CD_DESC,'' as NEW_SEGMT_CD,'' as NEW_SEGMT_CD_DESC,
			'' as STS_EFF_DT,'' as ENVMTL_AND_SOC_RISK_GRADE_CD,'' as ENVMTL_AND_SOC_RISK_GRADE_DESC,
			'' as GLOBL_BANK_RATG_FL,'' as DSCLS_AGMT_EFF_DT,'' as CR_RISK_RESP_LOC_CD,
			'' as IFRS_CNTPTY_TYPE_CD,'' as IFRS_CNTPTY_TYPE_DESC,'' as CTRY_OF_OPR_CD,
			'' as PRPS_OF_ACCT_OPN,'' as RL_TYPE,'' as RFL_STS,'' as RFL_STS_DESC,
			acc_type_code as PRTY_ACCT_TYPE_CD,'' as PRTY_ACCT_TYPE_CD_DESC,
			'' as SCB_STAF_FL,CONCAT('SCI','-','@cntryCode','-',trim(cast(XREF.LSX_LE_ID as string))) as ENTRP_PARNT_PRTY_SROGT_ID,
			XREF.LSX_LE_ID as ENTRP_PARNT_PRTY_HOST_NUM,'' as DCLR_INCM_AMT,
			'' as NET_WRTH_AMT,'' as TOT_CR_LMT_AMT,'' as SLS_TURN_AMT,'' as SLS_TURN_CURY,
			'' as ANL_INCM_AMT,'' as OTH_INCM_DESC,'' as Dodd_Frank_Incorporation_CTRY,
			'' as US_DMCLE_FL,'' as ELIG_CNTC_PARTP_FL,'' as SPL_ENT_FL,
			'' as SPL_ENT_SUB_CAT,'' as QIR_FL,'' as Dodd_Frank_ENT_TYPE_CD,
			'' as Dodd_Frank_ENT_SUB_TYPE_CD,'' as FIN_ENT_EXCPTN_FL,'' as TRAD_STS,
			'' as MAND_CLRG_FL,'' as Dodd_Frank_Compliant_FL,'' as PTCOL_2_FL,
			'' as PTCOL_2_SIDE_LTR_FL,'' as DF_PTCOL_1_FL,'' as END_USR_EXPT_NOTC_FL,
			'' as Opted_OUT_ANL_FIL_FL,'' as SEL_Mid_MK_EXPT_FL,'' as INIT_MRGN_METH,
			'' as TOT_PRVSN_AMT_IN_LCY,'' as RL_AVTN_DT,'' as BCA_REF_NUM,
			'' as SUPL_CHN_FIN_PRG_NM,'' as SUPL_CHN_FIN_PRG_SIZE,'' as PLCY_EXCPTN_CD,
			'' as SHR_HLD_OWN_PCT , '' as mkt_in_fin_instm_drctv_clas_de,
			'' as mkt_in_fin_instm_drctv_clas_cd, '' as cust_due_diligence_sts_cd,
			'' as cust_due_diligence_sts_cd_desc, '' as cust_bsl_sub_segmt_cd,
			'' as cust_bsl_sub_segmt_cd_desc, '' as dodd_frank_ent_type_cd_desc,
			'' as cust_trad_sts_desc, '' as dflt_uniq_trd_id_gen_pr, '' as dflt_uniq_trd_id_gen_pr_desc,
			'' as dodd_frank_due_dilig_cmpl_fl, '' as dodd_frank_due_dilig_cmpl_fl_d,
			'' as dodd_frank_due_diligence_compl, '' as cust_regs_num, '' as cust_ult_risk_ctry_cd,
			'' as lmts_cvrd, '' as mstr_programme_appl_id_num, '' as fm_appropriateness_ind_desc,
			'' as cust_appl_ack_dt, '' as cust_rl_rmk, concat('EBB','-',
			'@cntryCode','-',trim(EBBS_BRANCH_CODE),'-','BRANCH') as brnch_srogt_id,
			DEFAULT_TBU_CODE as sys_dept_cd 
	FROM	@PREFIX4_SCBT_R_PARTY_MST B LEFT JOIN @PREFIX4_SCBT_R_PARTY_SCI_MST SCI_PTY_MST 
		ON B.PARTY_ID= SCI_PTY_MST.PARTY_ID 
		AND B.BANK_GROUP_CODE = SCI_PTY_MST.BANK_GROUP_CODE 
		AND B.CTY_CODE = SCI_PTY_MST.CTY_CODE  LEFT JOIN (
	SELECT	* 
	FROM	(
	SELECT	PARTY_MST1.PARTY_ID AS PARTY_ID, XREF.LSX_LE_ID AS LSX_LE_ID,
			XREF.LSX_LSP_ID AS LSX_LSP_ID, ROW_NUMBER() OVER (PARTITION BY XREF.LSX_EXT_SYS_CUST_ID 
	ORDER BY XREF.LAST_UPD_DT DESC) AS MAXID 
	FROM	@PREFIX4_SCBT_R_PARTY_MST PARTY_MST1,@PREFIX3_LSPSYSXREF XREF,
			@PREFIX3_BKGLOC BOK_LOC  
	where	XREF.LSX_EXT_SYS_CUST_ID = PARTY_MST1.PARTY_ID 
		AND XREF.LSX_BKG_LOCTN_ID=BOK_LOC.BKL_SYS_GEN_BKG_LOCTN_ID 
		AND XREF.LSX_EXT_SYS_CD_VAL ='OAF' 
		AND BOK_LOC.bkl_cntry_iso_cd = PARTY_MST1.cty_code )AB 
	WHERE	AB.MAXID=1)XREF 
		ON B.PARTY_ID=XREF.PARTY_ID`
	err := Parse(&ast, sql1)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}
func TestSimpleSelectWhereQuotedStringEquality(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `select 1 FROM some_other_table where a = "giraffe"`)
	if err != nil {
		t.Error("EEE", err)
		t.Fail()
	}
	expectedOutput := `SELECT 1
FROM some_other_table
WHERE
	a = "giraffe"`
	if ast.String() != expectedOutput {
		t.Error("Unexpected output, got", ast.String(), "expected", expectedOutput)
	}
}

func TestSimpleSelectWhereQuotedStringAndBooleanEquality(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `select 1 FROM some_other_table where a = "giraffe" and b = true`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	expectedOutput := `SELECT 1
FROM some_other_table
WHERE
	a = "giraffe"
	AND b = TRUE`
	if ast.String() != expectedOutput {
		t.Error("Unexpected output, got", ast.String(), "expected", expectedOutput)
	}
}

func TestSimpleSelectWhereMultipleAndWithSubExpression(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `select 1 FROM some_other_table where a = "giraffe" and b = true AND (q is not null or q >= 3)`)
	// and last_known_date > "2015-05-02"
	// and created_at <= NOW()
	// `SELECT 1 FROM some_other_table WHERE a = 1 AND b = e.something AND (q IS NOT NULL OR q >= 3) AND last_known_date > "2015-05-02" AND created_at <= NOW()`
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	expectedOutput := `SELECT 1
FROM some_other_table
WHERE
	a = "giraffe"
	AND b = TRUE
	AND (q IS NOT NULL
	OR q >= 3)`
	if ast.String() != expectedOutput {
		t.Error("Unexpected output, got", ast.String(), "expected", expectedOutput)
	}
}

func TestSimpleSelectWhereMSingleWithSubExpression(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `select 1 FROM some_other_table where (q is not null or q >= 3)`)
	// and last_known_date > "2015-05-02"
	// and created_at <= NOW()
	// `SELECT 1 FROM some_other_table WHERE a = 1 AND b = e.something AND (q IS NOT NULL OR q >= 3) AND last_known_date > "2015-05-02" AND created_at <= NOW()`
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	expectedOutput := `SELECT 1
FROM some_other_table
WHERE
	(q IS NOT NULL
	OR q >= 3)`
	if ast.String() != expectedOutput {
		t.Error("Unexpected output, got", ast.String(), "expected", expectedOutput)
	}
}

// func TestScannerCanReadStuff(t *testing.T) {
// 	str := `1234567890 FRoM water SElect`
// 	scanner := NewScanner(bufio.NewReader(strings.NewReader(str)))

// 	item := scanner.Scan()
// 	if item.Token != Number {
// 		t.Error("Expected scanner to read number, got", item.Inspect())
// 	}
// 	if item.Val != "1234567890" {
// 		t.Error("Expected scanner to read number, got", item.Inspect())
// 	}

// 	if scanner.Scan().Token != Whitespace {
// 		t.Error("Expected whitespace")
// 	}

// 	item = scanner.Scan()
// 	if item.Token != From {
// 		t.Error("Expected scanner to read FROM keyword, got", item.Inspect())
// 	}
// 	if item.Val != "FRoM" {
// 		t.Error("Expected scanner to read FROM keyword, got", item.Inspect())
// 	}

// 	if scanner.Scan().Token != Whitespace {
// 		t.Error("Expected whitespace")
// 	}

// 	item = scanner.Scan()
// 	if item.Token != Identifier {
// 		t.Error("Expected scanner to read \"water\" identifier, got", item.Inspect())
// 	}
// 	if item.Val != "water" {
// 		t.Error("Expected scanner to read \"water\" identifier, got", item.Inspect())
// 	}

// 	if scanner.Scan().Token != Whitespace {
// 		t.Error("Expected whitespace")
// 	}

// 	item = scanner.Scan()
// 	if item.Token != Select {
// 		t.Error("Expected scanner to read SELECT keyword, got", item.Inspect())
// 	}
// 	if item.Val != "SElect" {
// 		t.Error("Expected scanner to read SELECT keyword, got", item.Inspect())
// 	}

// }

// func TestPeek(t *testing.T) {
// 	str := `1234567890 FRoM water SElect`
// 	s := NewScanner(bufio.NewReader(strings.NewReader(str)))
// 	log.Println("peek 1")
// 	if s.peek() != '1' {
// 		t.Error("1. peek failed")
// 	}
// 	log.Println("peek 1")
// 	if s.peek() != '1' {
// 		t.Error("2. peek failed")
// 	}
// 	log.Println("read 1")
// 	if s.read() != '1' {
// 		t.Error("3. read failed")
// 	}
// 	log.Println("peek 2")
// 	if r := s.peek(); r != '2' {
// 		t.Error("4. peek failed, expected 2, got", string(r))
// 	}
// 	log.Println("peek 2")
// 	if r := s.peek(); r != '2' {
// 		t.Error("5. peek failed, expected 2, got", string(r))
// 	}
// 	log.Println("read 2")
// 	if r := s.read(); r != '2' {
// 		t.Error("6. read failed, expected 2, got", string(r))
// 	}
// }
func TestUnion(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `SELECT A from some_table UNION SELECT A2 from some_table2`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if ast == nil {
		t.Error("Expected AST to be set but it was empty")
		t.FailNow()
	}
	outputTarget := `SELECT A
FROM some_table UNION SELECT A2
FROM some_table2`
	if ast.String() != outputTarget {
		t.Error("Unexpected output", ast.String())
		t.Error("target", []byte(outputTarget))
		t.Error("target", []byte(ast.String()))
	}
}
func TestSomething(t *testing.T) {
	var ast Statement
	source := "SELECT EBBSBHTRN.TRANSTYPECODE,EBBSBHTRN.TRANSTYPECODE2,DAUELIMIT,EBBSBHTRN.TRANSTYPECODE5,EBBSBHTRN.TRANSTYPECODE4,CNAPSBENBNKCD,CURRENCYCODE,COUNTRYCODE,CITYNAME,\"-9009\",\"Use this as  ALT to DEF EBBS~BH\" FROM EBBSPRD_BH_ACCTLMT\nLEFT OUTER JOIN EBBSPRD_BH_ACNOM ON ((EBBSPRD_BH_ACCTLMT.EXPIRYDT = EBBSPRD_BH_ACCTLMT.EXPIRYDT) AND (EBBSPRD_BH_ACNOM.POSTALCODE = EBBSPRD_BH_ACNOM.SEQNO))\nFULL OUTER JOIN EBBSPRD_BH_ACNOM ON ((EBBSPRD_BH_ACNOM.ADDRESS1 LIKE EBBSPRD_BH_ACNOM.ACCOUNTNO) OR (EBBSPRD_BH_ACNOM.ADDRESS1 \u003e '666666')) WHERE (EBBSPRD_BH_ACCTLMT.DAUELIMIT = EBBSPRD_BH_ACCTLMT.CURRENCYCODE)"
	err := Parse(&ast, source)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if ast == nil {
		t.Error("Expected AST to be set but it was empty")
		t.FailNow()
	}
	t.Log(ast.String())
}
func TestComplexUnion(t *testing.T) {
	var ast Statement
	err := Parse(&ast, `SELECT "SRC" AS SRC, COUNT(*)
	FROM EBBSPRD_BN_SYSTEM
		LEFT OUTER JOIN EBBSPRD_LK_RELADDR ON ((EBBSPRD_LK_MLITDEL.DEALNO = '1')
		AND (2 = '2'))
		RIGHT OUTER JOIN EBBSPRD_BN_SYSTEM ON (EBBSPRD_LK_MLITDEL.DEALNO = EBBSPRD_LK_MLITDEL.EXPIRYDATE)
	WHERE
		(EBBSPRD_LK_MLITDEL.ODACCOUNTNO = EBBSPRD_LK_MLITDEL.ODACCOUNTNO) UNION SELECT "TGT" AS TGT, COUNT(*)
	FROM LMT_SCTY WHERE PRCS_ID='' AND PROCESS_DATE='@Process_Date' AND DATA_SRC='NA'`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if ast == nil {
		t.Error("Expected AST to be set but it was empty")
		t.FailNow()
	}
	outputTarget := `SELECT "SRC" AS SRC, COUNT(*)
FROM EBBSPRD_BN_SYSTEM
	LEFT OUTER JOIN EBBSPRD_LK_RELADDR ON ((EBBSPRD_LK_MLITDEL.DEALNO = '1')
	AND (2 = '2'))
	RIGHT OUTER JOIN EBBSPRD_BN_SYSTEM ON (EBBSPRD_LK_MLITDEL.DEALNO = EBBSPRD_LK_MLITDEL.EXPIRYDATE)
WHERE
	(EBBSPRD_LK_MLITDEL.ODACCOUNTNO = EBBSPRD_LK_MLITDEL.ODACCOUNTNO) UNION SELECT "TGT" AS TGT, COUNT(*)
FROM LMT_SCTY
WHERE
	PRCS_ID = ''
	AND PROCESS_DATE = '@Process_Date'
	AND DATA_SRC = 'NA'`
	if ast.String() != outputTarget {
		t.Error("Unexpected output", ast.String())
		t.Error("target", []byte(outputTarget))
		t.Error("target", []byte(ast.String()))
	}
}
