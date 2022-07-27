sql_em_fund = '''
CREATE TABLE EMERGENCY_FUND AS
select 
	phx.ueid
	,id.year
	,efund.bankbalance as bank_balance
	,efund.estimated_monthly_expenses
	,efund.bankbalance / efund.estimated_monthly_expenses as liquidity_ratio
	,efund.mortgagepayment
	,efund.autoloanpayment
	,efund.otherloanpayment
	,efund.liabilitypayment
	,efund.carleasepayment
	,efund.rentpayment
from data_science.support_fdp_locator id 
INNER JOIN (select ssn,
            ueid
            from rpt.phx_person_all_v
            where pm_end_date is null
            and ueid is not null) phx ON phx.ssn = id.ssn
LEFT JOIN (
	SELECT 
	PrimID as primary_id
	,(1.5*mortgagepayment + 1.15*rentpayment + 1.5*(carleasepayment + autoloanpayment) + 900 + otherloanpayment + liabilitypayment) as estimated_monthly_expenses
	,bankbalance
	,mortgagepayment
	,autoloanpayment
	,otherloanpayment
	,liabilitypayment
	,carleasepayment
	,rentpayment
	FROM (
		SELECT 
			NVL(A.ID, -1) AS PrimID
			,NVL(BankSum, 0) AS BankBalance
			,NVL(MortgageSum, 0) AS MortgagePayment
			,NVL(AutoLoanSum, 0) AS AutoLoanPayment
			,NVL(MiscLoanSum, 0) AS OtherLoanPayment
			,NVL(LiabilitySum, 0) AS LiabilityPayment
			,NVL(CarPaymentSum, 0) AS CarLeasePayment
			,NVL(RentSum, 0) AS RentPayment 
		FROM (
			(SELECT PRIMARY_ID AS ID, SUM(YE_BALANCE) AS BankSum 
			FROM RPT.FDF_BANK_ACCOUNTS
			WHERE PM_END_DATE IS NULL
			GROUP BY PRIMARY_ID) A
			FULL OUTER JOIN (
				SELECT PRIMARY_ID AS ID, SUM(MONTHLY_PAYMENT) AS MortgageSum FROM RPT.FDF_LOANS
				WHERE PM_END_DATE IS NULL AND LOAN_TYPE IN ('MORTGAGE', '2ND_MORTGAGE', '3RD_MORTGAGE')
				GROUP BY PRIMARY_ID) B
				ON A.ID = B.ID
			FULL OUTER JOIN (
				SELECT PRIMARY_ID AS ID, SUM(MONTHLY_PAYMENT) AS AutoLoanSum FROM RPT.FDF_LOANS
				WHERE PM_END_DATE IS NULL AND LOAN_TYPE = 'AUTO'
				GROUP BY PRIMARY_ID) C
				ON A.ID = C.ID
			FULL OUTER JOIN (
				SELECT PRIMARY_ID AS ID, SUM(MONTHLY_PAYMENT) AS MiscLoanSum FROM RPT.FDF_LOANS
				WHERE PM_END_DATE IS NULL AND LOAN_TYPE NOT IN ('MORTGAGE', '2ND_MORTGAGE', '3RD_MORTGAGE', 'AUTO')
				GROUP BY PRIMARY_ID) D
				ON A.ID = D.ID
			FULL OUTER JOIN (
				SELECT PRIMARY_ID AS ID, SUM(MONTHLY_PAYMENT) AS LiabilitySum FROM RPT.FDF_LIABILITY
				WHERE PM_END_DATE IS NULL
				GROUP BY PRIMARY_ID) E
				ON A.ID = E.ID
			FULL OUTER JOIN (
				SELECT PRIMARY_ID AS ID, SUM(MONTHLY_PAYMENT) AS CarPaymentSum FROM RPT.FDF_LEASED_VEHICLES
				WHERE PM_END_DATE IS NULL
				GROUP BY PRIMARY_ID) F
				ON A.ID = F.ID
			FULL OUTER JOIN (
				SELECT PRIMARY_ID AS ID, SUM(MONTHLY_PAYMENT) AS RentSum FROM RPT.FDF_LEASED_REAL_ESTATE
				WHERE PM_END_DATE IS NULL
				GROUP BY PRIMARY_ID) G
				ON A.ID = G.ID
		)
	)
	WHERE PrimID <> -1
) efund on efund.primary_id = id.primary_id
order by phx.ueid, id.year
;'''

jdm_complaints = '''CREATE TABLE jdm_complaints AS
	SELECT distinct 
		d.employeemetadataid, d.cmsfullname, d.cmsssan, e.ssn, d.cmslastname, d.cmsfirstname, d.cmsmiddlename, 
		to_date(to_char(cast(d.cmsdob as date), 'DD-MON-YY')) as cmsdob,
		to_date(to_char(cast(c.datereceivedinipu as date), 'DD-MON-YY')) as datereceived, 
		to_date(to_char(cast(c.dateopened as date), 'DD-MON-YY')) as dateopened,
		to_date(to_char(cast(c.dateclosed as date), 'DD-MON-YY')) as dateclosed,
		c.DOJOIGNUMBER, c.ID AS COMPLAINT_ID, C.TIER, F.CASE_ID,
		X.CASEFILENUMBER, X.CRIMINAL, X.CMSCOMPLEXITY, G.VALUE
	FROM javelin_disciplinary.SUBJECT D
	JOIN javelin_disciplinary.COMPLAINT_SUBJECT B ON D.ID=B.SUBJECT_ID
	JOIN javelin_disciplinary.COMPLAINT C ON B.COMPLAINT_ID=C.ID
	LEFT JOIN spear_user.person_search E ON D.EMPLOYEEMETADATAID = E.ENTITY_ID
	LEFT JOIN javelin_disciplinary.COMPLAINT_CASE F ON C.ID=F.COMPLAINT_ID
	LEFT JOIN javelin_disciplinary."CASE" X ON F.CASE_ID=X.ID
	LEFT JOIN javelin_disciplinary.CASETYPELKP G ON X.CASETYPE=G.ID
;'''