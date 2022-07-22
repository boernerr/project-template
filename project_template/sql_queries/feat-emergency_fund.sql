/*             placeholder  */

/* ============================================================================
============================= (U) FEATURE =====================================

Emergency Fund
Monthly expenses are currently estimated as:
		1.5 x mortgage
		1.15 x rent
		1.5 x car lease and auto loan
		other loans
		liabilities (including credit cards)
		$900 additional for living expenses
	
	It is reasonable that these calculations should be evaluated and updated
	as times change. Multipliers simulate the operating and maintenance costs
	of the underlying assets, and are based on internet research of the U.S.
	population as of 2019.


============================================================================ */
ALTER SESSION SET current_schema = DATA_SCIENCE;

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
INNER JOIN (select ssn, ueid from rpt.phx_person_all_v where pm_end_date is null and ueid is not null) phx ON phx.ssn = id.ssn
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
;
