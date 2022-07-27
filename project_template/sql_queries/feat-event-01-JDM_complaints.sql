/*                 placeholder                      */

/* ============================================================================
=============================  FEATURE =====================================

Title: JDM Complaints


============================================================================ */
ALTER SESSION SET current_schema = DATA_SCIENCE;

CREATE TABLE jdm_complaints AS
	SELECT distinct 
		d.employeemetadataid,
		d.cmsfullname,
		d.cmsssan,
		e.ssn,
		d.cmslastname,
		d.cmsfirstname,
		d.cmsmiddlename,
		to_date(to_char(cast(d.cmsdob as date), 'DD-MON-YY')) as cmsdob,
		to_date(to_char(cast(c.datereceivedinipu as date), 'DD-MON-YY')) as datereceived, 
		to_date(to_char(cast(c.dateopened as date), 'DD-MON-YY')) as dateopened,
		to_date(to_char(cast(c.dateclosed as date), 'DD-MON-YY')) as dateclosed,
		c.DOJOIGNUMBER,
		c.ID AS COMPLAINT_ID,
		C.TIER,
		F.CASE_ID,
		X.CASEFILENUMBER,
		X.CRIMINAL,
		X.CMSCOMPLEXITY,
		G.VALUE
	FROM javelin_disciplinary.SUBJECT D
	JOIN javelin_disciplinary.COMPLAINT_SUBJECT B ON D.ID=B.SUBJECT_ID
	JOIN javelin_disciplinary.COMPLAINT C ON B.COMPLAINT_ID=C.ID
	LEFT JOIN spear_user.person_search E ON D.EMPLOYEEMETADATAID = E.ENTITY_ID
	LEFT JOIN javelin_disciplinary.COMPLAINT_CASE F ON C.ID=F.COMPLAINT_ID
	LEFT JOIN javelin_disciplinary."CASE" X ON F.CASE_ID=X.ID
	LEFT JOIN javelin_disciplinary.CASETYPELKP G ON X.CASETYPE=G.ID
;

GRANT select ON jdm_complaints TO data_science_role;
