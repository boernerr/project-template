/*                 UNCLASSIFIED//FOR OFFICIAL USE ONLY                       */

/* ============================================================================
============================= (U) FEATURE =====================================

(U//FOUO) Title: JDM Cases
(U) Version: 1
(U//FOUO) Author: Erikson Arcaira
(U) Description: 
	(U//FOUO) This table creates the CASE population based on the Javelin 
		Disciplinary tables. It identifies the subject of a case (2004 to 
		current) a unique caseid, the dates a case was opened, if and when it 
		was transferred to OPR, and when it was adjudicated. 

(U) Data Source
---------------
(U//FOUO) Original data source(s): Javelin_Disciplinary (JDM) Case and Subject 
	tables, Spear_User (for entity resolution) 
(U//FOUO) Data source info: 
(U//FOUO) Data classification: SECRET//NOFORN
(U) Data as of: 2022-05-19

(U) Feature info
----------------
(U//FOUO) Join key: SSN
(U//FOUO) Fields:

	TODO
	
(U) Usage:
	(U//FOUO) A precursor to the NON_DEROG feature table.

(U) Limitations:
	(U) TODO

============================================================================ */
ALTER SESSION SET current_schema = DATA_SCIENCE;

CREATE TABLE jdm_cases as
SELECT distinct d.employeemetadataid, d.cmsfullname, d.cmsssan, e.ssn, d.cmslastname, d.cmsfirstname, d.cmsmiddlename, to_date(to_char(cast(d.cmsdob as date), 'dd-mon-yy')) as cmsdob,
to_date(to_char(cast(c.dateopened as date), 'dd-mon-yy')) as casedateopened, 
to_date(to_char(cast(c.datetransferredtoopr as date), 'dd-mon-yy')) as caseto_opr, to_date(to_char(cast(c.adjudicationcomplete as date), 'dd-mon-yy')) as caseadjcomplete, 
a.caseid
FROM javelin_disciplinary.casesubjectmetadata a
JOIN javelin_disciplinary.subject d on a.subjectid=d.id
JOIN javelin_disciplinary.case c on a.caseid=c.id
JOIN spear_user.person_search e on d.employeemetadataid = e.entity_id;

GRANT select ON jdm_cases TO data_science_role;
