/*                 placeholder                      */

/* ============================================================================
=============================  FEATURE =====================================

Title: JDM Cases


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
