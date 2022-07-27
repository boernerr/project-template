CREATE TABLE PHX_P_SECALT_fct_tbl as
with cte_separate
        as (
        select
        distinct ssn, person_status_id, pm_begin_date,
        count(ssn) over (partition by ssn) cnt_ssn,
        max(pm_begin_date) over (partition by ssn) max_begin_date
        from rpt.phx_p_prsn
        where person_status_id != 1
        order by cnt_ssn desc, ssn, pm_begin_date
        )
,cte_main
        as (
        select
        count(sec.subj_person_id) over (partition by sec.subj_person_id) cnt_ssn,
        count(distinct sec.pk_id) over (partition by prs.ssn) cnt_sec_alerts,-- the count of distinct security alerts per person.
        prs.first_name,
        prs.last_name,
        prs.ssn,
        COALESCE (dutd.cease_duty_date,sep.separation_dt) cease_Duty_Date,
        CASE when sts.title is null
                then 'Active' else sts.title
        END as current_prsn_sts, -- based on PHX_P_PRSN
        pact.Title as secalt_act, -- based on PHX_P_SECALT, this is the 'action' taken
        acat.title as secalt_category, -- category of security alert
        atyp.title as type_title, --type of alert.
        to_char(regexp_substr(description, '[A-Z0-9]*-HQ-[A-Z0-9]*')) case_File,
        sec.*,
        SYSDATE as extract_date
        from (select * from rpt.PHX_P_SECALT) sec
                left join (select distinct first_name, last_name, pk_id, ssn /*,date_of_birth ,person_status_id*/ from rpt.phx_p_prsn ) prs on sec.subj_person_id = prs.pk_id
                    left join(select distinct ssn, pm_begin_date as separation_dt from cte_separate) sep on prs.ssn = sep.ssn
                left join (select distinct pk_id, cease_duty_date from rpt.phx_p_prsn where cease_duty_date is not null ) dutd on sec.subj_person_id = dutd.pk_id/*Have to include this in own join because if including in above, records are duplicated.*/
                left join (select distinct pk_id, person_status_id from rpt.phx_p_prsn where person_status_id !=1) psts on sec.subj_person_id = psts.pk_id -- This join brings in 12 duplicate records. need to invest later.
                    left join rpt.PHX_P_PRSN_STS sts on psts.person_status_id = sts.pk_id
                left join rpt.PHX_P_SECALT_PRSN_ACT pact on sec.sec_alert_person_act_id = pact.pk_id
                left join rpt.PHX_P_SECALT_CAT acat on sec.sec_alert_cat_id = acat.pk_id
                left join rpt.PHX_P_SECALT_TYP atyp on sec.sec_alert_type_id = atyp.pk_id
        )
select * from cte_main;