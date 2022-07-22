/*                   placeholder                    */

/* ============================================================================
=============================  FEATURE =====================================

Title: Self-reported Travel


============================================================================ */
ALTER SESSION SET current_schema = DATA_SCIENCE;

create table self_reported_travel as
with uniq_ssn as (--32205 distinct ssn
		select distinct 
		ssn, 
		people_id
		from rpt.CATS_CP_PPL_INTC
	)
	,threat as (--------------------------------------------------------------------------------------------------------------------- Quick/dirty table of threat countries from InTO threat list, this can be brought in w/python
		select 'China' as country,1 as rank from dual union
		select 'Russia' as country,2 as rank from dual union
		select 'Iran' as country,3 as rank from dual union
		select 'NorthKorea' as country,4 as rank from dual union
		select 'North Korea' as country,4 as rank from dual union
		select 'Pakistan' as country,5 as rank from dual union
		select 'Venezuela' as country,6 as rank from dual union
		select 'Syria' as country,7 as rank from dual union
		select 'India' as country,8 as rank from dual union
		select 'Turkey' as country,9 as rank from dual union
		select 'SaudiArabia' as country,10 as rank from dual union
		select 'Saudi Arabia' as country,10 as rank from dual union
		select 'Egypt' as country,11 as rank from dual union
		select 'Iraq' as country,12 as rank from dual union
		select 'Ukraine' as country,13 as rank from dual union
		select 'Afghanistan' as country,13 as rank from dual union
		select 'Cuba' as country,14 as rank from dual union
		select 'Mexico' as country,15 as rank from dual 
	)
	,fact_cte as (---------------------------------------------------------------------------------------------------------------------- CATS fact table 
		select 
			ppl.ssn
			,count(ppl.ssn) over (partition by ppl.ssn)  as totl_cnt_ssn
			--,tvl.*
			,tr.rank as threat_rank
			,tvl.people_id
			,tvl.from_month,tvl.from_year,tvl.to_month,tvl.to_year
			,tvl.country_id,co.country,tvl.change_date
			,max(tvl.change_date) over (partition by ppl.ssn) as max_ch_date
			,1 as cats_flag
		from rpt.CATS_CP_FRN_TRVL tvl 
		left join uniq_ssn ppl on tvl.people_id = ppl.people_id
		left join RPT.global_country_lkp co  on tvl.country_id = co.country_id
		left join threat tr on co.raw_xml_value = tr.country
		where to_year >=2017 ----------------------------------------------------------------------------------------------------------- year filter here
			and tvl.pm_end_date is null
	)
	,cats_distinct as (
		select distinct ssn,country,from_month,from_year,to_month,to_year,max_ch_date,cats_flag
		from fact_cte
	)
	,cats_ssn_list as (-------------------------------------------------------------------------------------------------------------- CATS ssn list, 10474 distinct
		select distinct ssn, max_ch_date as cats_max_ch_dt, 'CATS' as d_source 
		from fact_cte
	)
	,srsp772_fact as (---------------------------------------------------------------------------------------------------------------- SRSP fact table
		select
			ft.txtssn
			,ft.txttravelername
			,ft.dtmreportdate
			,ft.dtmdepart
			,ft.dtmreturn
			,count(itin.efolderid) over (partition by itin.efolderid) as legs_of_trip
			,count(ft.txtssn) over (partition by ft.txtssn) as cntSSN
			,count( itin.arrival_date) over (partition by extract (year from itin.arrival_date), ft.txtssn ) as trip_per_yr
			,count( itin.arrival_date) over (partition by extract (year from itin.arrival_date), ft.txtssn, itin.destination_country ) as trip_per_co_yr
			--,itin.*
			,ft.efolderid
			,itin.destination_country
			,itin.destination_city
			,tr.rank as threat_rank
			,itin.itinerary_id
			,min(itin.arrival_date) over (partition by ft.txtssn) as min_SRSP_dt
			,itin.arrival_date
			,itin.departure_date
			,1 as srsp_flag
		from RPT.srsp_fd772_itinerary itin
		left join RPT.srsp_fd772 ft on ft.efolderid = itin.efolderid
		left join threat tr on itin.destination_country = tr.country
		where ft.pm_end_date is null
			and itin.pm_end_date is null
			and extract (YEAR from arrival_date) >= 2017 ----------------------------------------------------------------------------------- year filter here
	)
	,srsp_distinct as (---------------------------------------------------------------------------------------------- This returns distinct country per month, per efolderid
		select 
			distinct txtssn, 
			efolderid,
			destination_country, 
			min(arrival_date) over (partition by efolderid, destination_country) as min_arrival_date,
			extract(month from arrival_date) as srs_from_month, 
			extract(year from arrival_date) as srs_from_yr
			,srsp_flag
		from srsp772_fact 
	)
	,srsp_ssn_list as (-------------------------------------------------------------------------------------------------------------- SRSP ssn list, 30605 distinct
		select distinct txtssn, 'SRSP' as d_source 
		from srsp772_fact
	)
	,master_ssn as (----------------------------------------------------------------------------------------------------------------- master SSN list, 33819 distinct
		select ssn as ssn_master 
		from cats_ssn_list
		union
		select txtssn as ssn_master
		from srsp_ssn_list
	)
	,compare as (---------------------------------------------------------------------------------------------------------- This compare is based on SRSP and joins in CATS
		select 
			distinct ssn_master
			,srs.cntSSN as srsp_ssn_cnt_raw ---------------------------------------------------------------- The count from srsp without any filtering (not really necessary)
			,cat.totl_cnt_ssn as cats_ssn_cnt
			,cat.max_ch_date as cats_max_ch_dt
			,srs.min_SRSP_dt
			,srs2.efolderid
			,srs2.destination_country
			,th.rank as threat_rank
			--,srs2.destination_city--could be included later on, including will break as currently stands.
			,srs2.min_arrival_date
			,srs2.srs_from_month
			,srs2.srs_from_yr
			,cat2.cats_flag
			,CASE   WHEN srs.min_SRSP_dt > cat.max_ch_date THEN 'SF86 not current'
					WHEN cat2.cats_flag = 1 THEN 'Reported in CATS and SRSP'
					WHEN cat2.cats_flag is null and srs2.min_arrival_date < cat.max_ch_date THEN 'Reported in ONLY SRSP-but SHOULD be in CATS'
					WHEN cat2.cats_flag is null THEN 'Reported in ONLY SRSP'
					END as flag_text
		from master_ssn mas
		left join srsp772_fact srs on mas.ssn_master = srs.txtssn
		left join fact_cte cat on mas.ssn_master = cat.ssn
		left join srsp_distinct srs2 on mas.ssn_master = srs2.txtssn  
		left join fact_cte cat2 on mas.ssn_master = cat2.ssn 
			AND srs2.destination_country = cat2.country 
			AND srs2.srs_from_month = cat2.from_month 
			AND srs2.srs_from_yr = cat2.from_year
		left join threat th on srs2.destination_country = th.country
	)
	,compare_part2 as (------------------------------------------------------------------------------------------------- This compare is based on CATS and joins SRSP secondary
		select 
			distinct ssn_master
			,srs.cntSSN as srsp_ssn_cnt_raw ------------------------------------------------------------------ The count from srsp without any filtering (not really necessary)
			,cat.totl_cnt_ssn as cats_ssn_cnt
			,cat.max_ch_date as cats_max_ch_dt
			,srs.min_SRSP_dt
			,catd.country
			,catd.from_month
			,catd.from_year
			,srs2.srs_from_month
			,srs2.srs_from_yr
			,srs2.srsp_flag
			,CASE   WHEN srs.min_SRSP_dt > cat.max_ch_date THEN 'Appears employee not hired yet. SF86 not current'
					WHEN srs2.srsp_flag = 1 THEN 'Reported in CATS and SRSP'
					WHEN srs2.srsp_flag is null and srs.min_SRSP_dt < cat.max_ch_date THEN 'Reported in ONLY CATS'
					END as flag_text
		from master_ssn mas
		left join srsp772_fact srs on mas.ssn_master = srs.txtssn
		left join fact_cte cat on mas.ssn_master = cat.ssn
		left join cats_distinct catd on mas.ssn_master = catd.ssn --and extract (year from srs.min_SRSP_dt) > catd.from_year
		left join srsp_distinct srs2 on mas.ssn_master = srs2.txtssn 
			AND catd.country = srs2.destination_country 
			AND catd.from_month = srs2.srs_from_month 
			AND catd.from_year = srs2.srs_from_yr
	)
	,eod_date as (
		select distinct prsnl_ssn,min(prsnl_eod_date) over (partition by prsnl_ssn) as min_eod
		from rpt.HR_FBI_PERSONNEL
	)
	,union_fiesta as (
		select 
			aa.*
			,cat.cats_flag --as cats_datasrc
			,srs.srsp_flag --as srsp_datasrc 
			/*for the following CASE block: Using 28(day) for compliancey w/all months. Doesn't have to be exact*/
			,CASE 
				WHEN cat.cats_flag is not null 
					and srs.srsp_flag is null 
					and to_date(28||'/'||aa.from_month||'/'||aa.from_year, 'DD/MM/YY') < coalesce (eod.min_eod,aa.min_srsp_dt) -- Captures travel dates recorded in cats BEFORE employee was hired by FBI,this is NOT an issue 
						THEN 'Recorded in CATS, prior to hire date' -- old message: 'Appears SF-86 record prior to hire date'
				WHEN srs.srsp_flag is not null 
					and cat.cats_flag is null 
					and to_date(28||'/'||aa.from_month||'/'||aa.from_year, 'DD/MM/YY') > aa.cats_max_ch_dt --Captures travel that has occured AFTER the most recently submitted SF86, this is NOT an issue
						THEN 'Recorded in SRSP post-updated SF86'      
				WHEN  cats_and_srsp_datasrc is not null --Captures travel that is recorded in both data sets, this is GOOD.
						THEN 'Recorded in both CATS/SRSP'
				WHEN cat.cats_flag is not null 
					and srs.srsp_flag is null 
					and to_date(28||'/'||aa.from_month||'/'||aa.from_year, 'DD/MM/YY') > aa.min_srsp_dt --Captures travel entered in CATS while employee is actively employed but NOT entered in SRSP, this is BAD.
						THEN 'Recorded in CATS, NOT in SRSP'      
				WHEN srs.srsp_flag is not null 
					and cat.cats_flag is null 
					and aa.from_year <= extract(year from aa.cats_max_ch_dt)--Captures travel entered in SRSP while employee is actively employed but NOT entered in CATS, this is BAD.
						THEN 'Recorded in SRSP, NOT in CATS '   
				WHEN aa.min_srsp_dt is null -- Indicates NO records input into SRSP for this person
						THEN 'Recorded in CATS, this SSN has no records in SRSP'
				WHEN aa.cats_max_ch_dt is null -- Indicates NO records input into CATS for this person, this doesn't make sense. Everyone should have completed SF86
						THEN 'Recorded in SRSP, this SSN has no records in CATS'
			END as record_description
			/*for the following CASE block: Using 28(day) for compliancey w/all months. Doesn't have to be exact*/
			,CASE
				WHEN cat.cats_flag is not null and srs.srsp_flag is null and to_date(28||'/'||aa.from_month||'/'||aa.from_year, 'DD/MM/YY') > eod.min_eod 
						THEN 1
				WHEN srs.srsp_flag is not null and cat.cats_flag is null and to_date(28||'/'||aa.from_month||'/'||aa.from_year, 'DD/MM/YY') < aa.cats_max_ch_dt
						THEN 1
				WHEN srs.srsp_flag is not null and cat.cats_flag is null and to_date(28||'/'||aa.from_month||'/'||aa.from_year, 'DD/MM/YY') > aa.cats_max_ch_dt
						THEN 0
				WHEN cat.cats_flag is not null and srs.srsp_flag is null and to_date(28||'/'||aa.from_month||'/'||aa.from_year, 'DD/MM/YY') > eod.min_eod  
						THEN 1
				ELSE 0
			END as discrepancy_record
			,count(aa.ssn_master) over (partition by aa.ssn_master) as ssn_total_records
			,eod.min_eod
		from (
			select 
			ssn_master,
			cats_max_ch_dt,
			min_srsp_dt,
			destination_country as country,
			srs_from_month as        from_month,
			srs_from_yr as           from_year,
			cats_flag as    cats_and_srsp_datasrc
			from compare
			where min_SRSP_dt is not null
			UNION
			select 
			ssn_master, cats_max_ch_dt, min_srsp_dt,                        country,CAST (from_month as INT)as from_month,CAST (from_year as INT) as from_year,  srsp_flag as  cats_and_srsp_datasrc
			from compare_part2 where cats_max_ch_dt is not null
		) aa
		left join cats_distinct cat on aa.ssn_master = cat.ssn 
			AND aa. from_month = cat.from_month 
			AND aa.from_year = cat.from_year 
			AND aa.country = cat.country 
			AND cats_and_srsp_datasrc is null
		left join srsp_distinct srs on aa.ssn_master = srs.txtssn 
			AND aa.from_month = srs.srs_from_month 
			AND aa.from_year = srs.srs_from_yr 
			AND aa.country = srs.destination_country 
			AND cats_and_srsp_datasrc is null
		left join eod_date eod on aa.ssn_master = eod.prsnl_ssn
	)
select 
	aa.*
	,sum(aa.discrepancy_record) over(partition by ssn_master) as bad_records
from union_fiesta aa
order by ssn_master, from_year, from_month;

