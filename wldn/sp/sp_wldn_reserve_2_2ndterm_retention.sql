CREATE OR REPLACE PROCEDURE `trans_crm_mart.sp_wldn_reserve_2_2ndterm_retention`(IN v_audit_key STRING, OUT result STRING)
begin

    declare institution string default 'WLDN';
    declare institution_id int64 default 5;
    declare dml_mode string default 'delete-insert';
    declare target_dataset string default 'rpt_crm_mart';
    declare target_tablename string default 't_wldn_reserve_2_2ndterm_retention';
    declare source_tablename string default 'reserve_2_2ndterm_retention';
    declare load_source string default 'trans_crm_mart.sp_wldn_reserve_2_2ndterm_retention';
    declare additional_attributes ARRAY<struct<keyword string, value string>>;
    declare last_refresh_time timestamp;
    declare tgt_table_count int64;

    /* common across */
    declare job_start_dt timestamp default current_timestamp();
    declare job_end_dt timestamp default current_timestamp();
    declare job_completed_ind string default null;
    declare job_type string default 'DS';
    declare load_method string default 'scheduled query';
    declare out_sql string;
    BEGIN
    SET additional_attributes= [("audit_load_key", v_audit_key),
              ("load_method",load_method),
              ("load_source",load_source),
              ("job_type", job_type)];
    /* end common across */

---Start of Table---

create temp table base_2 as
with base AS (
  SELECT *
  FROM (
    SELECT *
    FROM (
      SELECT List.*
        , case when concat(List.Applicant_id,List.Student_Start_Date) in (select distinct concat(student_id,start_date)from `rpt_academics.t_wldn_reconciled_list_cube`) then 1 else 0 end as Reconciled_Student
        ,Forecast.channel_c as Channel_Desc
        , Forecast.first_term as Start_Term_Cd
        , d.ACADEMIC_PERIOD AS Second_Term_Cd
        , IF(d.ACADEMIC_PERIOD IS NOT NULL, 1, 0) AS Second_Term_Retained
      FROM `trans_crm_mart.v_wldn_cur_reser_lst_list` List
      LEFT JOIN (
          SELECT distinct o.banner_id_c, channel_c,program_start_date_term_c as first_term,s.start_date_c FROM
          `raw_b2c_sfdc.opportunity` o
      left join `raw_b2c_sfdc.start_date_c` s  on s.id=o.selected_program_start_date_c
      where o.is_deleted=false and o.institution_c='a0ko0000002BSH4AAO') Forecast
      ON List.Applicant_id = Forecast.banner_id_c
      --   AND List.Report_Date = Forecast.Report_Date
      AND List.Student_Start_Date = Forecast.start_date_c
      INNER JOIN `trans_crm_mart.v_wldn_cur_reser_lst_max_date` AS Wal_Cur_Reser_Lst_Max_Date ON List.Report_Date = Wal_Cur_Reser_Lst_Max_Date.Report_Date
        AND List.Student_Start_Date = Wal_Cur_Reser_Lst_Max_Date.Student_Start_Date
      LEFT JOIN (select * from `rpt_academics.v_wldn_rev_gen_student_term_list`

      --where case when concat(id,academic_period) in (select distinct concat(student_id,term_cd)from `rpt_academics.t_wldn_reconciled_list_cube`) then 1 else 0 end = 1
      ) AS d
        ON List.Applicant_ID = d.id
        AND (
          CASE
            WHEN RIGHT(Forecast.first_term, 2) = '70' AND d.ACADEMIC_PERIOD = CAST((CAST(Forecast.first_term AS INT) + 40) AS STRING)
              THEN 1
            WHEN RIGHT(Forecast.first_term, 2) = '60' AND d.ACADEMIC_PERIOD = CAST((CAST(Forecast.first_term AS INT) + 60) AS STRING)
              THEN 1
            WHEN RIGHT(Forecast.first_term, 2) IN ('10', '20', '30', '40', '50') AND d.ACADEMIC_PERIOD = CAST((CAST(Forecast.first_term AS INT) + 20) AS STRING)
              THEN 1
            ELSE 0
          END=1
        )
    )
    WHERE
      Student_Start_Date>='2022-01-10'
  )
),
temp as (
  select distinct a.*, b.cum_credits_toc as TOC
  ,row_number() over(partition by Student_Start_Date, Applicant_ID order by Report_Date desc,Start_Term_Cd desc, cum_credits_toc desc) as rid
from base a
left join rpt_academics.v_wldn_gpa_prg_term b on a.Applicant_ID=b.id
	and b.cum_credits_toc > 0
	and (a.term_cd=b.term
			OR
		(case when substr(a.term_cd,5,2)='70' and b.term=cast(CAST(term_cd AS INT64) + 40 as string) then 1
              when substr(a.term_cd,5,2)='60' and b.term=cast(CAST(term_cd AS INT64) + 60 as string) then 1
              when substr(a.term_cd,5,2) in ('10','20','30','40','50') and b.term=cast(CAST(term_cd AS INT64) + 20 as string) then 1 else 0 end = 1 )
		 	OR
		(case when substr(a.term_cd,5,2)='10' and b.term=cast(CAST(term_cd AS INT64) - 40 as string) then 1
              when substr(a.term_cd,5,2)='20' and b.term=cast(CAST(term_cd AS INT64) - 60 as string) then 1
              when substr(a.term_cd,5,2) in ('30','40','50','60','70') and b.term=cast(CAST(term_cd AS INT64) - 20 as string) then 1 else 0 end = 1 )
		)
)
  select *except(rid) from temp
  where rid=1
;
create temp table base_4 as
with crs1st AS (
  SELECT a.person_uid
    ,a.credential_id
    , a.academic_period
    , a.course_identification
    , a.course_reference_number
    , DATE_TRUNC(a.start_date, DAY) AS course_start_date
    , DATE_TRUNC(a.end_date, DAY) AS course_end_date
    , a.Final_Grade
    , b.Degree_Level
    ,case when final_grade = ' ' then null
	when degree_level in ('BS')
		and (final_grade like 'A%'
			or final_grade like 'B%'
			or final_grade like 'C%'
			or final_grade like 'D%'
			or final_grade like 'P%'
			or final_grade like 'S%') then 1
	when degree_level in ('CERT','MS','PHD')
		and (final_grade like 'A%'
			or final_grade like 'B%'
			or final_grade like 'C%'
			or final_grade like 'P%'
			or final_grade like 'S%') then 1 else 0 end as passed_1st_course
  FROM `rpt_academics.t_student_course` AS a
  RIGHT JOIN base_2 AS b
    ON a.credential_id = b.Applicant_ID
    AND a.academic_period = b.Start_Term_Cd
    AND DATE_TRUNC(a.start_date, DAY) = b.Student_Start_Date
  WHERE a.registration_status IN ('RE', 'RW')
  AND (a.GRADABLE_IND = 'Y' OR a.GRADABLE_IND IS NULL)
  AND a.subject NOT IN ('WSRO', 'WWOW', 'RESI')
  AND (a.final_grade NOT IN ('TR', 'PR') OR a.final_grade IS NULL)
  and final_grade not in ('NC','NP','AU','RC')
),
crs1st_summarize as (
select credential_id, academic_period, max(passed_1st_course) as passed_1st_crs
from crs1st
group by credential_id, academic_period
),
crs2nd as (
select b.credential_id, b.academic_period, b.course_identification, b.course_reference_number
	, date(b.start_date) as course_start_date
	, date(b.end_date) as course_end_date
	, date(b.Last_Attend_Date) as Last_Attend_Date
from base_2 a
left join `rpt_academics.t_student_course`  b
on a.Applicant_ID=b.credential_id
	and a.Second_Term_Cd=b.academic_period
where b.registration_status in ('RE','RW')
	and b.subject not in ('WSRO','WWOW','RESI')
	and (b.final_grade not in ('TR','PR') or b.final_grade is NULL)
order by b.credential_id, b.academic_period, b.course_identification
),

crs2nd_1 as (
  select a.*, b.activity_date as Last_Activity_Date, b.ACTIVITY_MINUTES
from crs2nd a
left join `trans_crm_mart.bb_activity` b
on a.credential_id=b.ID
	and a.academic_period=b.term
	and a.course_identification=b.course_identification
	and a.course_reference_number=b.crn
	and a.course_start_date=b.course_start_date
	and a.course_end_date=b.course_end_date
),
crs2nd_1temp as (
  select *,row_number() over(partition by credential_id, ACADEMIC_PERIOD, course_identification, course_reference_number, course_start_date order by course_end_date desc, Last_Attend_Date desc,Last_Activity_Date desc) as rid
  from crs2nd_1
),
crs2nd_2 as (
  select *except(rid),
  case when Last_Activity_Date is not null then 1 else 0 end as participating_2nd_crs,
  case when Last_Activity_Date >= Course_Start_Date + 7 then 1 else 0 end as participating_2nd_crs_after1Wk from crs2nd_1temp
  where rid=1
),
crs2nd_summarize as (
select credential_id, academic_period, max(participating_2nd_crs) as participating_2nd_crs, max(participating_2nd_crs_after1Wk) as participating_2nd_crs_after1Wk
from crs2nd_2
group by credential_id, academic_period
),
crs_parti as (
select coalesce(b.credential_id,a.Applicant_ID) as ID
	, coalesce(b.academic_period, a.Start_Term_Cd) as academic_period
	, a.Student_Start_Date
	, b.registration_status
	, b.course_identification, b.course_reference_number
	, date(b.start_date) as course_start_date
	, date(b.end_date) as course_end_date
	, date(b.Last_Attend_Date) as Last_Attend_Date
	, b.Final_Grade, a.Degree_Level
  ,case when Last_Attend_Date is null then null
  when Last_Attend_Date >= date(b.start_date) + (date(b.end_date)-date(b.start_date))/2 then 1
  when Last_Attend_Date <  date(b.start_date) + (date(b.end_date)-date(b.start_date))/2 then 0 end as parti_half_1st_crs
from base_2 a
left join `rpt_academics.t_student_course` b on a.Applicant_ID=b.credential_id
			and a.Start_Term_Cd=b.academic_period
			and a.Student_Start_Date=date(b.start_date)
where a.Reconciled_Student=1
	and b.subject not in ('WSRO','WWOW','RESI')
	and (b.final_grade not in ('TR','PR') or b.final_grade is NULL)
  and registration_status not in ('DD','DR','DU','FC','FN')
  and final_grade not in ('NC','NP','AU','RC')
),
crs_parti_summarize as (
select ID, academic_period, max(parti_half_1st_crs) as parti_half_1st_crs
from crs_parti
group by ID, academic_period
 ),
wd_crs as (
select a.credential_id, a.academic_period, a.course_identification, a.course_reference_number
	, date(a.start_date) as course_start_date
	, date(a.end_date) as course_end_date
	, a.Final_Grade, b.Degree_Level
from `rpt_academics.t_student_course` a
right join base_2 b on a.credential_id=b.Applicant_ID
		and a.academic_period=b.Start_Term_Cd
		and date(a.start_date)=b.Student_Start_Date
where a.registration_status in ('WD','WI','WM','WN','WO','WT','W2','W3','A2','A3')
	and a.subject not in ('WSRO','WWOW','RESI')
	and (a.final_grade not in ('TR','PR') or a.final_grade is NULL)
),
still_re as (
select distinct a.credential_id
from `rpt_academics.t_student_course` a
right join wd_crs b on a.credential_id=b.credential_id
		and a.academic_period=b.academic_period
		and date(a.start_date)=b.course_start_date
where a.registration_status in ('RE','RW')
	and a.subject not in ('WSRO','WWOW','RESI')
	and (a.final_grade not in ('TR','PR') or a.final_grade is NULL)
),
wd_crs1 as (
select a.*
from wd_crs a
left join still_re b on a.credential_id=b.credential_id
where b.credential_id is NULL
),
wd_crs_summarize_temp as (
select distinct credential_id, academic_period, 1 as wd_1st_crs
from wd_crs1
),
wd_univ as (
select distinct p.Credential_ID as ID, a.academic_period as academic_period, date(a.effective_date) as Withdrawal_Effective_Date
from `rpt_academics.v_withdrawal` a
left join `rpt_academics.t_person` p on p.person_uid=a.person_uid
right join base_2 b on p.Credential_ID=b.Applicant_ID
		and a.academic_period=b.Start_Term_Cd
where a.withdrawal_code in ('AW','SW')
	and a.institution_id=5
),
wd_univ_summarize as (
select distinct ID, academic_period, 1 as wd_univ
from wd_univ
),
wd_crs_summarize as (
select a.*, b.wd_univ
from wd_crs_summarize_temp a
left join wd_univ_summarize b on a.Credential_ID=b.ID and a.academic_period=b.academic_period
where wd_1st_crs<>1 and wd_univ<>1
),
fail_all_summarize_temp as (
select Credential_ID, academic_period, max(passed_1st_course) as max_passed_1st_course, min(passed_1st_course) as min_passed_1st_course
from crs1st
group by Credential_ID, academic_period
),
fail_all_summarize as (
select Credential_ID, academic_period,case when max_passed_1st_course=min_passed_1st_course and min_passed_1st_course=0 then 1 end as failed_1st_crs from fail_all_summarize_temp
),
base_3 as (
select a.*
	, IFNULL(b.passed_1st_crs, 0) passed_1st_crs, IFNULL(c.participating_2nd_crs, 0)participating_2nd_crs, IFNULL(c.participating_2nd_crs_after1Wk, 0) participating_2nd_crs_after1Wk, IFNULL(d.parti_half_1st_crs, 0) parti_half_1st_crs
	,IFNULL(e.wd_1st_crs, 0) wd_1st_crs, IFNULL(f.wd_univ, 0)wd_univ, IFNULL(g.failed_1st_crs, 0)failed_1st_crs
from base_2 a
left join crs1st_summarize b on a.Applicant_ID=b.Credential_ID and a.Start_Term_Cd=b.academic_period
left join crs2nd_summarize c on a.Applicant_ID=c.Credential_ID and a.Second_Term_Cd=c.academic_period
left join crs_parti_summarize d on a.Applicant_ID=d.id and a.Start_Term_Cd=d.academic_period
left join wd_crs_summarize e on a.Applicant_ID=e.Credential_ID and a.Start_Term_Cd=e.academic_period
left join wd_univ_summarize f on a.Applicant_ID=f.id and a.Start_Term_Cd=f.academic_period
left join fail_all_summarize g on a.Applicant_ID=g.Credential_ID and a.Start_Term_Cd=g.academic_period
)
select * from base_3;
/*
,base_4 as (
  SELECT
  (passed_1st_crs - mean_passed_1st_crs) / stddev_passed_1st_crs AS standardized_passed_1st_crs,
  (participating_2nd_crs - mean_participating_2nd_crs) / stddev_participating_2nd_crs AS standardized_participating_2nd_crs,
  (participating_2nd_crs_after1Wk - mean_participating_2nd_crs_after1Wk) / stddev_participating_2nd_crs_after1Wk AS standardized_participating_2nd_crs_after1Wk,
  (parti_half_1st_crs - mean_parti_half_1st_crs) / stddev_parti_half_1st_crs AS standardized_parti_half_1st_crs,
  (wd_1st_crs - mean_wd_1st_crs) / stddev_wd_1st_crs AS standardized_wd_1st_crs,
  (wd_univ - mean_wd_univ) / stddev_wd_univ AS standardized_wd_univ,
  (failed_1st_crs - mean_failed_1st_crs) / stddev_failed_1st_crs AS standardized_failed_1st_crs
FROM
  base_3,
  (SELECT
    AVG(passed_1st_crs) AS mean_passed_1st_crs,
    STDDEV(passed_1st_crs) AS stddev_passed_1st_crs,
    AVG(participating_2nd_crs) AS mean_participating_2nd_crs,
    STDDEV(participating_2nd_crs) AS stddev_participating_2nd_crs,
    AVG(participating_2nd_crs_after1Wk) AS mean_participating_2nd_crs_after1Wk,
    STDDEV(participating_2nd_crs_after1Wk) AS stddev_participating_2nd_crs_after1Wk,
    AVG(parti_half_1st_crs) AS mean_parti_half_1st_crs,
    STDDEV(parti_half_1st_crs) AS stddev_parti_half_1st_crs,
    AVG(wd_1st_crs) AS mean_wd_1st_crs,
    STDDEV(wd_1st_crs) AS stddev_wd_1st_crs,
    AVG(wd_univ) AS mean_wd_univ,
    STDDEV(wd_univ) AS stddev_wd_univ,
    AVG(failed_1st_crs) AS mean_failed_1st_crs,
    STDDEV(failed_1st_crs) AS stddev_failed_1st_crs
  FROM
    base_3)
    )*/
create temp table reserve_2_2ndterm_retention as
with base_5 as (
select a.*, b.AA_Name, b.AA_Location, b.AA_Manager
from base_4 a
left join rpt_crm_mart.v_wldn_map_ea_aa b on a.Applicant_ID=b.ID
)
,sro as (
select distinct a.Credential_ID, a.academic_period, a.course_identification, a.start_date, a.Last_Attend_Date
from `rpt_academics.t_student_course` a, base_5 b
where a.Credential_ID=b.Applicant_ID and a.academic_period=b.Start_Term_Cd and a.SUBJECT='WSRO' and a.registration_Status in ('RE','RW')
and Last_Attend_Date is not null
)
,sro2 as (
select distinct Credential_ID, academic_period
from sro
)
,ret_ind as (
SELECT *
FROM `trans_crm_mart.v_wldn_ret_ind_for_qtr`
UNION all
SELECT *
FROM `trans_crm_mart.v_wldn_ret_ind_for_sem`
)
,src as (
select distinct 'WLDN' AS institution
,5 AS institution_id
,'WLDN_SF' as source_system_name
,a.*except(aa_location,ea_location),

case when EA_Location='Baltimore' then 'MidAtlantic'
	when EA_Location='Columbia' then 'MidAtlantic'
	when EA_Location='Baltimore International' then 'Baltimore-International'
	when EA_Location='Phoenix' then 'Tempe' else EA_Location end as ea_location,
  case when AA_Location='MNPLS' then 'Minneapolis' else AA_Location end as aa_location
, case when b.Credential_ID is NULL then false else true end as SRO_Attend, c.ret_ind
from base_5 a
left join sro2 b on a.Applicant_ID=b.Credential_ID and a.Start_Term_Cd=b.academic_period
left join ret_ind c on a.Applicant_ID=c.Credential_ID and a.Student_Start_Date=date(c.start_date)
)

---End of Table---
SELECT
    src.*,
    job_start_dt as etl_created_date,
    job_start_dt as etl_updated_date,
    load_source as etl_resource_name,
    v_audit_key as etl_ins_audit_key,
    v_audit_key as etl_upd_audit_key,
    farm_fingerprint(format('%T', concat(src.oppsfid))) AS etl_pk_hash,
    farm_fingerprint(format('%T', src )) as etl_chg_hash,
    FROM src
    union all
    select * from `trans_crm_mart.t_wldn_reserve_2_2ndterm_retention_history`
	;
-- merge process
CALL utility.sp_process_elt (institution, dml_mode , target_dataset, target_tablename, null, source_tablename, additional_attributes, out_sql );
SET job_end_dt = current_timestamp();
SET job_completed_ind = 'Y';
-- export success audit log record
CALL `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);
SET result = 'SUCCESS';
EXCEPTION WHEN error THEN
SET job_end_dt = cast (NULL as TIMESTAMP);
SET job_completed_ind = 'N';
CALL `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);
-- insert into error_log table
INSERT INTO
`audit_cdw_log.error_log` (error_load_key, process_name, table_name, error_details, etl_create_date, etl_resource_name, etl_ins_audit_key)
VALUES
(v_audit_key,'DS_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;
SET result =  @@error.message;
raise using message = @@error.message;
END;
END
