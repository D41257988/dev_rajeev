begin

declare institution string default 'WLDN';
declare institution_id int64 default 5;
declare source_system_name string default 'WLDN_SF';
declare dml_mode string default 'delete-insert';
declare target_dataset string default 'rpt_crm_mart';
declare target_tablename string default 't_wldn_cohort_retention';
declare source_tablename string default 'Walden_Cohort_retention';
declare load_source string default 'trans_crm_mart.sp_wldn_cohort_retention';
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

declare max_last_term int64 default 0;

begin

SET additional_attributes= [("audit_load_key", v_audit_key),
          ("load_method",load_method),
          ("load_source",load_source),
          ("job_type", job_type)];
/* end common across */


set max_last_term= (select max(term_cd) from `rpt_academics.t_wldn_reconciled_list_cube`);
create or replace temp table p_cohort AS (
  SELECT
    DISTINCT sc.cohort AS first_term,
    tas.credential_id AS id,
    gen.AGE_ADMITTED,
    CASE
      WHEN tas.program IN ('MS_CMHC_GQ', 'MS_MHC', 'MS_MHC_LOCK') THEN 'MS_CMHC_GQ'
    ELSE
    tas.program
  END
    AS program,
    CASE
      WHEN (tas.program LIKE 'CERT%')OR (tas.program LIKE 'CRT%') THEN 1
    ELSE
    0
  END
    AS cert_flag,
    tas.program_desc,
    tas.student_status,
    tas.student_status_desc,
    tas.student_level,
    tas.college,
    tas.college_desc
  FROM
    rpt_academics.t_academic_study tas
		left join `rpt_academics.t_general_student` gen
		on tas.credential_id=gen.credential_id
		and tas.academic_period=gen.academic_period
		and tas.primary_program_ind=gen.primary_program_ind
  RIGHT JOIN
    rpt_academics.v_student_cohort sc
  ON
    tas.person_uid = sc.person_uid
    AND tas.academic_period = sc.academic_period
    AND tas.academic_period = sc.cohort

  WHERE
    --tas.multi_source = 'USW1'
    --AND sc.multi_source = 'USW1'
    tas.primary_program_ind='Y'
		and sc.institution_id=5);

create or replace temp table p_student_course_with_pgm AS(
  SELECT
    scd.student_credential_id as id,
    scd.academic_period,
    scd.current_reg_status_cd as registration_status,
    scd.final_grade,
    scd.current_reg_status_desc as registration_status_desc,
    scd.registration_status_date,
    --scd.sub_academic_period,
    scd.course_identification,
    CASE
      WHEN tas.program IN ('MS_CMHC_GQ', 'MS_MHC', 'MS_MHC_LOCK') THEN 'MS_CMHC_GQ'
    ELSE
    tas.program
  END
    AS program,
    tas.program_desc,
    CASE
      WHEN (tas.program LIKE 'CERT%')OR (tas.program LIKE 'CRT%') THEN 1
    ELSE
    0
  END
    AS cert_flag,
		scd.inclass_flag as inclass,
    tas.student_status,
    tas.student_status_desc,
    tas.student_level,
    tas.college
  FROM rpt_academics.t_wldn_student_course_details scd
  LEFT JOIN
    rpt_academics.t_academic_study tas
  ON
    scd.student_credential_id = tas.credential_id
    AND scd.academic_period = tas.academic_period
  WHERE
    scd.student_credential_id LIKE 'A%'
    AND tas.primary_program_ind='Y' );

create or replace temp table course_catalog as (
SELECT distinct course_identification  FROM `rpt_academics.v_course_catalog`
where course_info2='NON_REVENUE'
);

create or replace temp table student_course as(
		select *
    /*,
				case when course_identification in (SELECT distinct course_identification  FROM course_catalog) then 0
		when course_identification like 'WSRO%' then 0
		when course_identification like 'WWOW%' then 0
		when course_identification like 'MGMT0999%' then 0
		when course_identification like 'CONT%' then 0
		when course_identification like 'EDUC6001%' then 0
		when course_identification = 'TESTTEST' then 0
		when (registration_status = 'A1' or registration_status = 'AU' or
		registration_status = 'DC' or registration_status = 'DD' or
		registration_status = 'DN' or registration_status = 'DU' or
		registration_status = 'DW' or registration_status = 'FC' or
		registration_status = 'FN' or registration_status = 'FS' or
		registration_status = 'W1' or registration_status = 'WL') then 0
		when final_grade = 'TR' or final_grade = 'PR' then 0
		else 1
	end as inclass*/
	from p_student_course_with_pgm
	);

create or replace temp table p_student_inclass as(
		select id, academic_period, student_level, program, max(inclass) as inclass
	from student_course
	where id like 'A%'
	group by id, academic_period, student_level, program
	order by id, academic_period,program
	);

create or replace temp table p_academic_outcome as(
		select distinct
		credential_id as id,
		program,
		college,
		status as award_status,
		academic_period_graduation
	from rpt_academics.t_academic_outcome
	where status in ('AN' , 'AW')
	);

create or replace temp table p_grads as(
		select *except(rn) from (select *,row_number() over(partition by id,program,academic_period_graduation) as rn
		from p_academic_outcome) where rn=1
	);

create or replace temp table expected_grads_data as(
		select *,
		case when program = 'MS_EDUCATION' and course_identification ="EDUC6600" then 1
		when program = 'MBA_LOCK' and course_identification = 'MMBA6780' then 1
		when program = 'MBA_SEM' and course_identification = 'MMBA6780' then 1
		when program = 'MISM' and (course_identification = 'NSEI6981' or course_identification = 'NSEI6982'
		or course_identification = 'NSEI6983' or course_identification = 'NSEI6984' or course_identification = 'NSEI6985')
		then 1
		when  program = 'MHA_HLTHADMN' and course_identification = 'MMHA6560' then 1
		when program = 'MPH_LOCK' and course_identification = 'PUBH6636' then 1
		when program = 'MS_CLRA' and course_identification = 'CLRA6560' then 1
		when program = 'MS_MHC_LOCK' and course_identification ="COUN6682" then 1
		when program = 'MS_NURSING' and course_identification ="NURS6510" then 1
		when program = 'MPA' and course_identification = 'MMPA6910' then 1
		when program = 'MS_NPMGMT' and course_identification = 'NPMG6910' then 1
		when program = 'MS_PSYC_LOCK' and course_identification = 'PSYC6393' then 1
		else 0 end as flag
		from p_student_course_with_pgm
		where (course_identification in
		('MMBA6780','NSEI6981','NSEI6982','NSEI6983','NSEI6984','NSEI6985','MMHA6560','PUBH6636','CLRA6560','MMPA6910',
		'NMPG6910','PSYC6393') or
		course_identification like '%EDUC6600%' or
		course_identification like '%COUN6682%' or
		course_identification like '%NURS6510%')
		order by id, academic_period
);
create or replace temp table p_acad_period_prog_inclass as(
with
expected_grads_data2 as (
select *,
case when registration_status in ('DW','DD','FN','FC','A1','FS','W1','WL') then 0
when final_grade in ('TR','PR') then  0
else 1 end as expected_grad
from expected_grads_data
),

expected_grads_data3 as (
select *,row_number() over(partition by id order by academic_period) as rid from expected_grads_data2
where expected_grad = 1
),

expected_grads_data4 as (
select *except(rid) from expected_grads_data3
),

p_expected_grads as (
select id, program, expected_grad from expected_grads_data4
),

grads_expectedgrads as (
select l.id as id,l.program as program,l.college,l.award_status,l.academic_period_graduation, r.expected_grad from p_grads l
left join p_expected_grads r
on l.id=r.id and l.program=r.program
),

p_grads_expectedgrads as (
select *except(expected_grad),
case when expected_grad is null then 0
when (award_status = 'AW' or award_status = 'AN') then 0
end as expected_grad,
case when (award_status = 'AW' or award_status = 'AN') then 1
else 0  end as grad
 from grads_expectedgrads
)
,
student_inclass as (
select *, case when ((student_level like "%Q%" and substr(academic_period, 5,2) in ('10','30','50','70'))) or
   ((student_level like "%S%" and substr(academic_period, 5,2) in ('20','40','60')))
then 1 else null end as flag
from p_student_inclass
where inclass = 1
),

student_inclass2 as (
select *,row_number() over(partition by id,program order by academic_period desc) as rid from student_inclass
where flag = 1
),

p_last_term as (
select *except(rid) from student_inclass2
where rid=1
)
,

grads_expected_grads_last_term as (
select a.*, b.academic_period as last_term
from p_grads_expectedgrads a
left join p_last_term b
on a.id = b.id and a.program = b.program
),

g_e_l_t as (
select *except(last_term),
case when (safe_cast(last_term as INTEGER)- safe_cast(academic_period_graduation as INTEGER)) > 0 then academic_period_graduation else last_term end as last_term
 from grads_expected_grads_last_term
)
,

grads_expected_grads_last_term2 as (
select distinct a.*, b.first_term
from g_e_l_t a
left join p_cohort b
on a.id = b.id and a.program = b.program
),

grads_expected_grads_last_term3 as (
select * from grads_expected_grads_last_term2
where safe_cast(first_term as INTEGER) < safe_cast(last_term as INTEGER)
),

p_grads_expected_grads_last_term as (
select *except(academic_period_graduation)
 from grads_expected_grads_last_term3
 where last_term is not null
),

ods_first_term_use as (
select *, safe_cast(first_term as INTEGER) as ft  from p_cohort
where id is not null
),

ods_first_term_use2_temp as (
select *,ft + 10 * i as term2 from ods_first_term_use
left join `trans_crm_mart.cohort_retention_sequence_table`
on 1=1
),

ods_first_term_use2 as (
select * from ods_first_term_use2_temp
where (term2-(trunc(term2/100)*100)) >= 10 and (term2-(trunc(term2/100)*100)) <= 70  and term2 <= max_last_term
order by id,ft,i
),

p_all_first_term_exploded as (
select *, safe_cast(term2 as string) as academic_period from ods_first_term_use2
where
 (substr(STUDENT_LEVEL,2,1) ='Q' and substr(safe_cast(term2 as string),5,2) in ('10','30','50','70'))
or (substr(STUDENT_LEVEL,2,1) ='S' and substr(safe_cast(term2 as string),5,2) in ('20','40','60'))
or STUDENT_LEVEL is null or safe_cast(term2 as string) is null
),

all_pgm_info as (
select distinct a.* except(program_desc,student_level,college,cert_flag,first_term,AGE_ADMITTED), b.program_desc, b.student_level
		, b.college,b.AGE_ADMITTED
    , b.cert_flag, b.first_term
from p_all_first_term_exploded a
left join p_cohort b
	on a.id = b.id and a.program = b.program
),

all_pgm_info_inclass as (
select a.*, b.inclass
from all_pgm_info a
left join p_student_inclass b
	on a.id = b.id and a.academic_period = b.academic_period and a.program=b.program
)
select distinct a.*, b.expected_grad, b.grad, b.last_term
from all_pgm_info_inclass a
left join p_grads_expected_grads_last_term b
	on a.id = b.id and a.program = b.program
);
create temp table Walden_Cohort_retention  as (
with P_acad_period_prog_inclass_b2b as(
select a.*,
--case when b.Tracking_code<> '' then 1 else 0 end as B2B,
--case when b.Tracking_code in ('4125202', '4160502', '4181201', '4181500', '4186700', '4201300','4221708', '4290300') then 1 else 0 end as Offshore
from p_acad_period_prog_inclass a
--left join b2b_student_lst b on a.ID =b.Applicant_id and a.program=b.program_code
),
P_acad_period_prog_inclass1 as(
  select * from P_acad_period_prog_inclass_b2b
  where (substr(STUDENT_LEVEL,2,1) ='Q' and substr(academic_period,5,2) in ('10','30','50','70'))
or (substr(STUDENT_LEVEL,2,1) ='S' and substr(academic_period,5,2) in ('20','40','60'))
or STUDENT_LEVEL is null or academic_period is null
),
P_acad_period_prog_inclass2 as(
  select * from P_acad_period_prog_inclass1
  where (substr(STUDENT_LEVEL,2,1) ='Q' and substr(academic_period,5,2) in ('10','30','50','70'))
or (substr(STUDENT_LEVEL,2,1) ='S' and substr(academic_period,5,2) in ('20','40','60'))
or STUDENT_LEVEL is null or academic_period is null
),

P_acad_period_prog_inclass3_temp as(
select *,trunc(cast(academic_period as int)/100) as yr,
trunc((cast(academic_period as int) - trunc(cast(academic_period as int)/100)*100))/10 as tm,
trunc(cast(first_term as INT)/100) as c_yr,
trunc((cast(first_term as INT) - trunc(cast(first_term as INT)/100)*100))/10 as c_tm
from P_acad_period_prog_inclass2
),
P_acad_period_prog_inclass3 as(
select *,
case when (substr(student_level,2,1) = 'S') then (yr-c_yr)*3 + trunc((tm-c_tm)/2)
when (substr(student_level,2,1) = 'Q') then (yr-c_yr)*4 + trunc((tm-c_tm)/2)
end as term_count
from P_acad_period_prog_inclass3_temp
),

P_acad_period_prog_inclass4_temp1 as(
select *,
trunc(cast(academic_period as int)/100) as yr,
trunc((cast(academic_period as int) - trunc(cast(academic_period as int)/100)*100))/10 as tm,
trunc(cast(first_term as INT)/100) as c_yr,
trunc((cast(first_term as INT) - trunc(cast(first_term as INT)/100)*100))/10 as c_tm
from P_acad_period_prog_inclass_b2b
),

P_acad_period_prog_inclass4_temp2 as(
select *,
case when (substr(student_level,2,1) = 'S') then (yr-c_yr)*3 + trunc((tm-c_tm)/2)
when (substr(student_level,2,1) = 'Q') then (yr-c_yr)*4 + trunc((tm-c_tm)/2)
end as term_count,
from P_acad_period_prog_inclass4_temp1
),

P_acad_period_prog_inclass4 as(
select *except(inclass,program),
case when (academic_period >= last_term and last_term is not null) then 1 else 0 end as Graduation,
case when inclass is NULL then 0
when term_count = 0 then 1
else inclass end as inclass,
case when program='MS_NURSING_SEM' then 'MSN_NURS'
when program='BS_IDST' then'BS_IDST_QTR'
else program end as program
from P_acad_period_prog_inclass4_temp2
where ((substr(STUDENT_LEVEL,2,1) ='Q' and substr(academic_period,5,2) in ('10','30','50','70'))
or (substr(STUDENT_LEVEL,2,1) ='S' and substr(academic_period,5,2) in ('20','40','60'))
or STUDENT_LEVEL is null or academic_period is null)
and safe_cast(first_term as integer) >= 200510 and academic_period >= first_term
),

P_acad_period_prog_inclass5 as(
select distinct a.*, b.gender, extract(year from birth_date) as birth_year, b.ipeds_ethnicity as ethnicity
from P_acad_period_prog_inclass4 a
left join rpt_academics.t_person b
on a.ID=b.credential_id --and b.multi_source='USW1'
),

P_acad_period_prog_inclass6 as(
select distinct a.*,
adi.STATE_PROVINCE,
adi.nation_scod_code_iso,
adi.nation_desc,
NUll as NATION_GROUP,
--NATION_GROUP length=20,
adi.postal_code as ZIP5,
--ZIP5 length=5,
case when adi.nation_scod_code_iso= 'USA' then 0 else 1 end as intl_flag
from P_acad_period_prog_inclass5 a
left join rpt_academics.v_address_international adi on a.ID=adi.credential_id
--left join smart.lk_address_ma b on a.ID=b.ID
),

walden_cohort_program_level_hold as (
select
distinct a.id, a.program , a.academic_period,
	  a.STUDENT_LEVEL as student_level, a.cert_flag,
	  a.intl_flag,
	  a.inclass, a.first_term,
	  a.expected_grad, a.grad, a.last_term,
	  /*a.b2b, a.Offshore, a.Channel_desc,*/ a.term_count, a.Graduation, a.NATION_SCOD_CODE_ISO,
	  a.STATE_PROVINCE, a.ZIP5, a.Gender, a.birth_year, a.ethnicity, a.AGE_ADMITTED,
	  /*hold1,hold2,hold3,hold4,hold5,hold6,hold7,hold8,hold9,hold10,hold11,hold12,hold13*/
 from (select *,row_number() over(partition by id,program,academic_period order by PROGRAM_DESC, STUDENT_LEVEL, COLLEGE, AGE_ADMITTED, cert_flag, first_term,
		inclass, expected_grad, grad, last_term,/* B2B, Offshore,*/ yr, tm, c_yr, c_tm, term_count, Graduation, Gender,
		birth_year, STATE_PROVINCE, NATION_SCOD_CODE_ISO, NATION_DESC, NATION_GROUP, ZIP5, intl_flag/*, hold1, hold2,
		hold3, hold4, hold5, hold6, hold7, hold8, hold9, hold10, hold11, hold12, hold13*/) as rid
		from P_acad_period_prog_inclass6) a
		where rid=1
),

Walden_lk_seasons as (
select distinct academic_period as term, /*substr(academic_period_desc,1 ,5+index(substr(academic_period_desc, 6,100 ),' ' ))*/ null as season
from rpt_academics.v_year_type_definition
where academic_period between '198700' and '202570'
),

Walden_lk_programs as (
SELECT distinct
program_group as program,
program_name as prg_desc,
sq_flag as prg_term,
degree_code as degree,
college_code as college,
budget_code as prg_budget_code,
budget_name,
concat(degree_code,'-',sq_flag) as degree_term,
concat(college_code,'-',sq_flag) as college_term,
case when substr(program_group,1, 4) in ('CERT' ,'CRT_' ) then concat(substr(program_group,1, 4),'-',sq_flag) else null end as cert_term
 FROM `rpt_academics.t_wldn_product_map`
 where --degree_code in ('PHD','MS','BS') and
 --college_code in ('CNUR','COEL','COHS','COMT','CSBS','CUGS') and
  budget_code is not null
),



first_concentration_desc as (
select distinct
first_concentration,
case when first_concentration='AGAC' then 'Adult/Gerontology Acute Care'
when first_concentration='AGPC' then 'Adult Gerontology Primary Care'
when first_concentration='ESBU' then 'Entrepreneur & Small Business'
when first_concentration='PMH' then 'Psychiatric & Mental Hlth Nurs'
when first_concentration='SDS' then 'Self-Designed'
else first_concentration_desc end as first_concentration_desc
from rpt_academics.t_academic_study
where first_concentration is not null
),

graduates as (
select distinct
credential_id as id,program,academic_period as academic_period_graduation,
case when (upper(substr(program,1,4))='CERT' or upper(substr(program,1,3))='CRT') then 'Cert Receiver' else 'Graduate' end AS category,
row_number() over(partition by credential_id order by academic_period_graduation) as rid
from rpt_academics.t_academic_outcome
where status in ('AW', 'AN','P1')
)
,

graduates2 as (
select * from graduates
where rid=1
),

alumni as (
select distinct a.*, case when (a.ID=b.id and b.category='Graduate' and a.first_term>=b.academic_period_graduation) then 1 else 0 end as alumni_flag,
a.ID as aid ,b.ID as bid
from walden_cohort_program_level_hold a
left join graduates2 b
on a.ID=b.ID and b.category='Graduate' and a.first_term>=b.academic_period_graduation
)
,

sec_term_flag as (
select distinct ID, Program, first_term, inclass as Sec_term_inclass, academic_period as sec_term_period
from alumni
where term_count=1
),

Thd_term_flag as (
select distinct ID, Program,first_term, inclass as Thd_term_inclass, academic_period as Thd_term_period
from alumni
where term_count=2
),

EA_assigned as (
  SELECT distinct l.application_id
		,l.banner_id as id
		--,l.onyx_incident_key
		,l.term_cd
		,l.es_curr_sfid as recruiter
		,l.es_curr_name as EA_Name
		,l.es_curr_manager_name as  Manager_Name
		,l.es_curr_site as location
		,r.start_date
		,r.application_status_date as status_date
		,l.date_rs as Reserved_Date

  FROM `rpt_crm_mart.t_wldn_opp_snapshot` l
	left join `rpt_academics.v_admissions_application` r
	on concat(r.credential_id,'-',r.application_number,'-',r.academic_period)=l.application_id
  where l.institution_id = 5
	and l.date_rs is not null
),

EA_assigned2 as (
select id,recruiter,EA_Name,Manager_Name,location,Reserved_Date from (select *,row_number() over(partition by id order by status_date desc,Reserved_Date desc) as rid from EA_assigned)
where rid=1
),

TOC as (
select distinct credential_id as id, academic_period, course_credits
from rpt_academics.t_student_course
where final_grade in ('TR','PR')
),

Walden_cohort_sec_term_final_toc as (
select distinct a.*,substr(a.STUDENT_LEVEL,2,1) as Term_Type, b.Sec_term_inclass, b.Sec_term_period,c.Thd_term_inclass,c.Thd_term_period,
	   d.recruiter as EA_ID, d.EA_name, Reserved_Date,
	   e.id as TOC_term, f.id as TOC_flag
from alumni a
left join sec_term_flag b
	on a.id=b.id and a.Program=b.Program and a.first_term=b.first_term
left join thd_term_flag c
	on a.id=c.id and a.Program=c.Program and a.first_term=c.first_term
left join EA_assigned2 d on a.id = d.id
left join toc e on a.id=e.id and a.ACADEMIC_PERIOD=e.ACADEMIC_PERIOD
left join toc f on a.id=f.id
where a.student_level <> 'ND'
),

previous_degree as (
SELECT distinct case when l.institution_type='C' then 'College' else null end as institution_type,l.school_gpa,l.attend_from_date,l.attend_to_date,l.degree_date, r.credential_id as id  FROM `rpt_academics.v_previous_degree` l
left join `rpt_academics.t_person` r
on l.person_uid=r.person_uid
where l.institution_id=5
),
/*
previous_degree2 as (
select * from (select *,row_number() over(partition by id order by attend_from_date desc) as rid from previous_degree)
where rid=1
),
*/
Walden_cohort_sec_term_f_toc1 as (
select distinct a.*, b.start_date as term_start, b.end_date as term_end
from Walden_cohort_sec_term_final_toc a
left join rpt_academics.v_term b on a.first_term=b.academic_period
),

Walden_cohort_sec_term_f_toc1_2_temp as (
select distinct a.*, b.institution_type as INSTITUTION_TYPE_DESC, cast(b.school_gpa as BIGNUMERIC) as school_gpa, b.degree_date
from Walden_cohort_sec_term_f_toc1 a
left join previous_degree b on a.id=b.id and (a.term_start>=safe_cast(b.degree_date as date) or b.degree_date is null)
)
,
Walden_cohort_sec_term_f_toc1_2 as (
select *except(rid) from (select *,row_number() over(partition by ID, PROGRAM, first_term, academic_period  order by term_count desc, DEGREE_DATE desc) as rid from Walden_cohort_sec_term_f_toc1_2_temp)
where rid=1),

fb_events as (
select distinct banner_id_c as ID,event_type_c as EventType,
safe_cast(SAFE.PARSE_DATE("%m.%d.%y", REGEXP_REPLACE(event_type_c, '[^0-9.]','')) as date) as fb_first_dt
from `raw_b2c_sfdc.opportunity`
where  institution_c = 'a0ko0000002BSH4AAO' and event_type_c like '%FB%'  and banner_id_c like 'A%'
),

fb_member_first as (
select *,
row_number() over(partition by id order by fb_first_dt) as rid
 from fb_events
),

Walden_cohort_sec_term_f_toc1_3_temp as (
select distinct a.*, case when c.fb_first_dt is not null and a.term_end>=c.fb_first_dt then 1 else 0 end as fb_flag
from  Walden_cohort_sec_term_f_toc1_2 a
left join fb_member_first c on a.id =c.id and rid=1
),

Walden_cohort_sec_term_f_toc1_3 as (
select l.*except(school_GPA),
case when school_GPA < 4 then school_GPA
when school_GPA>=4 and school_GPA<5 then 4
when school_GPA>=5 and school_GPA<70 then 1
when school_GPA>=70 and school_GPA<80 then 2
when school_GPA>=80 and school_GPA<90 then 3
when school_GPA>=90 then 4 else null end as Prior_school_GPA
,case when concat(id,first_term) in(
   select distinct concat(banner_id_c, program_start_date_term_c) cred_term
from `raw_b2c_sfdc.campaign` c
left join `raw_b2c_sfdc.opportunity` o on c.id=o.campaign_id
where
c.is_deleted = false
and c.institution_c = 'a0ko0000002BSH4AAO'
and (
activity_id_c in (
'4082201','4086302','4087812','4087814',
'4087900','4087901','4106800','4107700',
'4108916','4114301','4116301','4130305',
'4290300','AFFN0010','ASAS0100','AUOA0010',
'AUOA0100','BCAB010L','EARC0100','EARCOS',
'ECIS0100','EDAW0000','NESA0100','ONLI0000',
'RICO0060','SYPR0170','TRIA0100')
or c.activity_sub_type_c like '%B2B%'
or c.activity_sub_type_c like '%CIP%'
or (c.activity_type_c = 'Agent' and ifnull(aprimo_owner_name_c, 'Unknown') <> 'Unknown')
) and
activity_id_c not in (
'4098817','4099211','4099214','4107701',
'4109601','4110001','4110100','4111502',
'4111600','4111601','4111801','4114201',
'4114203','4114204','4114205','4114300',
'4118007','4120209','4130303','4241001',
'BCAB010L')

    ) then true else false end as b2b
 from Walden_cohort_sec_term_f_toc1_3_temp l

),
Walden_cohort_sec_term_f_toc1_4 as (
select
'WLDN' AS institution
,5 AS institution_id
,'WLDN_SF' as source_system_name
,ID
,PROGRAM
,academic_period
,STUDENT_LEVEL
,cert_flag
,intl_flag
,inclass
,first_term
,grad
,last_term
,b2b
--,Offshore
--,channel_desc
,safe_cast(term_count as BIGNUMERIC) as term_count
,Graduation
,NATION_SCOD_CODE_ISO
,STATE_PROVINCE
,GENDER
,ethnicity
,birth_year
,AGE_ADMITTED
,alumni_flag
,Term_Type
,INSTITUTION_TYPE_DESC as prior_degree_inst_type
,DATE(DEGREE_DATE) as prior_degree_award_date
,fb_flag
,Prior_school_GPA as prior_degree_gpa
from Walden_cohort_sec_term_f_toc1_3
where  (inclass =1 or graduation = 1)
),
census_table as (
  SELECT distinct
date_add(start_date, INTERVAL case when EXTRACT(DAYOFWEEK FROM start_date)=1 then 1 when EXTRACT(DAYOFWEEK FROM start_date)=2 then 7 else 9-EXTRACT(DAYOFWEEK FROM start_date) end DAY)+1*7 as census_date,academic_period, FROM `rpt_academics.v_term`
),
src as (
SELECT l.*except(inclass),
case when census_date>current_date() then 0 else inclass end as inclass
 FROM Walden_cohort_sec_term_f_toc1_4 l
left join census_table r
on l.academic_period=r.academic_period
)
select src.*,
    job_start_dt as etl_created_date,
    job_start_dt as etl_updated_date,
    load_source as etl_resource_name,
    v_audit_key as etl_ins_audit_key,
    v_audit_key as etl_upd_audit_key,
    farm_fingerprint(format('%T', concat(src.id))) AS etl_pk_hash,
    farm_fingerprint(format('%T', src )) as etl_chg_hash,
    FROM src);
-- merge process
 call utility.sp_process_elt (institution, dml_mode , target_dataset, target_tablename, null, source_tablename, additional_attributes, out_sql );

    set job_end_dt = current_timestamp();
    set job_completed_ind = 'Y';

    -- export success audit log record
    call `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);

    -- update audit refresh upon process successful completion
    --multiple driver tables need multiple inserts here, one for each driver table, In this case we only have sgbstdn
    --call `audit_cdw_log.sp_export_audit_table_refresh` (v_audit_key, 'general_student_stage',target_tablename,institution_id, job_start_dt, current_timestamp(), load_source );


    set result = 'SUCCESS';


EXCEPTION WHEN error THEN

SET job_end_dt = cast (NULL as TIMESTAMP);
SET job_completed_ind = 'N';

CALL `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);


-- insert into error_log table
insert into
`audit_cdw_log.error_log` (error_load_key, process_name, table_name, error_details, etl_create_date, etl_resource_name, etl_ins_audit_key)
values
(v_audit_key,'DS_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;


set result = @@error.message;
raise using message = @@error.message;

END;

END