CREATE OR REPLACE PROCEDURE `trans_academics.sp_rrt_common_code`(v_audit_key STRING, OUT v_rrt_common_code STRING)
OPTIONS (strict_mode=false)
begin
declare institution string default 'WLDN';
declare institution_id int64 default 5;
declare dml_mode string default 'delete-insert';       ------------------------------------------------------------------
declare target_dataset string default 'trans_academics';
declare target_tablename string default 'prc_rrt_base_list_comm_final';
declare source_tablename string default 'prc_rrt_base_list_comm_final_temp';
declare load_source string default 'trans_academics.sp_rrt_common_code';
declare additional_attributes ARRAY<struct<keyword string, value string>>;
declare last_refresh_time timestamp;
declare tgt_table_count int64;

/* common across */
declare job_start_dt timestamp default current_timestamp();
declare job_end_dt timestamp default current_timestamp();
declare job_completed_ind string default null;
declare job_type string default 'FT';  -------------------------------------
declare load_method string default 'scheduled query';   -------------------------------------
declare out_sql string;

begin

SET additional_attributes= [("audit_load_key", v_audit_key),
          ("load_method",load_method),
          ("load_source",load_source),
          ("job_type", job_type)];

/* end common across */



create or replace table `trans_academics.prc_rrt_student_course` as
select
	bp.institution,
	bp.institution_id,
	bp.person_uid,
	bp.credential_id as ID,
	Scourse.academic_period,
	sub_academic_period,
	course_identification,
	course_section_number,
	Course_reference_number,
	start_date,
	end_date,
	registration_status,
	registration_status_Desc,
	registration_status_date,
	Campus as campus, ------- used Campus in place of course_Campus
	Scourse.Subject,
	Scourse.FINAL_GRADE
from `rpt_academics.t_student_course` Scourse
inner join `rpt_academics.t_person` BP
on Scourse.person_uid = bp.person_uid and scourse.institution = bp.institution
join `trans_academics.prc_rrt_load_terms` PT
on Scourse.ACADEMIC_PERIOD = PT.Academic_Period
and Scourse.institution = PT.institution
Where bp.institution ='WLDN';


select * from `trans_academics.prc_rrt_student_course` limit 10;


create or replace temp table `PRC_RRT_BB_Activity` as
select
BB_Act.institution,
BB_ACT.ACADEMIC_PERIOD,
BB_ACT.ID_NUMBER,
BB_ACT.COURSE_REFERENCE_NUMBER,
SUM(BB_ACT.ACTIVITY_MINUTES) ACTIVITY_MINUTES,
MAX(BB_ACT.ACTIVITY_DATE) ACTIVITY_DATE,
Count(distinct BB_ACT.ACTIVITY_DATE) as last_activity_date
from `rpt_academics.v_lms_activity` BB_Act   --------------- Gayatri created this table
join `trans_academics.prc_rrt_load_terms` PT
on BB_Act.ACADEMIC_PERIOD = PT.Academic_Period
and  PT.institution = 'WLDN' AND BB_Act.COURSE_ROLE = 'S' and BB_Act.institution = 'WLDN'  ------------------------------------
where BB_Act.institution='WLDN'
Group by BB_Act.institution,BB_ACT.ACADEMIC_PERIOD,BB_ACT.ID_NUMBER,BB_ACT.COURSE_REFERENCE_NUMBER;


select * from `PRC_RRT_BB_Activity` limit 100;



create or replace temporary table `PRC_RRT_Base_List` as
Select
Reserve_List.Student_Start_Dt Student_Start_Date,
Reserve_List.Applicant_ID,
Reserve_List.Name,
DBP.First_Name,
DBP.Last_Name,
Reserve_List.AdmRepName,
Reserve_List.Manager,
Reserve_List.EA_Location,
Reserve_List.Audience_Name,
Reserve_List.Banner_Conc_Desc,
Reserve_List.Academic_Program,
Reserve_List.Address_Type,
Reserve_List.Address_line_1,
Reserve_List.Address_line_2,
Reserve_List.City,
case when (Reserve_List.State = 'RQ' or Reserve_List.state = 'Inter') then 'International'
	  when Reserve_List.State = 'SK' and Reserve_List.Country = 'United States of America' then 'SC'
	  else Reserve_List.State
end as State,
Case When Reserve_List.State = 'RQ' then 'Puerto Rico'
  else Reserve_List.Country
end as Country,
Reserve_List.Zip_Cd,
Reserve_List.EA_Region,
Reserve_List.Student_Type,
Reserve_List.Application_Dt Application_Date,
Reserve_List.Current_Status_Dt Current_Status_Date,
Reserve_List.Deferred,
Reserve_List.Prior_Start_Dt Prior_Start_Date,
Reserve_List.FA_App_Status,
Reserve_List.Tracking_Status,
Reserve_List.Term_Cd,
Reserve_List.Fin_Aid_Year,
Null Inquiry_Channel,
Reserve_List.Course_1,
Reserve_List.Course_2,
Reserve_List.Course_3,
Reserve_List.Course_4,
Reserve_List.Course_5,
Reserve_List.Course_6,
Reserve_List.Phone_Nbr_Home,
Reserve_List.Phone_Nbr_Business,
Reserve_List.Main_Email,
Reserve_List.Personal_Email,
Reserve_List.Application_ID,
Reserve_List.OppSfId,
Reserve_List.SGASTDN_Program,
Reserve_List.Program_Name as Rprg_name,
cast(BIADM_Product_Map.Program_Name as String) as Program_Name,
Reserve_List.Degree_Level as rdeg_lvl,
cast(BIADM_Product_Map.level_description as String) as Degree_Level,
Reserve_List.college as rcollname,
cast(BIADM_Product_Map.college_code as String) as College_Name,
BIADM_Product_Map.SQ_flag,

Case When (Reserve_List.State = 'RQ' or Reserve_List.state = 'International')  then 'INTL'
     else 'US'
end as International_Flag,
Reserve_List.Term

from

(Select
	Min_Term.*,
	ROW_NUMBER() over (partition by Min_term.Applicant_Id order by Term) as Min_term_rnk from (

	Select RList.*,
	row_number() over (partition by RList.Applicant_id,RList.Current_status_dt,pterm.academic_period order by RList.Current_status_dt desc) as rnk ,
	Pterm.Academic_Period as Term
	from
  `rpt_academics.t_wldn_reserve_list` RList -----------------mddbclbisql5.edw_sm_dm.dbo.RPT_WAL_Reserve_List RList (nolock)-------------------------------
	join  `rpt_academics.v_part_of_term` Pterm      --- having Academic_Period 202260
	  on RList.Student_Start_Dt = Pterm.Start_Date
	join `trans_academics.prc_rrt_load_terms` lterm
		on lterm.Academic_Period = Pterm.Academic_Period and Pterm.institution = lterm.institution and Pterm.institution = 'WLDN'
	Join `rpt_academics.t_person` Per
		on Rlist.Applicant_ID = Per.credential_id
	Join `rpt_academics.t_general_student` GenS   --- having Academic_Period 202270
		on Per.Person_UID = Gens.PERSON_UID
		And Rlist.Student_Start_Dt = GenS.START_DATE
		And GenS.ACADEMIC_PERIOD = Pterm.Academic_Period    --- this removing all records as Pterm and GenS does not have same Academic_Period
	--where rlist.etl_process_dt = cast(current_date() as date)

    )Min_Term
		where rnk = 1
	  ) Reserve_List

left join `rpt_academics.t_wldn_product_map` BIADM_Product_Map  ----------------------------------------------------
on Reserve_List.academic_program = BIADM_Product_Map.Product_Nbr
join `rpt_academics.t_person` DBP --------------------------------check this table-------------------------------------------
on Reserve_List.Applicant_ID = DBP.credential_id
where Reserve_List.Min_term_rnk = 1
and Reserve_List.degree_level <> 'NON'
and BIADM_Product_Map.Level_Description not in ('NON',' ')
and BIADM_Product_Map.College_code <> 'LIU PARTNERS';


select * from `PRC_RRT_Base_List` limit 100;


Delete from PRC_RRT_Base_List
where Date(Student_Start_Date) = Date(2020,01,06) and cast(Term as int64) = 202060;


select * from `PRC_RRT_Base_List` limit 100;


create or replace temp table `PRC_RRT_SRO_Details` as
Select Scourse.ID,
Scourse.academic_period,
Scourse.COURSE_IDENTIFICATION,
Scourse.course_reference_number,
Scourse.start_date as course_start_date
from `PRC_RRT_Base_List` blist
Join `trans_academics.prc_rrt_student_course` Scourse
On blist.applicant_Id = Scourse.Id
and Scourse.Academic_Period = blist.Term
and Scourse.Subject  IN ('WSRO','WWOW')
and SCourse.REGISTRATION_STATUS in ('RE','RW');


select * from `PRC_RRT_SRO_Details` limit 10;

create or replace temp table `PRC_RRT_SRO_Details_Final` as
Select
Scourse.ID,
Scourse.academic_period,
Scourse.COURSE_IDENTIFICATION,
sum(activity_minutes) as Activity_Minutes_Sum
from PRC_RRT_Base_List blist
Join PRC_RRT_SRO_Details Scourse
On blist.applicant_Id = Scourse.Id
and Scourse.Academic_Period = blist.Term
Join PRC_RRT_BB_Activity BB_Act
On Scourse.Id = BB_Act.ID_NUMBER
and Scourse.Academic_Period = BB_Act.ACADEMIC_PERIOD
--and Replace(BB_Act.COURSE_IDENTIFICATION,'-','')=Scourse.COURSE_IDENTIFICATION
and BB_Act.COURSE_REFERENCE_NUMBER=Scourse.course_reference_number
Group by Scourse.ID, Scourse.academic_period, Scourse.COURSE_IDENTIFICATION
having sum(activity_minutes) > 20;


select * from `PRC_RRT_SRO_Details_Final` limit 10;


create or replace temp table `PRC_RRT_Facebook_Data` as
select distinct  bp.banner_id_c as BannerId,
opp.brand_profile_c as brand_profile_id_c,
event_type_c as EventType
  FROM `raw_b2c_sfdc.opportunity`  opp   -------------------qa table

	left join `raw_b2c_sfdc.brand_profile_c` bp on opp.brand_profile_c = bp.id and bp.is_deleted = false -- rs - added the code to get the banner_id_c
  inner join PRC_RRT_Base_List bl on bp.banner_id_c = bl.Applicant_ID
  where opp.institution_c ='a0ko0000002BSH4AAO'and opp.is_deleted= false and Opp.event_type_c like '%fb%';

select * from `PRC_RRT_Facebook_Data` limit 10;

create or replace temp table `PRC_RRT_Non_SRO_Details` as
select Scourse.*
from `trans_academics.prc_rrt_student_course` Scourse
join `PRC_RRT_Base_List` blist
on Scourse.ID=blist.applicant_Id
and Scourse.Start_date=blist.Student_Start_Date
where Scourse.SUBJECT not  IN ('WSRO','WWOW') and Scourse.registration_status in ('RE','RW') and (Scourse.final_grade is null or Scourse.final_grade not in ('TR','PR'));


select * from `PRC_RRT_Non_SRO_Details` limit 10;

create or replace temp table `PRC_RRT_Login` as
select BB_Act.*
from PRC_RRT_BB_Activity BB_Act
join PRC_RRT_Non_SRO_Details Non_SRO
on BB_Act.ID_NUMBER=Non_SRO.id and BB_Act.ACADEMIC_PERIOD=Non_SRO.academic_period and BB_Act.course_reference_number=Non_SRO.course_reference_number;

select * from `PRC_RRT_Login` limit 10;


create or replace temp table `PRC_RRT_Login2` as
Select
ID_NUMBER,
ACADEMIC_PERIOD,
count(distinct activity_date) as Login_days
from `PRC_RRT_Login`
group by ID_NUMBER, ACADEMIC_PERIOD ;

select * from `PRC_RRT_Login2` limit 10;


create or replace temp table `PRC_RRT_Stud_Login` as
Select
ID_NUMBER,
last_activity_date,
Case when date_diff(current_date(),Date(last_activity_date),DAY) >5 then 1
	 else 0
end as No_Login_6days
From(select ID_NUMBER , FORMAT_DATE('%Y-%m-%d',Date(max(activity_date))) as last_activity_date  --------------- when data is availablle check the type of max(activity_date) and full calculation
	 from PRC_RRT_Login
	 group by ID_NUMBER) Stud_Login ;


select count(*) from `PRC_RRT_Stud_Login`;

create or replace temp table `PRC_RRT_Pre_Participation` as
select CO.COURSE_IDENTIFICATION,CO.START_DATE,CO.END_DATE,BB.*

--count(1)
from `rpt_academics.v_lms_act_submission_count` BB    ---------------------------------------------------------------------------
	INNER JOIN `rpt_academics.t_course_offering` CO
		ON upper(BB.institution) = upper(CO.institution) AND
			BB.COURSE_REFERENCE_NUMBER = CO.COURSE_REFERENCE_NUMBER
where Date(CO.start_date) in (select distinct Date(student_start_date) from PRC_RRT_Base_List);


select count(*) from `PRC_RRT_Pre_Participation` limit 10;


create or replace temp table `PRC_RRT_Participation` as
select PP.*
from `PRC_RRT_Pre_Participation` PP
inner Join `PRC_RRT_Non_SRO_Details` Non_SRO
	On PP.ID_NUMBER=Non_SRO.ID
		and PP.Start_date =Non_SRO.START_DATE
		and PP.COURSE_IDENTIFICATION = Non_SRO.COURSE_IDENTIFICATION
		and PP.ACADEMIC_PERIOD = Non_SRO.Academic_Period
where PP.ACTIVITY_SUBMISSION_COUNT <> 0;

select count(*) from `PRC_RRT_Participation` limit 10;


create or replace temp table `PRC_RRT_Stud_Participation` as
Select ID_Number
	  ,Last_Post_Date
	  ,Case When Date_diff(current_date(), Date(Last_Post_Date), DAY) > 5 then 1
			else 0
	   end as Flag_Non_Participation

From (select ID_Number, FORMAT_DATE('%Y-%m-%d',max(posted_date)) as last_post_date-------------------------------------------------------------------
	  from PRC_RRT_Participation
	  group by ID_Number) Stud_Participation;

select * from `PRC_RRT_Stud_Participation` limit 10;


---------------------------------------------------------------Keep orientation code w null values---------------------------------------------------------------

-- create or replace table `PRC_RRT_Orientations` as
-- select distinct
-- Orient_Attend.ApplicantID,
-- Orient_Attend.SRO,
-- Orient_Attend.NSO,
-- Orient_Attend.SSW,
-- Orient_Attend.Orientation_total,
-- Orient_Attend.anyorientation
-- from MDDBSQL25.BI_Analytics_DM.dbo.orientationattendance Orient_Attend-----------------------------------------------
-- Join PRC_RRT_Base_List b_list
-- on Orient_Attend.ApplicantID=b_list.Applicant_ID
-- and Orient_Attend.start_date=b_list.Student_Start_Date

---------------------------------------------------------------Ignore orientation code---------------------------------------------------------------


create or replace temp table `PRC_RRT_Bursar_Holds` as
select distinct SMart_Hold.credential_id as id   ------------------- using person_uid in place of id
from `rpt_academics.v_hold` SMart_Hold ----------------------------------------------------------------
Join `PRC_RRT_Base_List` b_list
On SMart_Hold.credential_id=b_list.Applicant_ID
where SMart_Hold.HOLD_FROM_DATE <= current_timestamp()
	and SMart_Hold.HOLD_TO_DATE >= current_timestamp()
	and SMart_Hold.HOLD_RELEASE_IND = 'N'
	and SMart_Hold.HOLD = 'BH';

select * from `PRC_RRT_Bursar_Holds` limit 10;

create or replace temp table `PRC_RRT_Alumni` as
select distinct SMart_Academic_Outcome.credential_id as ID
from `rpt_academics.t_academic_outcome` SMart_Academic_Outcome------------------------------------------------------------
join `PRC_RRT_Base_List` b_list
on SMart_Academic_Outcome.credential_id =b_list.Applicant_ID
where SMart_Academic_Outcome.STATUS in ('AW', 'AN', 'P1', 'P2', 'P3')
	and SMart_Academic_Outcome.GRADUATED_IND = 'Y'
	and SMart_Academic_Outcome.OUTCOME_GRADUATION_DATE is not null;


select * from `PRC_RRT_Alumni` limit 10;

create or replace temp table `PRC_RRT_RRO` as
select
RRO.banner_id as ID,
RRO.retention_score retention_likelihood_score,
RRO.scoring_date,
RRO.decile retention_likelihood_decile,
RRO.retention_factor_1 as Retention_risk_Driver1,
RRO.retention_factor_2 as Retention_risk_Driver2,
RRO.retention_factor_3 as Retention_risk_Driver3,
rank() over (partition by rro.banner_id order by scoring_date desc) rnk
from `rpt_ml_semantic.stud_retention_score_history` RRO       ---------------------------------------RISK-----------------------
join `PRC_RRT_Base_List` b_list
on RRO.banner_id=b_list.Applicant_ID
qualify rnk = 1;

select * from `PRC_RRT_RRO` limit 10;


create or replace temp table `PRC_RRT_Wrong_FAFSA` as
select
W_FAFSA.Banner_ID as BannerID
from `rpt_academics.t_wrong_fafsa_pull_automated` W_FAFSA-------------------------------------------
join PRC_RRT_Base_List b_list
on W_FAFSA.Banner_ID=b_list.Applicant_ID;


select * from `PRC_RRT_Wrong_FAFSA` limit 10;




create or replace temp table `PRC_RRT_Base_list_Comm1` as
select distinct
a.*,
case when a.Applicant_ID <> COALESCE(b.ID,'') then 1
	   else 0
end as No_SRO,
case when a.Applicant_ID <> COALESCE(c.BannerId,'') then 1
     else 0
end as No_facebook,
case when e.No_Login_6days=1 then 1
	   when e.No_Login_6days=0 then 0
	   when e.No_Login_6days is NULL then 1
end as No_Login_6days,
e.last_activity_date as Last_Login_Date,
case when a.Applicant_ID <> COALESCE(f.ID_NUMBER,'')  then 1
     else 0
end as No_Login,
case when g.Flag_Non_Participation=1 then 1
	   when g.Flag_Non_Participation=0 then 0
	   when g.Flag_Non_Participation is NULL then 1
end as No_Participation_6days,
g.last_post_date as last_participation_date,
case when h.Attribute_Code_c = 'CONT' then 1
	   else 0
end as CONT_hold,
h.CONT_reason,
case when j.Old_MILESTONE is NULL then 'NOT in FA360 as of Today'
	   else j.Old_MILESTONE
end as FinAid_General_Status,
j.Milestone_Status_ID,
j.SAP_END_DATE as SAP_END_DATE,
j.NEEDS_TO_ACCEPT_AWARD,
j.NEEDS_TO_COMPLETE_SUB_UNSUB_MPN,
j.NEEDS_TO_COMPLETE_GPLUS_MPN,
j.NT_COMPLETE_ENTRANCE_INTERIVEW,
j.NEEDS_TO_COMPLETE_UAAP,
j.HAS_AGGREGATE_LOAN_ISSUE,
j.HAS_DISBURSEMENT_ISSUE,
j.TERM_END_DATE as TERM_END_DATE,
j.COURSE_CREDITS,
j.refundAmount,
j.NUDGE_DUE_DATE as NUDGE_DUE_DATE,
j.NUDGE_OFFSET,
j.FIRST_NUDGE_DATE as FIRST_NUDGE_DATE,
j.PRIORITY,
NULL  SRO,        			-- k.SRO,  --coming from Orientations so NULL
NULL  NSO,        			-- k.NSO,  --coming from Orientations so NULL
NULL  SSW,        			-- k.SSW,  --coming from Orientations so NULL
NULL  Orientation_total,        			-- k.Orientation_total, --coming from Orientations so NULL
NULL  anyorientation,        			-- k.anyorientation,  --coming from Orientations so NULL
case when l.ID IS NOT NULL then 1
	   else 0
end as BURSAR_hold,
case when m.ID IS NOT NULL then 1
	   else 0
end as ALUMNI_flag,
n.Retention_Likelihood_Score,
n.Scoring_Date,
n.Retention_Likelihood_Decile,
n.Retention_risk_Driver1,
n.Retention_risk_Driver2,
n.Retention_risk_Driver3,
case When a.Applicant_ID=o.BannerID then 1
	   else 0
end as Wrong_FAFSA_Flag
from PRC_RRT_Base_List a
	left join PRC_RRT_SRO_Details_Final	b
		on a.Applicant_ID=b.ID
	left join PRC_RRT_Facebook_Data	c
		on a.Applicant_ID=c.BannerId
	left join PRC_RRT_Stud_Login e
		on a.Applicant_ID=e.ID_NUMBER
	left join PRC_RRT_Login2 f
		on a.Applicant_ID=f.ID_NUMBER
	left join PRC_RRT_Stud_Participation g
		on a.Applicant_ID=g.ID_NUMBER
	left join `trans_academics.prc_rrt_hold_cont`	h
		on a.OppSfId=h.Opportunity_c
	left join `trans_academics.prc_rrt_fa_data` j
		on a.Applicant_ID=j.ALTERNATE_ID  and ((a.Term = cast(j.Term as String) and j.Calculated_Future_Term_Flag=j.FutureTermFlag) or (a.term < cast(j.Term as String)))
	-- left join PRC_RRT_Orientations k  ------ ignore this-------
	-- 	on a.Applicant_ID=k.ApplicantID  ------ ignore this-------
	left join `PRC_RRT_Bursar_Holds` l
		on a.Applicant_ID=cast(l.ID as String)
	left join `PRC_RRT_Alumni` m
		on a.Applicant_ID=cast(m.ID as String)
	left join `PRC_RRT_RRO` n
		on a.Applicant_ID=cast(n.ID as String)
	left join `PRC_RRT_Wrong_FAFSA` o
		on a.Applicant_ID=o.BannerID;


select * from `PRC_RRT_Base_list_Comm1` limit 10;

Update `PRC_RRT_Base_list_Comm1`
Set No_Login_6days = 0
where No_Login = 1 and No_Login_6days = 1;

select * from `PRC_RRT_Base_list_Comm1` limit 10;


create or replace temp table `PRC_RRT_Base_list_Comm` as
Select
	Base_list1.*,
	RPT_Aging.CURRENT_AMOUNT_DUE,
	RPT_Aging.c_0_30_Dollars as DUE_0_30,
	RPT_Aging.c_31_60_Dollars as DUE_31_60,
	RPT_Aging.c_61_90_Dollars as DUE_61_90,
	RPT_Aging.c_91_120_Dollars as DUE_91_120,
	RPT_Aging.c_121_150_Dollars as DUE_121_150,
	RPT_Aging.c_151_180_Dollars as DUE_151_180,
	RPT_Aging.c_181_210_Dollars as DUE_181_210,
	RPT_Aging.c_211_240_Dollars as DUE_211_240,
	RPT_Aging.c_241_270_Dollars as DUE_241_270,
	RPT_Aging.c_271_300_Dollars as DUE_271_300,
	RPT_Aging.c_301_330_Dollars as DUE_301_330,
	RPT_Aging.c_331_dollars as DUE_331_P,
	RPT_Aging.final_category_cd as AGING_Status,
	RPT_Aging.COUNTRY as COUNTRY2
from PRC_RRT_Base_list_Comm1 Base_list1
Left Join `rpt_student_finance.t_wldn_aging` RPT_Aging     -------------------------------------rpt_student_finance.t_aging-------------------------------
On Base_list1.Applicant_ID = RPT_Aging.student_id
and date(RPT_Aging.etl_created_date) = current_date()
;
--and RPT_Aging.ETL_Process_Date = Convert(DateTime, DATEDIFF(DAY, 0, GETDATE()))          ----------------- check for this logic---------------------------

select * from `PRC_RRT_Base_list_Comm` limit 10;


/*Add prev institution info for the student*/
create or replace temp table `PRC_RRT_Previous_Edu` as
Select
	ID,
	INSTITUTION_TYPE_DESC,
	AWARD_CATEGORY,
	AWARD_CATEGORY_DESC,
	INSTITUTION_DESC,
	EDUCATION_ACTIVITY_DATE,
	ATTEND_TO_DATE ,
	SCHOOL_GPA,
	TOTAL_TRANSFER_CREDITS
from (select distinct
		credential_id as ID,
		INSTITUTION_TYPE_DESC,
		AWARD_CATEGORY,
		AWARD_CATEGORY_DESC,
		INSTITUTION_DESC,
		date(EDUCATION_ACTIVITY_DATE) EDUCATION_ACTIVITY_DATE,
		ATTEND_TO_DATE ,
		SCHOOL_GPA,
		TOTAL_TRANSFER_CREDITS,
		Row_Number() Over (Partition by credential_id Order by COALESCE(AWARD_CATEGORY,'') desc,date(EDUCATION_ACTIVITY_DATE) Desc,ATTEND_TO_DATE DESC) as Rnk
	  from `rpt_academics.v_previous_education` Prev_Edu
	  Join `PRC_RRT_Base_list_Comm` Base_List
		On Base_list.Applicant_ID = Prev_Edu.credential_id) Previous_Edu
Where Rnk = 1;



create or replace temp table `prc_rrt_base_list_comm_final_temp` as
with SRC as (select
	  'WLDN' as institution
	, 5 as institution_id
	, Base_List.*
	, Prev_Edu.INSTITUTION_TYPE_DESC as Prev_Institution_Type
	, Prev_Edu.AWARD_CATEGORY_DESC as Prev_Inst_Award_Category
	, Prev_Edu.INSTITUTION_DESC as Prev_Institution
	, Prev_Edu.SCHOOL_GPA as Prev_Inst_GPA
	, Prev_Edu.TOTAL_TRANSFER_CREDITS as Prev_Inst_TOC_to_Walden

from PRC_RRT_Base_list_Comm Base_List
left join PRC_RRT_Previous_Edu Prev_Edu
on Base_List.Applicant_ID=Prev_Edu.ID
)
select src.*
        -- ,job_start_dt as etl_created_date,
        -- job_start_dt as etl_updated_date,
        -- load_source as etl_resource_name,
        -- v_audit_key as etl_ins_audit_key,
        -- v_audit_key as etl_upd_audit_key,
        -- farm_fingerprint(format('%T', (Prev_Inst_GPA))) AS etl_pk_hash,  --------------------------------
        -- farm_fingerprint(format('%T', src )) as etl_chg_hash,
        FROM src;



-- merge process
--   call utility.sp_process_elt (institution, dml_mode , target_dataset, target_tablename, null, source_tablename, additional_attributes, out_sql );

	create or replace table `trans_academics.prc_rrt_base_list_comm_final`
	as select * from `prc_rrt_base_list_comm_final_temp` ;

    set job_end_dt = current_timestamp();
    set job_completed_ind = 'Y';

    -- export success audit log record
    call `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);

    set v_rrt_common_code = 'SUCCESS';


EXCEPTION WHEN error THEN

	SET job_end_dt = cast(NULL as TIMESTAMP);
	SET job_completed_ind = 'N';

	CALL `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);


	-- insert into error_log table
	insert into
	`audit_cdw_log.error_log` (error_load_key, process_name, table_name, error_details, etl_create_date, etl_resource_name, etl_ins_audit_key)
	values
	(v_audit_key,'FT_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;


	set v_rrt_common_code = @@error.message;

END;

END
