begin

declare institution string default 'WLDN';
declare institution_id int64 default 5;
declare dml_mode string default 'delete-insert';
declare target_dataset string default 'rpt_academics';
declare target_tablename string default 't_wldn_alumni_yoy_enrollments';
declare source_tablename string default 'wldn_alumni_yoy_enrollments_updated';
declare load_source string default 'trans_academics.sp_walden_alumni_yoy_enrollments';
declare additional_attributes ARRAY<struct<keyword string, value string>>;
declare last_refresh_time timestamp;
declare tgt_table_count int64;

/* common across */
declare job_start_dt timestamp default current_timestamp();
declare job_end_dt timestamp default current_timestamp();
declare job_completed_ind string default null;
declare job_type string default 'EDW';
declare load_method string default 'scheduled query';
declare out_sql string;
declare CurrentYrStart date;
declare PreviousYrStart date;

begin

SET additional_attributes= [("audit_load_key", v_audit_key),
          ("load_method",load_method),
          ("load_source",load_source),
          ("job_type", job_type)];
/* end common across */

/*Every year,Jan-June - CurrentYrStart is Previous Year and In July-Dec - CurrentYrStart is current year*/
/*Every year,Jan-June - PreviousYrStart is Previous to previous Year and In July-Dec - PreviousYrStart is previous year*/

set CurrentYrStart =
  case
    when extract(month from current_date()) in (1,2,3,4,5,6)
    then (cast(extract(year from current_date()-365) ||'-07-01' as date))
    else (cast(extract(year from current_date()) ||'-07-01' as date))
  end;

set PreviousYrStart =
  case
    when extract(month from current_date()) in (1,2,3,4,5,6)
    then (cast(extract(year from current_date()-730) ||'-07-01' as date))
    else (cast(extract(year from current_date()-365) ||'-07-01' as date))
  end;

-- set CurrentYrStart = '2022-07-01';
-- set PreviousYrStart = '2021-07-01';

create or replace temp table Max_Recon_Start_Date as
(
select max(start_date) as Latest_Start
from `rpt_academics.t_wldn_reconciled_list_cube`
where institution = 'WLDN'
and CBL_Flag is false ---- orginally 'N'
);
-- 2022-06-27

create or replace temp table Max_Recon_Start_LastYear as
(
select max(start_date) as Latest_Start
from `rpt_academics.t_wldn_reconciled_list_cube`
where start_date<=(select distinct Latest_Start from Max_Recon_Start_Date)-364
and institution = 'WLDN'
and CBL_Flag is false
);
-- 2021-06-28

create or replace temp table Max_Recon_Start_Date_Tempo as
(
select max(start_date) as Latest_Start
from `rpt_academics.t_wldn_reconciled_list_cube`
where institution like 'WLDN'
and  CBL_Flag is true
);

create or replace temp table Max_Recon_Start_LastYear_Tempo as
(
select max(start_date) as Latest_Start
from `rpt_academics.t_wldn_reconciled_list_cube`
where start_date<=(select distinct Latest_Start from Max_Recon_Start_Date_Tempo )-364
and institution like 'WLDN'
and CBL_Flag is true
);


create or replace temp table CR_List_CY as
(
select
distinct a.Applicant_Id as Student_ID
,cast(a.Student_Start_Dt as date) as Start_Date
,extract(year from a.Student_Start_Dt) as Year
,case
  when (a.Student_Start_Dt) = '2020-06-29' then 3
  when extract(month from a.Student_Start_Dt) in (1,2,3) then 1
  when extract(month from a.Student_Start_Dt) in (4,5,6) then 2
  when extract(month from a.Student_Start_Dt) in (7,8,9) then 3
else 4 end as Quarter
,extract(Month from a.Student_Start_Dt) as Month
,adin.international_flag
,a.Academic_Program as Product_Nbr
,cast(m.Level_Description as string) as Level_Desc
 ,m.College_code as college_name
 ,m.program_name as program_name
 ,banner_conc_desc as specialization
 ,e.Channel_c as Channel
,e.Activity_id_c
,c.last_name
,c.first_name
,a.Main_Email as Email
,a.Personal_Email
,a.Phone_Nbr_Home
,a.State
,a.Country
,a.Oppsfid
,false as CBL_Flag
,a.AdmRepName AS EA_Name
,a.Manager AS EA_Manager
,a.EA_Location,
'Reserves' as List
,'Current' as Period
,a.Application_ID
,d.brand_profile_c
,d.contact_c
,a.etl_created_date as processdt
 from `rpt_academics.t_wldn_reserve_list` a
 left join `rpt_academics.t_person` c on a.Applicant_Id = c.credential_id
 left join `raw_b2c_sfdc.opportunity` d on a.Oppsfid=d.id and d.institution_c = 'a0ko0000002BSH4AAO' and d.is_deleted=false
 left join `rpt_academics.t_wldn_product_map` m on  a.Academic_Program=m.Product_Nbr
 left join `raw_b2c_sfdc.campaign` e on d.campaign_id=e.id and e.institution_c = 'a0ko0000002BSH4AAO' and e.is_deleted= false
 left join rpt_academics.v_address_international adin on adin.credential_id=a.Applicant_Id and adin.institution_id=5

where extract(date from a.etl_created_date) = current_date()
and a.Selected_Quarter in ('Previous Quarter','Present Quarter','Future Quarter')
and cast(a.Student_Start_Dt as date) > (select distinct Latest_Start from Max_Recon_Start_Date)
and cast(a.Student_Start_Dt as date) >= CurrentYrStart
);


create or replace temp table CR_List_LY as
(
select
distinct a.Applicant_Id as Student_ID
,cast(a.Student_Start_Dt as date) as Start_Date
,extract(year from a.Student_Start_Dt) as Year
,case
  when (a.Student_Start_Dt) = '2020-06-29' then 3
  when extract(month from a.Student_Start_Dt) in (1,2,3) then 1
  when extract(month from a.Student_Start_Dt) in (4,5,6) then 2
  when extract(month from a.Student_Start_Dt) in (7,8,9) then 3
else 4 end as Quarter
,extract(Month from a.Student_Start_Dt) as Month
,adin.international_flag
,a.Academic_Program as Product_Nbr
,cast(m.Level_Description as string) as Level_Desc
 ,m.College_code as college_name
 ,m.program_name as program_name
 ,banner_conc_desc as specialization
 ,e.Channel_c as Channel
,e.Activity_id_c
,c.last_name
,c.first_name
,a.Main_Email as Email
,a.Personal_Email
,a.Phone_Nbr_Home
,a.State
,a.Country
,a.Oppsfid
,false as CBL_Flag
,a.AdmRepName AS EA_Name
,a.Manager AS EA_Manager
,a.EA_Location,
'Reserves' as List
,'Previous' as Period
,a.Application_ID
,d.brand_profile_c
,d.contact_c
,a.etl_created_date as processdt
from `rpt_academics.t_wldn_reserve_list` a
 left join `rpt_academics.t_person` c on a.Applicant_Id = c.credential_id
 left join `raw_b2c_sfdc.opportunity` d on a.Oppsfid=d.id and d.institution_c = 'a0ko0000002BSH4AAO' and d.is_deleted= false
 left join `rpt_academics.t_wldn_product_map` m on  a.Academic_Program=m.Product_Nbr
 left join `raw_b2c_sfdc.campaign` e on d.campaign_id=e.id and e.is_deleted=false and e.institution_c = 'a0ko0000002BSH4AAO'
 left join rpt_academics.v_address_international adin on adin.credential_id=a.Applicant_Id and adin.institution_id=5
 where
a.Selected_Quarter in ('Previous Quarter','Present Quarter','Future Quarter')

and cast(a.Student_Start_Dt as date) > (select distinct Latest_Start from Max_Recon_Start_LastYear)
and cast(a.Student_Start_Dt as date) >= PreviousYrStart
and cast(a.etl_created_date as date)= current_date()-364
);

create or replace temp table CR_LIST as
(
  select * from CR_List_LY
  union all
select * from CR_List_CY
);

create  or replace temp table Recon_List_YTD as
(
select distinct Student_ID
,Start_Date
,year
,case when Start_Date='2020-06-29' then 3 else Quarter end as Quarter
,Month
,International_Flag
,Product_Nbr
,level_desc
,college_name
,PROGRAM_NAME
,Specialization
,Channel
,ActivityID
,Last_Name
,First_Name
,walden_email
,Personal_Email
,home_phone_c
,State,Country
,opportunity_id
,CBL_Flag
,EA_Name
,EA_Manager
,EA_Location
,'Reconciled' as List
,'Current' as Period
, cast(null as string) as applicant_id
,brand_profile_c
,contact_id as contact_c
, cast(null as timestamp) as processdt
from `rpt_academics.t_wldn_reconciled_list_cube`
where Institution = 'WLDN'
and Start_Date >= CurrentYrStart
and Start_Date <= (select distinct Latest_Start from Max_Recon_Start_Date)and CBL_Flag is false
);

create  or replace temp table Recon_List_LY as
(
select distinct Student_ID
,Start_Date
,year
,case when Start_Date='2020-06-29' then 3 else Quarter end as Quarter
,Month
,International_Flag
,Product_Nbr
,level_desc
,college_name
,PROGRAM_NAME
,Specialization
,Channel
,ActivityID
,Last_Name
,First_Name
,walden_email
,Personal_Email
,home_phone_c
,State,Country
,opportunity_id
,CBL_Flag
,EA_Name
,EA_Manager
,EA_Location
,'Reconciled' as List
,'Previous' as Period
, cast(null as string) as applicant_id
,brand_profile_c
,contact_id as contact_c
, cast(null as timestamp) as processdt
from `rpt_academics.t_wldn_reconciled_list_cube`
where Institution = 'WLDN'
and Start_Date>=  PreviousYrStart
and Start_Date<=
(select distinct Latest_Start from Max_Recon_Start_LastYear)and CBL_Flag is false
);

create or replace temp table Total as
(

	select * from Recon_List_YTD
	Union ALL
	select * from Recon_List_LY
	Union ALL
	select * from CR_LIST
);

create or replace temp table Alumni_List as
(
select distinct
id
,start_date1 as start_date1
,OUTCOME_GRADUATION_DATE1
,program1
,program1_name,
FIRST_CONCENTRATION_DESC1
,degree_level1
,college1,cert_flag1
,graduation_flag1
,oc_category1,
outcome_grad_year1
,cohort1,prog_order,years,quarters,Latest_r_date
from `rpt_academics.t_wldn_alumni`
where graduation_flag1=1 and enrollment_flag1=1 and cert_flag1=0
and OUTCOME_GRADUATION_DATE1< CURRENT_DATE()
);

create or replace temp table Alumni_List_FirstGrad AS
(
select *except(rn) from (
select *,
row_number() over(partition by id order by id, outcome_graduation_date1) as rn
 from Alumni_List)
where rn = 1
);

create or replace temp table Alumni_List_LatestGrad AS
(
select *except(rn) from (
select *,
row_number() over(partition by id order by id, outcome_graduation_date1 desc) as rn
 from Alumni_List)
where rn = 1
);

create or replace temp table All_Data as
(
select
distinct a.*, 1 as Students
,b.graduation_flag1
,b.OUTCOME_GRADUATION_DATE1 as First_Grad_Dt
,b.program1_name as First_Graduated_Program
,c.OUTCOME_GRADUATION_DATE1 as Latest_Grad_Dt
,c.program1_name as Latest_Graduated_Program
,c.degree_level1 as Latest_Graduated_Level
,c.college1 as Latest_Graduated_College
,case when b.graduation_flag1=1 and a.start_Date>b.OUTCOME_GRADUATION_DATE1
then 1 else 0 end as Alumni_Flag
from Total a
left join Alumni_List_FirstGrad b on a.student_id=b.id
left join Alumni_List_LatestGrad c on a.student_id=c.id
);

create or replace temp table Course_Output AS
(select distinct
Student_ID,Start_Date,Year,Quarter,Month,International_Flag,
Product_Nbr,level_desc,college_name,PROGRAM_NAME,Specialization
,case when CBL_Flag is true then 'Y' else 'N'end  as CBL_Flag
,Channel,
ActivityID,	State,Country,opportunity_id,Students,First_Grad_Dt,First_Graduated_Program,
Latest_Grad_Dt,Latest_Graduated_Program,
Latest_Graduated_Level,Latest_Graduated_College,
Alumni_Flag,
EA_Name,EA_Manager,EA_Location,
List
,processdt as ReserveList_Date
,Period
,brand_profile_c
,contact_c
from All_Data
);

create or replace temp table Recon_List_CBL_YTD AS
(
select
distinct
 a.Student_ID
 ,b.Banner_id_c as BannerID_Opp
 ,b.brand_profile_c
 ,b.contact_c
 ,a.Start_Date
 ,a.year
 ,a.Quarter
 ,a.Month
 ,a.International_Flag
 ,a.Product_Nbr
 ,a.level_desc
 ,a.college_name
 ,a.PROGRAM_NAME
 ,a.Specialization
 ,a.Channel
 ,a.ActivityID
 ,a.Last_Name
 ,a.First_Name
 ,a.walden_email
 ,a.Personal_Email
 ,a.home_phone_c
 ,a.State
 ,a.Country
 ,a.opportunity_id
 ,case when a.CBL_Flag is true then 'Y' else 'N' end as CBL_flag
 ,a.EA_Name,a.EA_Manager,a.EA_Location,
'Reconciled' as List
,'Current' as Period
, cast(null as date) as processdt
from `rpt_academics.t_wldn_reconciled_list_cube`  a
left join `raw_b2c_sfdc.opportunity` b on a.opportunity_id=b.id  and b.institution_c="a0ko0000002BSH4AAO" and b.is_deleted=false
-- and b.institutiondimkey=3
and b.id is not null
where Institution like 'WLDN'
and Start_Date>= CurrentYrStart
and Start_Date<=
(select distinct Latest_Start from Max_Recon_Start_Date_Tempo)
and CBL_Flag is true
);

create or replace temp table Recon_List_CBL_LY AS
(
select
distinct
 a.Student_ID
 ,b.Banner_id_c as BannerID_Opp
 ,b.brand_profile_c
 ,b.contact_c
 ,a.Start_Date
 ,a.year
 ,a.Quarter
 ,a.Month
 ,a.International_Flag
 ,a.Product_Nbr
 ,a.level_desc
 ,a.college_name
 ,a.PROGRAM_NAME
 ,a.Specialization
 ,a.Channel
 ,a.ActivityID
 ,a.Last_Name
 ,a.First_Name
 ,a.walden_email
 ,a.Personal_Email
 ,a.home_phone_c
 ,a.State
 ,a.Country
 ,a.opportunity_id
 ,case when a.CBL_Flag is true then 'Y' else 'N' end as CBL_flag
 ,a.EA_Name,a.EA_Manager,a.EA_Location,
'Reconciled' as List
,'Previous' as Period
, cast(null as date) as processdt
from `rpt_academics.t_wldn_reconciled_list_cube`  a
left join `raw_b2c_sfdc.opportunity` b on
a.opportunity_id=b.id and  b.institution_c="a0ko0000002BSH4AAO" and b.is_deleted=false
-- and b.institutiondimkey=3
and b.id is not null
where Institution like 'WLDN'
and Start_Date>= PreviousYrStart
and Start_Date<=
(select distinct Latest_Start from Max_Recon_Start_LastYear_Tempo)
and CBL_Flag is true
);


create or replace temp table Reserves_Pipeline_CBL_YTD as
(
select
distinct case when a.banner_id is not null then a.Banner_ID else a.brand_profile_sfid end as Student_ID,
a.Banner_ID as BannerID_Opp,
a.brand_profile_sfid as brand_profile_c,
a.contact_sfid as contact_c,
a.intended_start_date as Start_Date,
extract(year from a.intended_start_date) as Year,
case
when extract(month from a.intended_start_date)<=3 then 1
when extract(month from a.intended_start_date) in (4,5,6) then 2
when extract(month from a.intended_start_date) in (7,8,9) then 3
when extract(month from a.intended_start_date)>=10 then 4 else 0 end as Quarter,
extract(month from a.intended_start_date) as Month,
case when a.International_Flag is false then 'N' when a.International_Flag is true then 'Y' else ' ' end as International_Flag,
a.curr_product_nbr as Product_Nbr,
cast(a.Level as string) as Level_Desc,
a.college_code as college_description
,a.program_name as Program_Name
,cast(null as string) as Specialization
,a.Channel
,a.Activity_Id
,b.first_name_c as first_name
,b.last_name_c as last_name
,b.University_email_c as Email ------***
,b.Personal_email_c
,b.home_phone_c
,c.state_cd
,a.Country_Name as Country
,a.opp_sfid
,Case when a.is_tempo_flag is true then 'Y' else 'N' end as CBL_Flag,
case when a.es_curr_name='Unknown' then a.es_orig_name else a.es_curr_name end as EA_Name,
case when a.es_curr_manager_name='Unknown' then a.es_orig_manager_name else a.es_curr_manager_name end as EA_Manager,
case when a.es_curr_site='Unknown' then a.es_orig_site else a.es_curr_site end as EA_Location,
'Reserves' as List
,a.period
,cutoff_date as processdt
from `rpt_crm_mart.t_wldn_opp_pipeline` a
left join `raw_b2c_sfdc.brand_profile_c` b on a.brand_profile_sfid = b.id and  b.is_deleted=false and b.institution_c='a0ko0000002BSH4AAO'
left join rpt_crm_mart.t_wldn_opp_snapshot c on a.opp_sfid=c.opp_sfid
where
a.period='Current'
and a.is_tempo_flag is true
and a.last_stage in ('Pre-enroll','Student')
and a.intended_start_date>
(select distinct Latest_Start from Max_Recon_Start_Date_Tempo)
and a.intended_start_date >= CurrentYrStart
);


create or replace temp table Reserves_Pipeline_CBL_LY as
(
select
distinct case when a.banner_id is not null then a.Banner_ID else a.brand_profile_sfid end as Student_ID,
a.Banner_ID as BannerID_Opp,
a.brand_profile_sfid as brand_profile_c,
a.contact_sfid as contact_c,
a.intended_start_date as Start_Date,
extract(year from a.intended_start_date) as Year,
case
when extract(month from a.intended_start_date)<=3 then 1
when extract(month from a.intended_start_date) in (4,5,6) then 2
when extract(month from a.intended_start_date) in (7,8,9) then 3
when extract(month from a.intended_start_date)>=10 then 4 else 0 end as Quarter,
extract(month from a.intended_start_date) as Month,
case when a.International_Flag is false then 'N' when a.International_Flag is true then 'Y' else ' ' end as International_Flag
,a.curr_product_nbr as Product_Nbr
,cast(a.Level as string) as Level_Desc
,a.college_code as college_description
,a.program_name as Program_Name
,cast(null as string) as Specialization
,a.Channel
,a.Activity_Id
,b.first_name_c as first_name
,b.last_name_c as last_name
,b.University_email_c as Email ------***
,b.Personal_email_c
,b.home_phone_c
,c.state_cd
,a.Country_Name as Country
,a.opp_sfid
,Case when a.is_tempo_flag is true then 'Y' else 'N' end as CBL_Flag,
case when a.es_curr_name='Unknown' then a.es_orig_name else a.es_curr_name end as EA_Name,
case when a.es_curr_manager_name='Unknown' then a.es_orig_manager_name else a.es_curr_manager_name end as EA_Manager,
case when a.es_curr_site='Unknown' then a.es_orig_site else a.es_curr_site end as EA_Location,
'Reserves' as List
,a.period
,cutoff_date as processdt

from `rpt_crm_mart.t_wldn_opp_pipeline` a
left join `raw_b2c_sfdc.brand_profile_c` b on a.brand_profile_sfid = b.id and b.is_deleted=false and b.institution_c='a0ko0000002BSH4AAO'
left join rpt_crm_mart.t_wldn_opp_snapshot c on a.opp_sfid=c.opp_sfid
where
a.period='Previous'
and a.is_tempo_flag is true
and a.last_stage in ('Pre-enroll','Student')
and a.intended_start_date>
(select distinct Latest_Start from Max_Recon_Start_LastYear_Tempo)
and a.intended_start_date >= PreviousYrStart
);


create or replace temp table Total_CBL as
(
 select * from Recon_List_CBL_LY
 Union All
 select * from Recon_List_CBL_YTD
 union ALL
 select * from Reserves_Pipeline_CBL_LY
 union all
select * from Reserves_Pipeline_CBL_YTD
);

create or replace temp table Email_Match_temp as
(
select distinct a.*,
b.credential_id as BannerID_PersonalEmail,
b2.credential_id as BannerID_Email,
case
when a.BannerID_opp like 'A%' then a.BannerID_opp
when a.BannerID_opp not like 'A%' and b.credential_id is null then b2.credential_id
else b.credential_id end as BannerID
from Total_CBL a
left join `rpt_academics.t_person` b
  on a.Personal_Email is not null
  and a.Personal_Email=b.email_preferred_address and b.email_preferred = 'PERS' and b.institution_id=5
left join `rpt_academics.t_person` b2
  on a.Walden_email is not null
  and a.Walden_email=b2.email_preferred_address and  b2.email_preferred ='UNIV' and  b2.institution_id=5
);

create or replace temp table Email_Match AS
( select * from
	(
	select *,
	row_number() over(Partition by Start_Date, Walden_Email, Personal_Email,Student_ID order by
	 BannerID,BannerID_Email, Start_Date, Walden_Email, Personal_Email) as rn
	from
	Email_Match_temp
	) where rn = 1
);

create or replace temp table CBL_Recon_Output as
(
select
a.BannerID as Student_ID
,a.Start_Date
,a.year
,a.Quarter
,a.Month
,a.International_Flag
,a.Product_Nbr
,a.level_desc
,a.college_name
,a.PROGRAM_NAME
,a.Specialization
,a.CBL_Flag
,a.Channel
,a.ActivityID
,a.State
,a.Country
,a.opportunity_id
,1 as Students
,b.OUTCOME_GRADUATION_DATE1 as First_Grad_Dt
,b.program1_name as First_Graduated_Program
,c.OUTCOME_GRADUATION_DATE1 as Latest_Grad_Dt
,c.program1_name as Latest_Graduated_Program
,c.degree_level1 as Latest_Graduated_Level
,c.college1 as Latest_Graduated_College
,case when b.graduation_flag1=1 and a.start_Date>b.OUTCOME_GRADUATION_DATE1
then 1 else 0 end as Alumni_Flag
,a.EA_Name
,a.EA_Manager
,a.EA_Location
,a.List
,cast(processdt as timestamp) as reserved
,Period
,a.brand_profile_c
,a.contact_c

from Email_Match a
left join Alumni_List_FirstGrad b
	on a.BannerID=b.id
	and a.bannerid is not null
left join Alumni_List_LatestGrad c
	on a.BannerID=c.id
	and a.bannerid is not null
);

create or replace temp table alumni_yoy_enrollments AS(
select * from  Course_Output
Union ALL
select * from  CBL_Recon_Output);


create or replace temp table  partnership_accounts as (
select distinct
  a1.id as partnersfid
  ,a1.name as partnername
  ,coalesce (a8.name,a7.name,a6.name,a5.name,a4.name,a3.name,a2.name,a1.name) as partner_account_l1
  ,a1.parent_id
  ,a2.name as parent_name1
  ,a3.name as parent_name2
  ,a4.name as parent_name3
  ,a5.name as parent_name4
  ,a6.name as parent_name5
  ,a7.name as parent_name6
  ,a8.name as parent_name7
-- into #partneraccounts
 from     `raw_b2c_sfdc.account` a1
left join `raw_b2c_sfdc.account` a2 on a1.parent_id=a2.id
left join `raw_b2c_sfdc.account` a3 on a2.parent_id=a3.id
left join `raw_b2c_sfdc.account` a4 on a3.parent_id=a4.id
left join `raw_b2c_sfdc.account` a5 on a4.parent_id=a5.id
left join `raw_b2c_sfdc.account` a6 on a5.parent_id=a6.id
left join `raw_b2c_sfdc.account` a7 on a6.parent_id=a7.id
left join `raw_b2c_sfdc.account` a8 on a7.parent_id=a8.id
where a1.id is not null
and  a1.is_deleted=false
and a1.institution_brand_c ='a0ko0000002BSH4AAO'
);

create or replace temp table students as
(
select distinct
  r.student_id
  ,r.contact_c as contact_id
  ,r.opportunity_id
  ,r.start_date
  ,r.year
  --,r.istempo
  ,r.channel as nonhybrid_channel
  ,brand_profile_c
  ,b.partnername as partner_nonhybrid
  ,b.partner_account_l1 as partner_account_l1_nonhybrid
from alumni_yoy_enrollments r
left join `raw_b2c_sfdc.campaign` c
  on r.ActivityID=c.activity_id_c
left join partnership_accounts b
  on c.partner_account_c = b.partnersfid
where extract(year from r.start_date)>=2017
and c.is_deleted=false and c.institution_c = 'a0ko0000002BSH4AAO'
);

create or replace temp table employers_bd as
(
select * from (
select distinct
  r.student_id
  ,r.brand_profile_c
  ,b.employer_name_c
  ,b.are_you_currently_employed_here_c
  ,b.bd_partner_c
  ,b.bd_account_c
  ,b.from_date_c
  ,b.to_date_c
  ,b.created_date
  ,1 as bd_employer
  ,d.partnername as bd_partner_account
  ,d.partner_account_l1 as parent_account_employer
  , row_number() over(partition by r.brand_profile_c,student_id order by created_date desc, from_date_c desc) as rn
from students r
left join `raw_b2c_sfdc.employment_c` b
  on r.brand_profile_c = b.brand_profile_c
left join partnership_accounts d
  on b.bd_account_c = d.partnersfid /*using linked partner/top tier parent from above*/
/*left join biadm.walden_campaign_partner_detail c on b.bdaccount=c.partnersfid*/
where b.are_you_currently_employed_here_c is True
and b.bd_partner_c is true
) where rn = 1
);

create or replace temp table sf_case_partnershipname as
(
  select * from (
select distinct
   a.banner_id_c as bannerid
  ,a.contact_id
  ,a.subtype_c
  ,a.bd_partner_account_lookup_c
  ,b1.partnername as bd_partner_account
  ,b1.partner_account_l1 as parent_account_discount
  ,a.start_date_c
  ,a.created_date
  ,row_number() over(partition by banner_id_c ,start_date_c order by a.created_date desc) as rn
from `raw_b2c_sfdc.case` a
left join partnership_accounts b1
  on a.bd_partner_account_lookup_c=b1.partnersfid
where a.subtype_c='BD Partnership Tuition Reduction'
and extract(year from a.start_date_c)>=2021
and is_deleted=false and institution_brand_c = 'a0ko0000002BSH4AAO' -- rs - filter added
) where rn = 1
);

 create or replace temp table partner_discounts_sf as (
select distinct
  r.student_id
  ,r.start_date
  ,r.contact_id
  ,r.brand_profile_c
  ,case when b.contact_id is not null or b2.contact_id is not null then 1 else 0 end as bd_discount
  ,case when b.contact_id is not null then b.bd_partner_account else b2.bd_partner_account end as partnership_discount
  ,case when b.contact_id is not null then b.parent_account_discount else b2.parent_account_discount end as parent_account_discount
  ,case when b.contact_id is not null then b.bd_partner_account_lookup_c else b2.bd_partner_account_lookup_c end as bd_account_disc
from students r

/*join on start date as first priority*/
left join sf_case_partnershipname b
  on r.contact_id=b.contact_id
  and r.start_date = cast(b.start_date_c as date)
/*join on or after start date as catchall*/
left join sf_case_partnershipname b2
  on r.contact_id = b2.contact_id
  and r.start_date >= cast(b2.start_date_c as date)
where r.start_date >='2021-07-01'
and (
    b.contact_id is not null
      or
    b2.contact_id is not null
    )
);

create or replace temp table partner_discounts as
(
  select * from
  (
	select distinct
	  r.student_id
	  ,r.start_date
      ,r.contact_id
	  ,r.brand_profile_c
	  ,s.bd_discount as bd_discount
	  ,s.partnership_discount as partnership_discount
	  ,s.parent_account_discount as parent_account_discount
	  ,s.bd_account_disc
	  ,s.bd_discount as bd_discount_sf
	  /**dedupe to ensure only one partner/parent name per bannerid/start date ***/
	  ,row_number() over(partition by r.student_id, r.start_date) as rn
	from students r
	left join partner_discounts_sf s
    on
    r.contact_id=s.contact_id

    and r.start_date=s.start_date
    and r.start_date >= '2021-07-01' -----parse_date("%d%h%y", '01aug21')
	where s.bd_discount=1
	)
	where rn = 1
);

create or replace temp table bd_opps as (
select * from
	(
	select distinct
	  r.student_id
	  ,r.start_date
	  ,r.brand_profile_c
	  ,r.nonhybrid_channel
	  ,r.partner_nonhybrid
	  ,r.partner_account_l1_nonhybrid
	  ,a.inq_date
	  ,a.opp_sfid
	  ,a.channel
	  ,a.activity_id
	  ,b.partner_account_c
	  ,1 as bd_channel
	  ,d.partnername
	  ,d.partner_account_l1
	  /**sort and de-dupe by latest inq date**/
	  ,row_number() over(partition by r.brand_profile_c, r.start_date order by inq_date desc) as rn
	from students r
	left join `rpt_crm_mart.t_wldn_opp_snapshot` a
	on r.brand_profile_c = a.brand_profile_sfid
	and r.start_date>=a.inq_date
	left join `raw_b2c_sfdc.campaign` b on a.activity_id=b.activity_id_c
	left join partnership_accounts d
	on b.partner_account_c=d.partnersfid
	and b.partner_account_c is not null
	where a.channel like 'Business%'
  and b.is_deleted=false and b.institution_c = 'a0ko0000002BSH4AAO'
	)
	where rn = 1
);

create or replace temp table bd_opps_partner as
(
select distinct
  *
  ,case
    when nonhybrid_channel like 'Business%' and partner_nonhybrid not in ('','Inbound Call') then partner_nonhybrid
    when nonhybrid_channel not like 'Business%' /*and Hybrid_Channel not like 'Business%' */
then partnername
    else partnername end as partner_channel
  ,case
    when nonhybrid_channel like 'Business%' and partner_nonhybrid not in ('','Inbound Call') then partner_account_l1_nonhybrid
    when nonhybrid_channel not like 'Business%' then partner_account_l1
    else partner_account_l1 end as partner_account_l1_channel


from bd_opps
);

create or replace temp table alldata as
(
select distinct
  a.*
  ,e.bd_partner_account as partner_employer
  ,e.parent_account_employer as partner_account_l1_employer
  ,e.bd_partner_c
  ,case
    when e.bd_employer=1 then e.bd_employer
    else 0 end as bd_employer
  ,d.partnership_discount
  ,d.parent_account_discount
  ,case
    when d.bd_discount=1 then d.bd_discount
    else 0 end as bd_discount
    ,c.channel
    ,c.partner_channel
    ,c.partner_account_l1_channel
    ,case
      when c.bd_channel=1 then c.bd_channel
      else 0 end as bd_channel
    ,case
      when e.bd_employer=1 or d.bd_discount=1 or c.bd_channel=1 then 1
      else 0 end as bd_partner_enrollment_flag
      ,e.bd_account_c as emp_partnerid
      ,d.bd_account_disc as disc_partnerid
      ,c.partner_account_c as chan_partnerid
from students a
left join employers_bd e on a.brand_profile_c = e.brand_profile_c
left join partner_discounts d on /*a.student_id=d.student_id*/ a.contact_id=d.contact_id and a.start_date=d.start_date
left join bd_opps_partner c on a.brand_profile_c=c.brand_profile_c and a.start_date=c.start_date
);

create or replace temp table BD_Enrollments_Data as
(
	select distinct
	  *
	  ,concat(bd_employer,bd_channel,bd_discount) as bd_source_total
	  ,case
		when bd_employer=1 then partner_employer
		when bd_discount=1 and partnership_discount<>'' and  parent_account_discount<>'' and bd_employer=0 then partnership_discount
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='') and bd_employer=0 and bd_channel=1 and partner_channel<>'' then partner_channel
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='') and bd_employer=0 and bd_channel=1 and partner_channel='' then partnership_discount
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='') and bd_employer=0 and bd_channel=0 then partnership_discount
		when bd_discount=0 and bd_employer=0 and bd_channel=1 then partner_channel
		else ''
	  end as partnership_name

	  ,case
		when bd_employer=1 then partner_account_l1_employer
		when bd_discount=1 and partnership_discount<>'' and  parent_account_discount<>'' and bd_employer=0 then parent_account_discount
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='') and bd_employer=0 and bd_channel=1 and partner_channel<>'' then partner_account_l1_channel
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='') and bd_employer=0 and bd_channel=1 and partner_channel='' then parent_account_discount
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='') and bd_employer=0 and bd_channel=0 then parent_account_discount
		when bd_discount=0 and bd_employer=0 and bd_channel=1 then partner_account_l1_channel
		else ''
	  end as parent_account

	  , case
		when bd_employer=1 then 'Employer'
		when bd_discount=1 and partnership_discount<>'' and  parent_account_discount<>''
		and bd_employer=0 then 'Discount'
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='')
		and bd_employer=0 and bd_channel=1 and partner_channel<>'' then 'Channel'
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='')
		and bd_employer=0 and bd_channel=1 and partner_channel='' then 'Discount'
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='')
		and bd_employer=0 and bd_channel=0 then 'Discount'
		when bd_discount=0 and bd_employer=0 and bd_channel=1 then 'Channel'
		else '' end as partnership_name_source

	  ,case
		when bd_employer=1 then emp_partnerid
		when bd_discount=1 and partnership_discount<>'' and  parent_account_discount<>''
		and bd_employer=0 then disc_partnerid
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='')
		and bd_employer=0 and bd_channel=1 and partner_channel<>'' then chan_partnerid
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='')
		and bd_employer=0 and bd_channel=1 and partner_channel='' then disc_partnerid
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='')
		and bd_employer=0 and bd_channel=0 then disc_partnerid
		when bd_discount=0 and bd_employer=0 and bd_channel=1 then chan_partnerid
		else '' end as partnersfid

	from alldata
	where bd_partner_enrollment_flag=1
);

create or replace temp table wldn_alumni_yoy_enrollments_updated as
(with src as (
select
	distinct a.*
	,COALESCE(rcl.bd_partner_enrollment_flag,b.BD_Partner_Enrollment_Flag) as bd_partner_enrollment_flag
	,COALESCE(rcl.partnersfid,b.PartnerSFID) as partnersfid
	,COALESCE(rcl.partnership_name,b.Partnership_Name) as partnership_name
	,COALESCE(rcl.parent_account,b.Parent_Account) as parent_account
	,COALESCE(rcl.partnership_name_source,b.Partnership_Name_Source) as partnership_name_source
	,COALESCE(rcl.bd_discount,b.BD_Discount) as bd_discount
	,COALESCE(rcl.bd_channel,b.BD_Channel) as bd_channel
	,COALESCE(rcl.bd_employer,b.BD_Employer) as bd_employer
from alumni_yoy_enrollments a
left join BD_Enrollments_Data b
on a.Student_ID=b.Student_ID
and a.brand_profile_c = b.brand_profile_c
and a.Start_Date=b.Start_Date
left join rpt_academics.t_wldn_reconciled_list_cube rcl on a.brand_profile_c = rcl.brand_profile_c and a.Start_Date=rcl.start_date
and a.brand_profile_c  is not null
)
select src.*,
		5 as institution_id,
		'WLDN' as Institution,
 		job_start_dt as etl_created_date,
        job_start_dt as etl_updated_date,
        load_source as etl_resource_name,
        v_audit_key as etl_ins_audit_key,
        v_audit_key as etl_upd_audit_key,
        farm_fingerprint(format('%T', (src.Student_ID, src.Start_Date))) AS etl_pk_hash,
        farm_fingerprint(format('%T', src )) as etl_chg_hash,

from src
);

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
(v_audit_key,'EDW_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;


set result = @@error.message;
raise using message = @@error.message;

END;

END