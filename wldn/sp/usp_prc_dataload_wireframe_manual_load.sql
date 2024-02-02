CREATE OR REPLACE PROCEDURE `tds_analytics_partner_portal.usp_prc_dataload_wireframe_manual_load`()
OPTIONS (strict_mode=false)
BEGIN

delete from tds_analytics_partner_portal.stage_wireframe_cu_report where true;

insert into tds_analytics_partner_portal.stage_wireframe_cu_report (b2b_account_id,b2b_account_name
,chamberlain_account_id,chamberlain_account_name,b2b_parent_id,chamberlain_parent_id,b2b_global_unique_id
,institution,state,hospital,chamberlain_program,program_group,opportunity_id,contact_id,stage_name,stage_alumni
,is_alumni,stage_type,fiscal_year,application_start_year,session_start_year,application_start_date
,session_start_date,graduation_year,graduation_date_c,location,dsi,record_insert_date,stage_type_la
)

select
wi.Legacy_Employer_SFDC_ID as b2b_account_id,
wi.B2B_Organization_Name as b2b_account_name,
null as chamberlain_account_id,
null as chamberlain_account_name,
wi.B2B_Parent_ID as b2b_parent_id,
null as chamberlain_parent_id,
null as b2b_global_unique_id,
'Chamberlain' as institution,
wi.billing_state as state,
wi.B2B_Organization_Name as hospital,
wi.Program_Code as chamberlain_program,
--RIGHT(wi.Program, LENGTH(Program) - 2) as program_group,
trim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(wi.Program,'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9',''),'.','')) as program_group,
wi.Opportunity_ID as opportunity_id,
wi.Contact_ID as contact_id,
case when (wi.student_status_c='New' and wi.Stage = ('Closed - Started') and wi.Session_Start_Date is not null) then 'New'
     when (wi.student_status_c='Continuing' and wi.Stage in ('Closed - Started') and wi.Session_Start_Date is not null) then 'Continuing'
	   else null end as stage_name,
--(select student_status_c from tds_analytics_partner_portal.raw_cu_sfdc_contact c where c.id = wi.Contact_ID) as stage_name,
case when  wi.Is_Alumni = '1' and Stage in ('Closed - Started') and Person_Account_Graduation_Date is not null then 'Alumni'
     else null end as stage_alumni,
--case when wi.Is_Alumni = '1' then 'Alumni' else null end as stage_alumni,
case when wi.Is_Alumni = '1' then true else false end as is_alumni,
case When (Stage in ('Approved Application','Approved Carryover Application','Closed - Not Started','Closed - Expired') and Application_Date is not null) then 'Applying'
     When (Stage in ('Closed - Started') and Session_Start_Date is not null) then 'Students'
     else Stage
     end as stage_type,
null as fiscal_year,
cast(right(Application_Date,4)as int64) as application_start_year,
cast(right(Session_Start_Date,4)as int64) as session_start_year,
PARSE_DATE('%d-%m-%Y',Application_Date) as application_start_date,
PARSE_DATE('%d-%m-%Y',Session_Start_Date) as session_start_date,
cast(right(Person_Account_Graduation_Date,4)as int64) as graduation_year,
PARSE_DATE('%d-%m-%Y',Person_Account_Graduation_Date) as graduation_date_c,
Person_Account_Mailing_State_Province as location,
wi.DSI as dsi,
current_datetime() as record_insert_date,
case When (Stage in ('Approved Application','Approved Carryover Application') and Application_Date is not null) then 'Apply'  end as stage_type_la
from tds_analytics_partner_portal.wireframe_initial_cu_report wi;


--********************************************************
delete from tds_analytics_partner_portal.stage_wireframe_wu_report where true;

insert into tds_analytics_partner_portal.stage_wireframe_wu_report (
b2b_account_id
,b2b_account_name
,walden_account_id
,walden_account_name
,b2b_parent_id
,walden_parent_id
,b2b_global_unique_id
,walden_global_unique_id
,institution
,state
,hospital
,opportunity_id
,contact_c
,brand_profile_c
,emp_brand_profile_c
,emp_opportunity_c
,eid
,start_date_c
,stage_name
,stage_type
,stage_alumni
,disposition_c
,primary_flag_c
,banner_id_c
,program_start_date_formula_c
,application_submitted_c
,anticipated_graduation_date_c
,anticipated_graduation_date_text_c
,current_program_banner_code_c
,most_recent_inquiry_date_c
,app_started_date_time_c
,program_of_interest_c
,banner_program_code_c
,program_group
,location
,fiscal_year
,application_start_year
,program_start_year
,graduation_year
,record_insert_date, stage_type_la)
select
wi.B2B_Account_ID as b2b_account_id,
wi.B2B_Account_Name as b2b_account_name,
null as	walden_account_id,
null as walden_account_name,
wi.B2B_Parent_ID as 	b2b_parent_id,
null as	walden_parent_id,
wi.B2B_Global_Unique_ID as	b2b_global_unique_id,
null as	walden_global_unique_id,
'Walden' as	institution,
wi.State as	state,
wi.B2B_Account_Name as	hospital,
wi.Opportunity_CASESAFEID as opportunity_id,
wi.Contact_ID as	contact_c,
wi.Opportunity_Brand_Profile_Id as	brand_profile_c,
null as	emp_brand_profile_c,
null as emp_opportunity_c,
wi.EID as	eid,
null as start_date_c,
--wi.Opportunity_Stage as stage_name,
case when wi.Is_Alumni ='Alumni' then 'Alumni' else wi.Opportunity_Stage end as stage_name,
case when trim(wi.Opportunity_Stage) in ('Applicant') and wi.Primary_Flag is not null and wi.App_Started_Date_Time_c is not null then 'Applying'
 When trim(wi.Opportunity_Stage) in ('Pre-enroll','Paused','Pause','Student') and wi.Program_Start_Date_formula is not null then 'Students'
 when wi.Is_Alumni ='Alumni' and  wi.Graduation_Year is not null  then 'Alumni'
else wi.Opportunity_Stage end as stage_type,
wi.Is_Alumni	as stage_alumni,
wi.Opportunity_Disposition	 as disposition_c,
--case when wi.Primary_Flag  = 'True' then true else false end as primary_flag_c,
case when lower(wi.Primary_Flag)  = 'true' then true else false end as primary_flag_c, --primary_flag value to lower case
wi.Opportunity_Banner_Id	 as banner_id_c,
--wi.Program_Start_Date_formula as	program_start_date_formula_c,
PARSE_DATE('%d-%m-%Y',Program_Start_Date_formula) as program_start_date_formula_c,
null as application_submitted_c,
null as anticipated_graduation_date_c,
null as anticipated_graduation_date_text_c,
wi.Banner_Program_Code as current_program_banner_code_c,
null as	most_recent_inquiry_date_c,
null	as app_started_date_time_c,
null as	program_of_interest_c,
wi.Banner_Program_Code as banner_program_code_c,
trim(wi.Partner_Portal_Program_Name)	 as program_group,
wi.Location as	location,
null as	fiscal_year,
cast(right(wi.App_Started_Date_Time_c,4)as int64) as application_start_year,
cast(right(wi.Program_Start_Date_formula,4)as int64) as program_start_year,
cast(right(wi.Graduation_Year,4)as int64) as graduation_year,
current_datetime() as	record_insert_date,
null as stage_type_la
from tds_analytics_partner_portal.wireframe_initial_wu_report wi;

--*************************************************************************

delete from tds_analytics_partner_portal.wireframe_partner_portal_static where true;


INSERT INTO tds_analytics_partner_portal.wireframe_partner_portal_static(
org_id,org_name,org_parent_id,b2b_account_id,b2b_account_name,walden_account_id,walden_account_name,
b2b_parent_id,walden_parent_id,b2b_global_unique_id,walden_global_unique_id,institution,state_name,state,
hospital,emp_brand_profile_c,emp_opportunity_c,opportunity_id,contact_c,eid,brand_profile_c,start_date_c,
stage_name,stage_type,stage_alumni,is_alumni,disposition_c,primary_flag_c,banner_id_c,program_start_date_formula_c,
app_started_date_time_c,program_start_year,application_submitted_c,anticipated_graduation_date_c,anticipated_graduation_date_text_c,
current_program,program_group,location,fiscal_year,application_start_year,session_start_year,application_start_date,session_start_date,
graduation_year,graduation_date_c,chamberlain_account_id,chamberlain_account_name,chamberlain_parent_id,dsi,record_insert_date,stage_type_la)

select
b2b_account_id,b2b_account_name,b2b_parent_id,
b2b_account_id,b2b_account_name,walden_account_id,walden_account_name,
b2b_parent_id,walden_parent_id,b2b_global_unique_id,walden_global_unique_id,institution,state,
(SELECT name  FROM raw_b2c_sfdc.state_c
   WHERE (trim(state) = trim(name) or trim(state) = trim(iso_code_c)) and state is not null),hospital,
emp_brand_profile_c,emp_opportunity_c,opportunity_id,contact_c,eid,brand_profile_c,start_date_c,stage_name,stage_type,stage_alumni,null,
disposition_c,primary_flag_c,banner_id_c,program_start_date_formula_c,app_started_date_time_c,program_start_year,application_submitted_c,
anticipated_graduation_date_c,anticipated_graduation_date_text_c,
banner_program_code_c,program_group,
(SELECT name  FROM raw_b2c_sfdc.state_c
   WHERE (trim(location) = trim(name) or trim(location) = trim(iso_code_c)) and location is not null)
,fiscal_year,application_start_year,null,null,null,graduation_year,null,null,null,null,null,record_insert_date,stage_type_la
from
tds_analytics_partner_portal.stage_wireframe_wu_report
where state is not null and location is not null

union all

select
b2b_account_id,b2b_account_name,b2b_parent_id,b2b_account_id,b2b_account_name,
null,null,b2b_parent_id,null,b2b_global_unique_id,null,institution,state,
(SELECT name  FROM raw_b2c_sfdc.state_c
   WHERE (trim(state) = trim(name) or trim(state) = trim(iso_code_c)) and state is not null),
hospital,null,null,opportunity_id,contact_id,null,null,
null,stage_name,stage_type,stage_alumni,is_alumni,null,null,null,null,
null,null,null,null,null,chamberlain_program,program_group,
(SELECT name  FROM raw_b2c_sfdc.state_c
   WHERE (trim(location) = trim(name) or trim(location) = trim(iso_code_c)) and location is not null),
fiscal_year,application_start_year,session_start_year,application_start_date,session_start_date,graduation_year,
graduation_date_c,chamberlain_account_id,chamberlain_account_name,chamberlain_parent_id,dsi,record_insert_date,stage_type_la
from
tds_analytics_partner_portal.stage_wireframe_cu_report
where state is not null and location is not null;

END;
