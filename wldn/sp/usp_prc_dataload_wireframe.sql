CREATE OR REPLACE PROCEDURE `tds_analytics_partner_portal.usp_prc_dataload_wireframe`()
OPTIONS (strict_mode=false)
BEGIN

delete from tds_analytics_partner_portal.wireframe_partner_portal where true;

INSERT INTO tds_analytics_partner_portal.wireframe_partner_portal
(
org_id,org_name,org_parent_id,b2b_account_id,b2b_account_name,walden_account_id,walden_account_name,b2b_parent_id,walden_parent_id,b2b_global_unique_id,walden_global_unique_id,institution,state_name,state,hospital,emp_brand_profile_c,emp_opportunity_c,opportunity_id,contact_c,eid,brand_profile_c,start_date_c,stage_name,stage_type,stage_alumni,is_alumni,disposition_c,primary_flag_c,banner_id_c,program_start_date_formula_c,app_started_date_time_c,program_start_year,application_submitted_c,anticipated_graduation_date_c,anticipated_graduation_date_text_c,current_program,program_group,location,fiscal_year,application_start_year,session_start_year,application_start_date,session_start_date,graduation_year,graduation_date_c,chamberlain_account_id,chamberlain_account_name,chamberlain_parent_id,dsi,record_insert_date
)

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
,fiscal_year,application_start_year,null,null,null,graduation_year,null,null,null,null,null,record_insert_date
from
tds_analytics_partner_portal.stage_wireframe_wu
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
graduation_date_c,chamberlain_account_id,chamberlain_account_name,chamberlain_parent_id,dsi,record_insert_date

from
tds_analytics_partner_portal.stage_wireframe_cu
where state is not null and location is not null;

END
