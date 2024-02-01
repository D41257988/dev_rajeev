CREATE OR REPLACE PROCEDURE `trans_crm_mart.sp_wldn_opp_snapshot`(IN v_audit_key STRING, OUT result STRING)
begin

    declare institution string default 'WLDN';
    declare institution_id int64 default 5;
    declare dml_mode string default 'scd1';
    declare target_dataset string default 'rpt_crm_mart';
    declare target_tablename string default 't_wldn_opp_snapshot';
    declare source_tablename string default 'temp_opp_snapshot';
    declare load_source string default 'trans_crm_mart.sp_wldn_opp_snapshot';
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

create or replace temp table opp_status_history as (
  select *
  from `rpt_crm_mart.t_opp_status_history`
);
create or replace temp table brand_table as(
  with combined_table as (
  select
  o.id as opp_sfid,
  b.banner_id_c as banner_id,
  b.billing_country_c as billing_country ,
  b.billing_state_c as billing_state,
  b.billing_city_c as billing_city,
  b.billing_country_text_c as billing_country_text,
  b.billing_state_text_c as billing_state_text,
  b.billing_city_c as city,
  ct.name as country_name,
  st.name as state,
  r.mailing_country as mailing_country,
  c.activity_id_c as activity_id,
  c.channel_c as channel,
  c.name as campaign_name,
  o.selected_program_start_date_c as selected_program_start_date_id,
  s.start_date_c as selected_program_start_date,
  --s.start_date_c as start_date,
  COALESCE(o.contact_id,o.contact_c) as contact_sfid,
  r.name as contact_name,
  e.bd_account_c as bd_account_sfid,
  e.created_date,
  o.brand_profile_c as brand_profile_sfid,
  row_number() over(partition by o.id order by e.created_date desc) as rid,
  case when e.bd_partner_c=true or c.channel_c='Business Development' then true else false END AS partner_flag,
  case when o.military_affiliation_c = true or lower(b.active_us_military_c) in ('true','yes') or lower(b.military_veteran_c) in ('true','yes') or lower(b.is_has_your_spouse_been_in_us_military_c) in ('true','yes') THEN true
  ELSE false END AS military_flag,
  st.iso_code_c
  from `raw_b2c_sfdc.opportunity` o
  left join `raw_b2c_sfdc.brand_profile_c` b on o.brand_profile_c = b.id and b.is_deleted=false
  left join `raw_b2c_sfdc.campaign` c on o.campaign_id =c.id and c.is_deleted=false and c.institution_c = 'a0ko0000002BSH4AAO'
  left join `raw_b2c_sfdc.start_date_c` s on o.selected_program_start_date_c = s.id and s.is_deleted=false
  left join `raw_b2c_sfdc.contact` r on COALESCE(o.contact_id,o.contact_c) = r.id and r.is_deleted=false and lower(r.institution_code_c)='walden'
  left join `raw_b2c_sfdc.employment_c` e on  o.brand_profile_c = e.brand_profile_c and e.is_deleted=false and e.bd_partner_c=true
  left join `raw_b2c_sfdc.state_c` st on b.billing_state_c = st.id and st.is_deleted=false
  left join `raw_b2c_sfdc.country_c` ct on b.billing_country_c=ct.id
  where o.is_deleted=false and o.institution_c='a0ko0000002BSH4AAO'

)
  select *
  except(country_name,state),
  case when iso_code_c = 'MP' then 'Northern Mariana Islands'
  when iso_code_c = 'UM' then 'United States Minor Outlying Islands'
  when iso_code_c = 'VI' then 'Virgin Islands, U.S.' else state end as state,

  case when iso_code_c in ('AB','BC','MB','NB','NL','NS','NT','NU','ON','PE','QC','SK','YT','NF','PQ')
  and (country_name = 'United States' or country_name is null or country_name = 'Unknown' ) then 'Canada'
  when billing_state_text like 'Armed Forces%' OR iso_code_c in ( 'AA', 'AE', 'AP' ) then 'Military'
  when iso_code_c in ('AS','GU','MP','PR','UM','VI','PW','FM','MH','CM','CZ','RQ')
	and (country_name = 'United States' or country_name is null ) then state
  when country_name in ( 'USA', 'U.S.A.', 'US', 'U.S.', 'United States', 'United States of America' ) then 'United States of America'
  when (country_name is null and billing_country_text in ( 'USA', 'U.S.A.', 'US', 'U.S.', 'United States', 'United States of America' )) then 'United States of America'

  else country_name end as country_name
  from combined_table
  where rid=1
);

create or replace temp table curr_owner as (
select
l.id as opp_sfid,
l.institution_c as institution_id,
l.created_date,
--l.primary_flag_c as primary_flag,
COALESCE(r.id,'Unknown') as es_curr_sfid,
r.name as es_curr_name,
r.manager_id as es_curr_manager_sfid,
r.manager_name as es_curr_manager_name,
r.director_c as es_curr_director_sfid,
r.director_name as es_curr_director_name,
r.division as es_curr_division,
case when coalesce (track_stage_name_c, '1900-01-01') > coalesce(track_disposition_c,'1900-01-01')  then track_stage_name_c ELSE track_disposition_c END  AS current_stage_disposition_timestamp,
u.location_c as es_curr_site,
from `raw_b2c_sfdc.opportunity` l
left join `rpt_crm_mart.v_wldn_loe_enrollment_advisors` r
on l.owner_id = r.id
left join `raw_b2c_sfdc.user` u
on l.owner_id = u.id
where l.is_deleted=false and l.institution_c='a0ko0000002BSH4AAO'
);

create or replace temp table orig_owner as (
  with history_table as (
  select opp_sfid,
  owner_id,
  row_number() over(partition by opp_sfid
  order by created_date) as rid,
  from `rpt_crm_mart.v_opp_original_owner`
  where is_es_flag=1
),
filtered_table as (
  select * from history_table
  where rid=1
),
owner_table as (
  select
  l.id as opp_sfid,
  l.owner_id,
  m.owner_id as es_orig_sfid,
  institution_c,
  from `raw_b2c_sfdc.opportunity` l
  left join filtered_table m
  on l.id = m.opp_sfid
  where l.is_deleted=false and l.institution_c='a0ko0000002BSH4AAO'
)
select opp_sfid,
owner_id as owner_sfid,
es_orig_sfid,
r.name as es_orig_name,
r.manager_id as es_orig_manager_sfid,
r.manager_name as es_orig_manager_name,
r.director_c as es_orig_director_sfid,
r.director_name as es_orig_director_name,
r.division as es_orig_division,
u.location_c as es_orig_site,
from owner_table l
left join `rpt_crm_mart.v_wldn_loe_enrollment_advisors` r
on l.es_orig_sfid = r.id
left join `raw_b2c_sfdc.user` u
on l.es_orig_sfid = u.id
where institution_c = 'a0ko0000002BSH4AAO'
);

create or replace temp table curr_product as (
  select
l.id as opp_sfid,
l.program_of_interest_c as curr_product_sfid,
r.product_name as curr_product_name,
r.program_name as curr_program_name,
r.customer_friendly_poi_name as curr_customer_friendly_poi_name,
r.product_nbr as curr_product_nbr,
--r.product_nbr_bnr as curr_product_nbr_bnr,
r.program_group as curr_program_group,
r.college_code as curr_college_code,
r.college_description as curr_college_description,
r.degree_code as curr_degree_code,
r.concentration_code as curr_concentration_code,
r.concentration_description as curr_concentration_description,
r.level_description as curr_level,
r.ui_area_of_study as curr_ui_area_of_study,
r.SQ_flag as sq_flag,
r.is_tempo AS is_tempo_flag,
from `raw_b2c_sfdc.opportunity` l
--left join `stg_l1_salesforce.product_2` m
--on l.program_of_interest_c = m.id and m.is_deleted=false
left join `rpt_academics.t_wldn_product_map` r
on l.program_of_interest_c = r.product_sfid
where l.is_deleted=false and l.institution_c='a0ko0000002BSH4AAO'
);

create or replace temp table orig_product as (
with history_table as (
  select opp_sfid,
  program_of_interest_id,
  program_of_interest_name,
  row_number() over(partition by opp_sfid
  order by created_date) as rid,
  from `rpt_crm_mart.t_opp_program_history`
),
filtered_table as (
  select * from history_table
  where rid=1
),
product_table as (
  select
  l.id as opp_sfid,
  m.program_of_interest_id as orig_product_sfid,
  --m.program_of_interest_name as orig_product_name,

  from `raw_b2c_sfdc.opportunity` l
  left join filtered_table m
  on l.id = m.opp_sfid
  where l.is_deleted=false and l.institution_c='a0ko0000002BSH4AAO'
  )
select *,
  r.product_name as orig_product_name,
  r.program_name as orig_program_name,
  r.customer_friendly_poi_name as orig_customer_friendly_poi_name,
  r.product_nbr as orig_product_nbr,
  --r.product_nbr_bnr as orig_product_nbr_bnr,
  r.program_group as orig_program_group,
  r.college_code as orig_college_code,
  r.college_description as orig_college_description,
  r.degree_code as orig_degree_code,
  r.concentration_code as orig_concentration_code,
  r.concentration_description as orig_concentration_description,
  r.level_description as orig_level,
  r.ui_area_of_study as orig_ui_area_of_study,
  from product_table l
  left join `rpt_academics.t_wldn_product_map` r
  on l.orig_product_sfid = r.product_sfid
);
create or replace temp table sorted_flag_table as (
with
table_1 as (
  select * from (select opp_sfid,registered_flag,
  row_number() over(partition by opp_sfid order by created_date) as rid,
  from (select opp_sfid,created_date,case when disposition in ('Registered','Logged-In','Participated') THEN true ELSE false END AS registered_flag,
        from opp_status_history)
        where registered_flag=true)
  where rid=1
),
table_2 as (
  select * from (select opp_sfid,logged_in_flag,
  row_number() over(partition by opp_sfid order by created_date) as rid,
  from (select opp_sfid,created_date,case when disposition in ('Logged-In','Participated') THEN true ELSE false END AS logged_in_flag,
        from opp_status_history)
        where logged_in_flag=true)
  where rid=1
),
table_3 as (
  select * from (select opp_sfid,participated_flag,
  row_number() over(partition by opp_sfid order by created_date) as rid,
  from (select opp_sfid,created_date,case when disposition = 'Participated' THEN true ELSE false END AS participated_flag,
        from opp_status_history)
        where participated_flag=true)
  where rid=1
),
table_4 as (
  select * from (select opp_sfid,reserved_flag,
  row_number() over(partition by opp_sfid order by created_date) as rid,
  from (select opp_sfid,created_date,case when disposition = 'Reserved' THEN true ELSE false END AS reserved_flag,
        from opp_status_history)
        where reserved_flag=true)
  where rid=1
),
table_5 as (
  select * from (select opp_sfid,active_1st_term_flag,
  row_number() over(partition by opp_sfid order by created_date) as rid,
  from (select opp_sfid,created_date,case when disposition = 'Active (1st term)' THEN true ELSE false END AS active_1st_term_flag,
        from opp_status_history)
        where active_1st_term_flag=true)
  where rid=1
),
table_6 as (
  select * from (select opp_sfid,alumni_flag,
  row_number() over(partition by opp_sfid order by created_date) as rid,
  from (select opp_sfid,created_date,case when disposition = 'Alumni' THEN true ELSE false END AS alumni_flag,
        from opp_status_history)
        where alumni_flag=true)
  where rid=1
),
table_7 as (
  select * from (select opp_sfid,withdrawn_flag,
  row_number() over(partition by opp_sfid order by created_date) as rid,
  from (select opp_sfid,created_date,case when disposition = 'Withdrawn' THEN true ELSE false END AS withdrawn_flag,
        from opp_status_history)
        where withdrawn_flag=true)
  where rid=1
),
table_8 as (
  select * from (select opp_sfid,inactive_flag,
  row_number() over(partition by opp_sfid order by created_date) as rid,
  from (select opp_sfid,created_date,case when disposition = 'Inactive' THEN true ELSE false END AS inactive_flag,
        from opp_status_history)
        where inactive_flag=true)
  where rid=1
),
table_9 as (
  select * from (select opp_sfid,student_active_flag,
  row_number() over(partition by opp_sfid order by created_date) as rid,
  from (select opp_sfid,created_date,case when stage_disposition like 'Student-Active%' THEN true ELSE false END AS student_active_flag,
        from opp_status_history)
        where student_active_flag=true)
  where rid=1
)
select id as opp_sfid,
COALESCE(registered_flag, false) as registered_flag,
COALESCE(logged_in_flag, false) as logged_in_flag,
COALESCE(participated_flag, false) as participated_flag,
COALESCE(reserved_flag, false) as reserved_flag,
COALESCE(active_1st_term_flag, false) as active_1st_term_flag,
COALESCE(alumni_flag, false) as alumni_flag,
COALESCE(withdrawn_flag, false) as withdrawn_flag,
COALESCE(inactive_flag, false) as inactive_flag,
COALESCE(student_active_flag, false) as student_active_flag,
from `raw_b2c_sfdc.opportunity` l
left join table_1
on id = table_1.opp_sfid
left join table_2
on id = table_2.opp_sfid
left join table_3
on id = table_3.opp_sfid
left join table_4
on id = table_4.opp_sfid
left join table_5
on id = table_5.opp_sfid
left join table_6
on id = table_6.opp_sfid
left join table_7
on id = table_7.opp_sfid
left join table_8
on id = table_8.opp_sfid
left join table_9
on id = table_9.opp_sfid
where institution_c = 'a0ko0000002BSH4AAO'
and l.is_deleted=false
);

create or replace temp table sorted_timestamp_table as(
with
table_1 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when stage_disposition like 'Open-Uncontacted%' THEN created_date_est  ELSE null END AS open_uncontacted_timestamp,
        from opp_status_history)
        where open_uncontacted_timestamp is not null)
  where rid=1
),
table_2 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when stage_disposition = 'Active-Engaged' THEN created_date_est  ELSE null END AS active_engaged_timestamp,
        from opp_status_history)
        where active_engaged_timestamp is not null)
  where rid=1
),
table_3 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when stage_disposition = 'Active-Interest Verified' THEN created_date_est  ELSE null END AS active_interest_verified_timestamp,
        from opp_status_history)
        where active_interest_verified_timestamp is not null)
  where rid=1
),
table_4 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when stage_disposition = 'Qualified-Committed' THEN created_date_est  ELSE null END AS qualified_committed_timestamp,
        from opp_status_history)
        where qualified_committed_timestamp is not null)
  where rid=1
),
table_5 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when stage_disposition = 'Qualified-Documents Pending' THEN created_date_est  ELSE null END AS qualified_documents_pending_timestamp,
        from opp_status_history)
        where qualified_documents_pending_timestamp is not null)
  where rid=1
),
table_6 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when stage_disposition = 'Applicant-Complete - EA Ready for Review' THEN created_date_est  ELSE null END AS applicant_complete_ready_for_review_timestamp,
        from opp_status_history)
        where applicant_complete_ready_for_review_timestamp is not null)
  where rid=1
),
table_7 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when stage_disposition = 'Applicant-Admissions Review in Progress' THEN created_date_est  ELSE null END AS applicant_admissions_review_in_progress_timestamp,
        from opp_status_history)
        where applicant_admissions_review_in_progress_timestamp is not null)
  where rid=1
),
table_8 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when stage_disposition = 'Applicant-Admitted' THEN created_date_est  ELSE null END AS applicant_admitted_timestamp,
        from opp_status_history)
        where applicant_admitted_timestamp is not null)
  where rid=1
),
table_9 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when stage_disposition = 'Pre-enroll-Reserved' THEN created_date_est  ELSE null END AS preenroll_reserved_timestamp,
        from opp_status_history)
        where preenroll_reserved_timestamp is not null)
  where rid=1
),
table_10 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when stage_disposition in ('Pre-enroll-Registered','Pre-enroll-Logged-In','Pre-enroll-Participated') THEN created_date_est  ELSE null END AS preenroll_registered_timestamp,
        from opp_status_history)
        where preenroll_registered_timestamp is not null)
  where rid=1
),
table_11 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when stage_name = 'Applicant' THEN created_date_est  ELSE null END AS applicant_new_timestamp,
        from opp_status_history)
        where applicant_new_timestamp is not null)
  where rid=1
),
table_12 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when stage_name = 'Closed Lost' THEN created_date_est  ELSE null END AS closed_lost_timestamp,
        from opp_status_history)
        where closed_lost_timestamp is not null)
  where rid=1
),


table_13 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,CAST((case when stage_name = 'Pre-enroll' or stage_name ='Student' THEN created_date_est  ELSE null END) AS DATE) AS date_rs,
        from opp_status_history)
        where date_rs is not null)
  where rid=1
),
table_14 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,CAST((case when disposition = 'Admitted' and stage_name = 'Applicant' THEN created_date_est  ELSE null END) AS DATE) AS date_admitted,
        from opp_status_history)
        where date_admitted is not null)
  where rid=1
),
table_15 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,CAST((case when disposition = 'Admissions Review in Progress' and stage_name = 'Applicant' THEN created_date_est  ELSE null END) AS DATE) AS date_appcomplete,
        from opp_status_history)
        where date_appcomplete is not null)
  where rid=1
),
table_16 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,CAST((case when disposition in ('In Process', 'New', 'New - No Outreach', 'None','Uncontacted') and stage_name = 'Applicant' THEN created_date_est  ELSE null END) AS DATE) AS date_app,
        from opp_status_history)
        where date_app is not null)
  where rid=1
),
table_17 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,CAST((case when stage_name = 'Qualified' THEN created_date_est  ELSE null END) AS DATE) AS date_qualified,
        from opp_status_history)
        where date_qualified is not null)
  where rid=1
),
table_18 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,CAST((case when stage_name = 'Active' THEN created_date_est  ELSE null END) AS DATE) AS date_active,
        from opp_status_history)
        where date_active is not null)
  where rid=1
),
table_19 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,CAST((case when stage_name = 'Open' THEN created_date_est  ELSE null END) AS DATE) AS date_open,
        from opp_status_history)
        where date_open is not null)
  where rid=1
),
table_20 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,CAST((case when stage_name = 'Closed Lost' THEN created_date_est  ELSE null END) AS DATE) AS date_closed_lost,
        from opp_status_history)
        where date_closed_lost is not null)
  where rid=1
),
table_21 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,CAST((case when stage_name in('Paused','Pause') THEN created_date_est  ELSE null END) AS DATE) AS date_pause,
        from opp_status_history)
        where date_pause is not null)
  where rid=1
),
table_22 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,CAST((case when stage_name = 'Pre-Opportunity' THEN created_date_est  ELSE null END) AS DATE) AS date_pre_opportunity,
        from opp_status_history)
        where date_pre_opportunity is not null)
  where rid=1
),
table_23 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,case when disposition = 'In Process' THEN created_date_est  ELSE null END AS
   applicant_in_process_timestamp,
        from opp_status_history)
        where applicant_in_process_timestamp is not null)
  where rid=1
),
table_24 as (
 select * from (select *,
  row_number() over(partition by opp_sfid order by created_date_est ) as rid,
  from (select opp_sfid,created_date_est ,CAST((case when disposition = 'Complete - EA Ready for Review' THEN created_date_est  ELSE null END) AS DATE) AS date_appsubmitted,
        from opp_status_history)
        where date_appsubmitted is not null)
  where rid=1
)

select id as opp_sfid,
open_uncontacted_timestamp,
active_engaged_timestamp,
active_interest_verified_timestamp,
qualified_committed_timestamp,
qualified_documents_pending_timestamp,
applicant_complete_ready_for_review_timestamp,
applicant_admissions_review_in_progress_timestamp,
applicant_admitted_timestamp,
preenroll_reserved_timestamp,
preenroll_registered_timestamp,
applicant_new_timestamp,
closed_lost_timestamp,
date_rs,
date_admitted,
date_appcomplete,
date_appsubmitted,
date_app,
date_qualified,
date_active,
date_open,
date_closed_lost,
date_pause,
date_pre_opportunity,
applicant_in_process_timestamp

from `raw_b2c_sfdc.opportunity` l
left join table_1
on id = table_1.opp_sfid
left join table_2
on id = table_2.opp_sfid
left join table_3
on id = table_3.opp_sfid
left join table_4
on id = table_4.opp_sfid
left join table_5
on id = table_5.opp_sfid
left join table_6
on id = table_6.opp_sfid
left join table_7
on id = table_7.opp_sfid
left join table_8
on id = table_8.opp_sfid
left join table_9
on id = table_9.opp_sfid
left join table_10
on id = table_10.opp_sfid
left join table_11
on id = table_11.opp_sfid
left join table_12
on id = table_12.opp_sfid
left join table_13
on id = table_13.opp_sfid
left join table_14
on id = table_14.opp_sfid
left join table_15
on id = table_15.opp_sfid
left join table_16
on id = table_16.opp_sfid
left join table_17
on id = table_17.opp_sfid
left join table_18
on id = table_18.opp_sfid
left join table_19
on id = table_19.opp_sfid
left join table_20
on id = table_20.opp_sfid
left join table_21
on id = table_21.opp_sfid
left join table_22
on id = table_22.opp_sfid
left join table_23
on id = table_23.opp_sfid
left join table_24
on id = table_24.opp_sfid
where institution_c = 'a0ko0000002BSH4AAO'
and l.is_deleted=false
);

CREATE OR REPLACE temp TABLE `temp_opp_snapshot`
AS
with
next_start as (
select min(start_date_c) as next_start_date,program_c from `raw_b2c_sfdc.start_date_c`
where start_date_c >current_date()
group by program_c
),
opp_table as (
  select o.id as opp_sfid,
	o.created_date,
  utility.udf_convert_UTC_to_EST(o.created_date) as created_date_est,
	primary_flag_c as primary_flag,
  safe_cast(call_attempts_c as BIGNUMERIC) as contact_attempts,
  closed_lost_reason_1_c as closed_lost_reason_1,
  closed_lost_reason_2_c as closed_lost_reason_2,
  closed_lost_reason_3_c as closed_lost_reason_3,
  safe_cast(onyx_incident_key_c as BIGNUMERIC) as onyx_incident_key,
  stage_name,
  disposition_c as disposition,
  safe_cast(student_defer_requests_c as BIGNUMERIC) as student_defer_requests,
  o.start_date_c as discussed_start_date,
  intended_start_date_c as intended_start_date,
  recommended_admit_status_c as recommended_admit_status,
  first_ea_contact_c as first_ea_contact,
  o.first_ea_outreach_attempt_c as first_ea_outreach_attempt,
  last_ea_outreach_attempt_c as last_ea_outreach_attempt,
  last_ea_two_way_contact_c as last_ea_two_way_contact,
  admissions_decision_date_c as admissions_decision_date,
  individual_course_c as individual_course,
  safe_cast(reinquiry_count_c as BIGNUMERIC) as reinquiry_count,
  next_start.next_start_date,
  timezone_c as timezone,
  o.cid_c as cid,
  o.system_modstamp,
  confirmed_education_work_requirements_c as confirmed_education_work_requirements,
  o.referred_by_c as referred_by,
  application_id_c as application_id,
  o.raw_survey_question_11_c as raw_survey_question_11,
  defer_program_start_date_c as defer_program_start_date,
  ux_academic_history_complete_c  as ux_academic_history_complete_flag,
  ux_admissions_requirements_complete_c as ux_admissions_requirements_complete_flag,
  ux_background_info_complete_c as ux_background_info_complete_flag,
  ux_contact_info_complete_c as ux_contact_info_complete_flag,
  ux_employment_history_complete_c as ux_employment_history_complete_flag,
  ux_program_of_interest_complete_c as ux_program_of_interest_complete_flag,
  active_program_c as active_program_flag,
  update_next_start_date_flag_c as update_next_start_date_flag,
  communication_flag_c as communication_flag,
  domestic_c as domestic_c_flag,
  application_fee_paid_c as application_fee_paid_flag,
  application_started_c as application_started_flag,
  application_submitted_c application_submitted_flag,
  case when interested_in_federal_fin_aid_c = 'Yes' THEN true ELSE false END as  interested_in_federal_fin_aid_flag,
  case when (SUBSTRING( o.referred_by_c, 1, 3 )) = '003' THEN 'contact' ELSE null END AS advocate_object,
  --created_date as begin_stage_disposition_timestamp,
  --CAST(created_date AS DATE) AS inq_date,
  tempo_coach_c as tempo_coach,
  tempo_coach_s_email_c as tempo_coach_s_email,
  tempo_opp_id_c as tempo_owner_sfid,
  --EXTRACT(YEAR from created_date) as year_,
  --EXTRACT(MONTH from created_date) as month_,
  --case when EXTRACT(MONTH from created_date)>9 then concat(EXTRACT(YEAR from created_date), EXTRACT(MONTH from created_date)) else concat(EXTRACT(YEAR from created_date),'0', EXTRACT(MONTH from created_date)) end as year_month,
  case_final_decision_c as recommended_admit_decision,
  reconcile_c as new_student_flag,
  received_fafsa_application_c as received_fafsa_application,
  financial_aid_award_status_c as financial_aid_award_status,
  safe_cast(fafsa_award_year_c as BIGNUMERIC)  as fafsa_award_year,
  last_interesting_moment_date_c as last_interesting_moment_date,
  preferred_email_c as preferred_email,
  event_type_c as event_type,
  walden_sms_double_opt_in_c_c as walden_sms_double_opt_in,
  o.created_by_id as created_by_sfid,
  o.is_deleted,
  o.microsite_c,
  o.lead_source,
  u.name as Created_By_Name,
  u.Title as Created_By_Title,
  comments_c
	from `raw_b2c_sfdc.opportunity` o
  left join next_start on o.program_of_interest_c=next_start.program_c
  left join raw_b2c_sfdc.user u on o.created_by_id=u.id
  where o.is_deleted=false and o.institution_c='a0ko0000002BSH4AAO'
),
opp_est as (
  select opp_sfid,
  created_date_est as begin_stage_disposition_timestamp,
  CAST(created_date_est AS DATE) AS inq_date,
  EXTRACT(YEAR from created_date_est) as year_,
  EXTRACT(MONTH from created_date_est) as month_,
  case when EXTRACT(MONTH from created_date_est)>9 then concat(EXTRACT(YEAR from created_date_est), EXTRACT(MONTH from created_date_est)) else concat(EXTRACT(YEAR from created_date_est),'0', EXTRACT(MONTH from created_date_est)) end as year_month,
  from opp_table
),
international_flag_table as (
  select

    o.id as opp_sfid,
	/*
    case when  s.ISO_Code_c in ('AA', 'AE', 'AP', 'PR', 'VI', 'GU', 'AS', 'FR','MP', 'UM','PW','FM','MH','CM','CZ','RQ') Then True
	when cast(Ifnull(c.Name,b.Billing_Country_Text_c) AS string) Is Not Null and cast(Ifnull(c.Name,b.Billing_Country_Text_c) as string) Not In ('us', 'u.s.','United States','usa','u.s.a.') Then True
	when cast(Ifnull (c.Name,b.Billing_Country_Text_c) as string) Is Null AND b.Billing_State_text_c  is not null  And (s.Country_ISO_Code_c !='US'  OR  s.Country_ISO_Code_c is null) Then True
	when s.iso_code_c in ('AS','GU','MP','PR','UM','VI','PW','FM','MH','CM','CZ','RQ') and ( b.billing_country_text_c = 'United States' or b.billing_country_text_c is null ) Then True
	when b.billing_state_text_c like 'Armed Forces%' OR s.iso_code_c in ( 'AA', 'AE', 'AP' ) Then True
	when s.iso_code_c in ('AB','BC','MB','NB','NL','NS','NT','NU','ON','PE','QC','SK','YT','NF','PQ')
	and ( b.billing_country_text_c = 'United States' or b.billing_country_text_c is null )
  Then True
	ELSE False END As international_flag,
    */
	case when b.InternationalFlag =1 then true else false end As international_flag,
    s.iso_code_c as state_cd,

  from `raw_b2c_sfdc.opportunity` o
    left join `rpt_crm_mart.v_wldn_brandprofile` b
    on o.brand_profile_c = b.id and b.is_deleted=false
    left join `raw_b2c_sfdc.state_c` s
    on b.billing_state_c = s.id and s.is_deleted=false
    --left join `stg_l1_salesforce.country_c` c
    --on b.billing_country_c = c.id and c.is_deleted=false
	where o.is_deleted=false and o.institution_c='a0ko0000002BSH4AAO'
),
first_contacted_table as (
select opp_sfid, case
  when   coalesce(Open_Uncontacted_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) < coalesce( Active_Engaged_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then Open_Uncontacted_Timestamp
  when   coalesce(Open_Uncontacted_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<  coalesce( Qualified_Committed_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then Open_Uncontacted_Timestamp
  when   coalesce(Open_Uncontacted_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<  coalesce( Applicant_In_Process_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )then Open_Uncontacted_Timestamp
  when   coalesce(Open_Uncontacted_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<  coalesce( Applicant_Complete_Ready_for_Review_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then Open_Uncontacted_Timestamp
  when   coalesce(Open_Uncontacted_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<  coalesce( closed_lost_timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then Open_Uncontacted_Timestamp

  when  coalesce( Active_Engaged_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) < coalesce( Open_Uncontacted_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then Active_Engaged_Timestamp
  when   coalesce(Active_Engaged_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<  coalesce( Qualified_Committed_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then Active_Engaged_Timestamp
  when  coalesce( Active_Engaged_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<  coalesce( Applicant_In_Process_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then Active_Engaged_Timestamp
  when  coalesce( Active_Engaged_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<  coalesce( Applicant_Complete_Ready_for_Review_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then Active_Engaged_Timestamp
  when  coalesce( Active_Engaged_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<  coalesce( closed_lost_timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )then Active_Engaged_Timestamp

  when  coalesce( Qualified_Committed_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) < coalesce( Open_Uncontacted_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then   Qualified_Committed_Timestamp
  when  coalesce( Qualified_Committed_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<   coalesce(Active_Engaged_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )then  Qualified_Committed_Timestamp
  when   coalesce(Qualified_Committed_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) <  coalesce( Applicant_In_Process_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then  Qualified_Committed_Timestamp
  when  coalesce( Qualified_Committed_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) <  coalesce( Applicant_Complete_Ready_for_Review_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then Qualified_Committed_Timestamp
  when  coalesce( Qualified_Committed_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<  coalesce( closed_lost_timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then  Qualified_Committed_Timestamp

  when  coalesce( Applicant_In_Process_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) < coalesce( Open_Uncontacted_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then   Applicant_In_Process_Timestamp
  when   coalesce(Applicant_In_Process_Timestamp,cast('2100-01-01 00:00:00.000000' as datetime)  )<  coalesce( Active_Engaged_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )then  Applicant_In_Process_Timestamp
  when  coalesce( Applicant_In_Process_Timestamp,cast('2100-01-01 00:00:00.000000' as datetime) ) <  coalesce(  Qualified_Committed_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then  Applicant_In_Process_Timestamp
  when  coalesce( Applicant_In_Process_Timestamp,cast('2100-01-01 00:00:00.000000' as datetime)  ) < coalesce(  Applicant_Complete_Ready_for_Review_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then Applicant_In_Process_Timestamp
  when  coalesce( Applicant_In_Process_Timestamp,cast('2100-01-01 00:00:00.000000' as datetime)  ) < coalesce(  closed_lost_timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then  Applicant_In_Process_Timestamp

  when  coalesce( Applicant_Complete_Ready_for_Review_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) <  coalesce(Open_Uncontacted_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then   Applicant_Complete_Ready_for_Review_Timestamp
  when  coalesce( Applicant_Complete_Ready_for_Review_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<   coalesce(Active_Engaged_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then  Applicant_Complete_Ready_for_Review_Timestamp
  when  coalesce( Applicant_Complete_Ready_for_Review_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<  coalesce(  Qualified_Committed_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then  Applicant_Complete_Ready_for_Review_Timestamp
  when  coalesce( Applicant_Complete_Ready_for_Review_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) <  coalesce( Applicant_In_Process_Timestamp ,cast('2100-01-01 00:00:00.000000' as datetime) ) then Applicant_Complete_Ready_for_Review_Timestamp
  when  coalesce( Applicant_Complete_Ready_for_Review_Timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) )<  coalesce( closed_lost_timestamp  ,cast('2100-01-01 00:00:00.000000' as datetime) ) then Applicant_Complete_Ready_for_Review_Timestamp

  else closed_lost_timestamp end as first_contacted_date

FROM sorted_timestamp_table
),

src as (
    select distinct
    case when curr_owner.institution_id = 'a0ko0000002BSH4AAO' THEN 'WLDN' ELSE 'Unknown' END AS institution,
	case when curr_owner.institution_id = 'a0ko0000002BSH4AAO' THEN 5 ELSE -1 END AS institution_id,
	'WLDN_SF' as source_system_name,

	curr_owner.opp_sfid,
	curr_owner.es_curr_sfid,
	curr_owner.es_curr_name,
	curr_owner.es_curr_manager_sfid,
  curr_owner.es_curr_manager_name,
	curr_owner.es_curr_director_sfid,
  curr_owner.es_curr_director_name,
	curr_owner.es_curr_division,
	utility.udf_convert_UTC_to_EST(curr_owner.current_stage_disposition_timestamp) as current_stage_disposition_timestamp,
	curr_owner.es_curr_site,

  orig_owner.owner_sfid,
	orig_owner.es_orig_sfid,
	orig_owner.es_orig_name,
	orig_owner.es_orig_manager_sfid,
  orig_owner.es_orig_manager_name,
	orig_owner.es_orig_director_sfid,
  orig_owner.es_orig_director_name,
	orig_owner.es_orig_division,
	orig_owner.es_orig_site,

	curr_product.curr_product_sfid,
	curr_product.curr_product_name,
	curr_product.curr_program_name,
	curr_product.curr_customer_friendly_poi_name,
	curr_product.curr_product_nbr,
	--curr_product.curr_product_nbr_bnr,
	curr_product.curr_program_group,
	curr_product.curr_college_code,
	curr_product.curr_college_description,
	curr_product.curr_degree_code,
	curr_product.curr_concentration_code,
	curr_product.curr_concentration_description,
	curr_product.curr_level,
	curr_product.curr_ui_area_of_study,
	curr_product.sq_flag,
  curr_product.is_tempo_flag,

	orig_product.orig_product_sfid,
	orig_product.orig_product_name,
	orig_product.orig_program_name,
	orig_product.orig_customer_friendly_poi_name,
	orig_product.orig_product_nbr,
	--orig_product.orig_product_nbr_bnr,
	orig_product.orig_program_group,
	orig_product.orig_college_code,
	orig_product.orig_college_description,
	orig_product.orig_degree_code,
	orig_product.orig_concentration_code,
	orig_product.orig_concentration_description,
	orig_product.orig_level,
	orig_product.orig_ui_area_of_study,


	opp_table.created_date,
  opp_table.created_date_est,
	opp_table.primary_flag,
	opp_table.contact_attempts,
	opp_table.closed_lost_reason_1,
	opp_table.closed_lost_reason_2,
	opp_table.closed_lost_reason_3,
	opp_table.onyx_incident_key,
	opp_table.stage_name,
	opp_table.disposition,
	opp_table.student_defer_requests,
	opp_table.discussed_start_date,
	opp_table.intended_start_date,
	opp_table.recommended_admit_status,
	opp_table.first_ea_contact,
	opp_table.first_ea_outreach_attempt,
	opp_table.last_ea_outreach_attempt,
	opp_table.last_ea_two_way_contact,
	opp_table.admissions_decision_date,
	opp_table.individual_course,
	opp_table.reinquiry_count,
	opp_table.next_start_date,
	opp_table.timezone,
	opp_table.cid,
	opp_table.system_modstamp,
	opp_table.confirmed_education_work_requirements,
	opp_table.referred_by,
	opp_table.application_id,
	opp_table.raw_survey_question_11,
	opp_table.defer_program_start_date,
	opp_table.ux_academic_history_complete_flag,
	opp_table.ux_admissions_requirements_complete_flag,
	opp_table.ux_background_info_complete_flag,
	opp_table.ux_contact_info_complete_flag,
	opp_table.ux_employment_history_complete_flag,
	opp_table.ux_program_of_interest_complete_flag,
	opp_table.active_program_flag,
	opp_table.update_next_start_date_flag,
	opp_table.communication_flag,
	opp_table.domestic_c_flag,
	opp_table.application_fee_paid_flag,
	opp_table.application_started_flag,
	opp_table.application_submitted_flag,
	opp_table.interested_in_federal_fin_aid_flag,

	opp_table.advocate_object,
	opp_est.begin_stage_disposition_timestamp,
	opp_est.inq_date,
	opp_table.tempo_coach,
	opp_table.tempo_coach_s_email,
	opp_table.tempo_owner_sfid,
	opp_est.year_ as year,
	opp_est.month_ as month,
	opp_est.year_month,
	opp_table.recommended_admit_decision,
	opp_table.new_student_flag,
	opp_table.received_fafsa_application,
	opp_table.financial_aid_award_status,
	opp_table.fafsa_award_year,
	opp_table.last_interesting_moment_date,
	opp_table.preferred_email,
	opp_table.event_type,
	opp_table.walden_sms_double_opt_in,
	opp_table.created_by_sfid,
	opp_table.is_deleted,
	opp_table.microsite_c,
  opp_table.lead_source,
  opp_table.Created_By_Name,
  opp_table.Created_By_Title,
	case when opp_est.month_>=7 then opp_est.year_ +1 else opp_est.year_ end as FY_Year,
  opp_table.comments_c,

	sorted_flag_table.registered_flag,
	sorted_flag_table.logged_in_flag,
	sorted_flag_table.participated_flag,
	sorted_flag_table.reserved_flag,
	sorted_flag_table.active_1st_term_flag,
	sorted_flag_table.alumni_flag,
	sorted_flag_table.withdrawn_flag,
	sorted_flag_table.inactive_flag,
	sorted_flag_table.student_active_flag,

	sorted_timestamp_table.open_uncontacted_timestamp,
	sorted_timestamp_table.active_engaged_timestamp,
	sorted_timestamp_table.active_interest_verified_timestamp,
	sorted_timestamp_table.qualified_committed_timestamp,
	sorted_timestamp_table.qualified_documents_pending_timestamp,
	sorted_timestamp_table.applicant_complete_ready_for_review_timestamp,
	sorted_timestamp_table.applicant_admissions_review_in_progress_timestamp,
	sorted_timestamp_table.applicant_admitted_timestamp,
	sorted_timestamp_table.preenroll_reserved_timestamp,
	sorted_timestamp_table.preenroll_registered_timestamp,
	sorted_timestamp_table.applicant_new_timestamp,
	sorted_timestamp_table.closed_lost_timestamp,
	sorted_timestamp_table.date_rs,
	sorted_timestamp_table.date_admitted,
	sorted_timestamp_table.date_appcomplete,
	sorted_timestamp_table.date_appsubmitted,
	sorted_timestamp_table.date_app,
	sorted_timestamp_table.date_qualified,
	sorted_timestamp_table.date_active,
	sorted_timestamp_table.date_open,
	sorted_timestamp_table.date_closed_lost,
	sorted_timestamp_table.date_pause,
	sorted_timestamp_table.date_pre_opportunity,
	sorted_timestamp_table.applicant_in_process_timestamp,

  brand_table.banner_id,
	brand_table.billing_country,
	brand_table.billing_state,
	brand_table.billing_city,
	brand_table.billing_country_text,
	brand_table.billing_state_text,
	brand_table.city,
	brand_table.country_name,
	brand_table.state,
	brand_table.mailing_country,
	brand_table.activity_id,
	brand_table.channel,
	brand_table.selected_program_start_date_id,
	brand_table.selected_program_start_date,
	--brand_table.start_date,
	brand_table.contact_sfid,
	brand_table.contact_name,
	brand_table.bd_account_sfid,
	brand_table.brand_profile_sfid,
	brand_table.partner_flag,
	brand_table.military_flag,
	brand_table.campaign_name,

	retention_table.applicant_id,
	retention_table.term_cd,
	retention_table.second_term,
	retention_table.third_term,
	retention_table.second_term_retention,
	retention_table.start_date_term2,
	retention_table.third_term_retention,
	retention_table.start_date_term3,
	retention_table.student_start_date as enr_date,

	international_flag_table.international_flag,
	international_flag_table.state_cd,

	first_contacted_table.first_contacted_date,
  #added this column on Nicoles request 2023-10-13
  case when  Alumni.id is not null then 1 else 0 end as alumni_inquiry_flag


	from curr_owner
	left join orig_owner
	on curr_owner.opp_sfid = orig_owner.opp_sfid
	left join curr_product
	on curr_owner.opp_sfid = curr_product.opp_sfid
	left join orig_product
	on curr_owner.opp_sfid = orig_product.opp_sfid
	left join opp_table
	on curr_owner.opp_sfid = opp_table.opp_sfid
	left join sorted_flag_table
	on curr_owner.opp_sfid = sorted_flag_table.opp_sfid
	left join sorted_timestamp_table
	on curr_owner.opp_sfid = sorted_timestamp_table.opp_sfid
	left join brand_table
	on curr_owner.opp_sfid = brand_table.opp_sfid
	left join `trans_crm_mart.prc_wldn_student_retention_details` as retention_table
	on curr_owner.opp_sfid = retention_table.opp_sfid
  left join international_flag_table
  on curr_owner.opp_sfid = international_flag_table.opp_sfid
  left join first_contacted_table
  on curr_owner.opp_sfid = first_contacted_table.opp_sfid
  left join opp_est
  on curr_owner.opp_sfid = opp_est.opp_sfid
  LEFT JOIN (select distinct id,
    min(outcome_graduation_date1) as First_Graduation_Date
    from `rpt_academics.t_wldn_alumni`
    where graduation_flag1=1 and enrollment_flag1=1 and cert_flag1=0
    group by id ) Alumni ON (brand_table.banner_id = Alumni.id AND (opp_est.inq_date) >= Alumni.First_Graduation_Date)
	where curr_owner.institution_id = 'a0ko0000002BSH4AAO'
	and curr_owner.es_curr_sfid <> '005o0000002R53aAAC'
)

SELECT
    src.*,
    job_start_dt as etl_created_date,
    job_start_dt as etl_updated_date,
    load_source as etl_resource_name,
    v_audit_key as etl_ins_audit_key,
    v_audit_key as etl_upd_audit_key,
    farm_fingerprint(format('%T', concat(src.opp_sfid))) AS etl_pk_hash,
    farm_fingerprint(format('%T', src )) as etl_chg_hash,
    FROM src;

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
