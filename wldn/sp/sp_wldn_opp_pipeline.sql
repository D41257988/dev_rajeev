CREATE OR REPLACE PROCEDURE `daas-cdw-dev.trans_crm_mart.sp_wldn_opp_pipeline`(IN v_audit_key STRING, OUT result STRING)
begin

    declare institution string default 'WLDN';
    declare institution_id int64 default 5;
    declare dml_mode string default 'delete-insert';
    declare target_dataset string default 'rpt_crm_mart';
    declare target_tablename string default 't_wldn_opp_pipeline';
    declare source_tablename string default 'temp_walden_opp_pipeline';
    declare load_source string default 'trans_crm_mart.sp_wldn_opp_pipeline';
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

	DECLARE adj int64 default 1;
	DECLARE running_date DATE default CAST(utility.udf_convert_UTC_to_EST(CURRENT_TIMESTAMP()) AS DATE);
	DECLARE num_yr int64 default 4;
	DECLARE cutoff_date DATE default DATE_ADD(DATE'1900-01-01', INTERVAL DATE_DIFF(running_date,DATE'1900-01-01', DAY) DAY);
	DECLARE num_start_month INT64 default 12;
	DECLARE diff0 int64 default 0;
	DECLARE diff1 int64 default 0;
	DECLARE base_month_curr DATE default DATE'1900-01-01';
	DECLARE diff_curr int64 default 0;
	DECLARE start_date_group_curr STRING default '';
	DECLARE base_month_prev DATE default DATE'1900-01-01';
	DECLARE diff_prev int64 default 0;
	DECLARE start_date_group_prev STRING default '';

	BEGIN
    SET additional_attributes= [("audit_load_key", v_audit_key),
              ("load_method",load_method),
              ("load_source",load_source),
              ("job_type", job_type)];
    /* end common across */


CREATE
OR REPLACE temp TABLE opp_param AS (
with
temp_param as (
  select
  DATE_ADD(DATE'1900-01-01', INTERVAL DATE_DIFF(cutoff_date-1,DATE'1900-01-01', MONTH)+1-12*num_yr MONTH) as begin_date,
  cutoff_date as end_date,
  cutoff_date as cutoff_date,
  '(a) Current' as period,
  null as adj,
  DATE_ADD(DATE'1900-01-01', INTERVAL DATE_DIFF(cutoff_date-1,DATE'1900-01-01', MONTH) MONTH) as base_month

  union all

  select
  DATE_ADD(DATE'1900-01-01', INTERVAL DATE_DIFF(cutoff_date-1,DATE'1900-01-01', MONTH)+1-12*(num_yr+1) MONTH) as begin_date,
  DATE_SUB(cutoff_date, INTERVAL 364 DAY) as end_date,
  DATE_SUB(cutoff_date, INTERVAL 364 DAY) as cutoff_date,
  '(b) Previous' as period,
  adj as adj,
  DATE_ADD(DATE'1900-01-01', INTERVAL DATE_DIFF(cutoff_date-1,DATE'1900-01-01', MONTH)-12 MONTH) as base_month

)
select
CAST(begin_date as STRING) as begin_dtkey,
CAST(end_date as STRING) as end_dtkey,
CAST(cutoff_date as STRING) as cutoff_dtkey,
*,
EXTRACT(DAYOFWEEK FROM cutoff_date) AS week_day,
running_date as running_date
from temp_param
);
CREATE
OR REPLACE temp TABLE opp_start_month_range AS
(
  with
tmp_start_month_range as (
  select
  base_month,
  DATE_ADD(DATE'1900-01-01', INTERVAL DATE_DIFF(base_month, DATE'1900-01-01', QUARTER)-1 QUARTER) as month0,
  from opp_param
  where period = '(a) Current'

)
select *,
  DATE_ADD(month0, INTERVAL num_start_month-1 MONTH) as month1,
  DATE_DIFF(month0, base_month, MONTH) as diff0,
  DATE_DIFF(month0, base_month, MONTH)+(num_start_month-1) as diff1,
from tmp_start_month_range
);

SET diff0 = (select diff0 from opp_start_month_range);
SET diff1 = (select diff1 from opp_start_month_range);

CREATE
OR REPLACE temp TABLE leads_curr AS (
with leads_cur_temp as (
  select
  institution,
  institution_id,
  'WLDN_SF' as source_system_name,
  opp_sfid,
  created_date,
  DATE_ADD(DATE'1900-01-01', INTERVAL DATE_DIFF(inq_date,DATE'1900-01-01', MONTH) MONTH) as inq_month_day1,
  DATE_DIFF(base_month,inq_date, MONTH) as month_index,
  'NA' as groupnewid,
  brand_profile_sfid,
  contact_sfid,
  banner_id,
  owner_sfid,
  international_flag,
  billing_country,
  country_name,
  state,
  billing_city as city,
  activity_id,
  channel,
  curr_product_sfid,
  curr_product_nbr,
  is_tempo_flag,
  military_flag,
  inq_date,
  enr_date,
  date_rs,
  date_admitted,
  date_appcomplete,
  date_app,
  date_qualified,
  date_active,
  date_open,
  date_closed_lost,
  date_pause,
  date_pre_opportunity,
  es_curr_sfid,
  es_curr_name,
  es_curr_manager_name,
  es_curr_director_name,
  es_curr_division,
  es_curr_site,
  es_orig_sfid,
  es_orig_name,
  es_orig_manager_name,
  es_orig_director_name,
  es_orig_division,
  es_orig_site,
  partner_flag,
  base_month,
  cutoff_date,
  created_by_sfid
from `rpt_crm_mart.t_wldn_opp_snapshot` t1
inner join (select * from opp_param
             where period = '(a) Current') t2
on t2.begin_date <= t1.inq_date and t1.inq_date < t2.end_date
)

select t1.*
 ,t3.Region_name
 ,month_index/12 + 1 as cohort_age
 ,case when month_index between 0 and 11 then month_index + 1 else null end as cohort_month_id
 ,case when month_index between 0 and 11 then FORMAT_DATE("%B", inq_month_day1) else null end as cohort_month
 ,DATE_DIFF(inq_month_day1, base_month,year) as calendar_year_id
from leads_cur_temp t1
left join `raw_b2c_sfdc.country_c` t2 on t1.billing_country = t2.id and t2.is_deleted=false
left join `trans_crm_mart.v_country_region_map` t3 on t2.iso_code_c = t3.Country_Cd
);

CREATE
OR REPLACE temp TABLE leads_prev as (
with
leads_prev_temp as (
  select
  institution,
  institution_id,
  'WLDN_SF' as source_system_name,
  opp_sfid,
  created_date,
  DATE_ADD(DATE'1900-01-01', INTERVAL DATE_DIFF(inq_date,DATE'1900-01-01', MONTH) MONTH) as inq_month_day1,
  DATE_DIFF(base_month,inq_date, MONTH) as month_index,
  'NA' as groupnewid,
  brand_profile_sfid,
  contact_sfid,
  banner_id,
  owner_sfid,
  international_flag,
  billing_country,
  country_name,
  state,
  billing_city as city,
  activity_id,
  channel,
  curr_product_sfid,
  curr_product_nbr,
  is_tempo_flag,
  military_flag,
  inq_date,
  enr_date,
  date_rs,
  date_admitted,
  date_appcomplete,
  date_app,
  date_qualified,
  date_active,
  date_open,
  date_closed_lost,
  date_pause,
  date_pre_opportunity,
  es_curr_sfid,
  es_curr_name,
  es_curr_manager_name,
  es_curr_director_name,
  es_curr_division,
  es_curr_site,
  es_orig_sfid,
  es_orig_name,
  es_orig_manager_name,
  es_orig_director_name,
  es_orig_division,
  es_orig_site,
  partner_flag,
  base_month,
  cutoff_date,
  created_by_sfid
from `rpt_crm_mart.t_wldn_opp_snapshot` t1
inner join (select * from opp_param
             where period = '(b) Previous') t2
on t2.begin_date <= t1.inq_date and t1.inq_date < t2.end_date
),
leads_prev_mod as (
  select
  institution,
  institution_id,
  source_system_name,
  opp_sfid,
  created_date,
  case when month_index=-1 then DATE_ADD(inq_month_day1, INTERVAL -1 MONTH) else inq_month_day1 end as inq_month_day1,
  case when month_index=-1 then 0 else month_index end as month_index,
  groupnewid,
  brand_profile_sfid,
  contact_sfid,
  banner_id,
  owner_sfid,
  international_flag,
  billing_country,
  country_name,
  state,
  city,
  activity_id,
  channel,
  curr_product_sfid,
  curr_product_nbr,
  is_tempo_flag,
  military_flag,
  inq_date,
  enr_date,
  date_rs,
  date_admitted,
  date_appcomplete,
  date_app,
  date_qualified,
  date_active,
  date_open,
  date_closed_lost,
  date_pause,
  date_pre_opportunity,
  es_curr_sfid,
  es_curr_name,
  es_curr_manager_name,
  es_curr_director_name,
  es_curr_division,
  es_curr_site,
  es_orig_sfid,
  es_orig_name,
  es_orig_manager_name,
  es_orig_director_name,
  es_orig_division,
  es_orig_site,
  partner_flag,
  base_month,
  cutoff_date,
  created_by_sfid
  from leads_prev_temp
)

select t1.*
 ,t3.Region_name
 ,month_index/12 + 1 as cohort_age
 ,case when month_index between 0 and 11 then month_index + 1 else null end as cohort_month_id
 ,case when month_index between 0 and 11 then FORMAT_DATE("%B", inq_month_day1) else null end as cohort_month
 ,DATE_DIFF(inq_month_day1, base_month,year) as calendar_year_id
from leads_prev_mod t1
left join `raw_b2c_sfdc.country_c` t2 on t1.billing_country = t2.id and t2.is_deleted=false
left join `trans_crm_mart.v_country_region_map` t3 on t2.iso_code_c = t3.Country_Cd
);

CREATE
OR REPLACE temp TABLE leads_curr_update1 AS (
with
hist_product_curr as (
  select opp_sfid ,created_date ,program_of_interest_id,program_of_interest_name
  from (
  select t1.* ,ROW_NUMBER() OVER(partition by opp_sfid order by created_date desc ,opp_hist_sfid desc) as rid
  from `rpt_crm_mart.t_opp_program_history` t1
  inner join ( select * from opp_param where period = '(a) Current') t2 on cast(t1.created_date_est as DATE) < t2.cutoff_date
  ) t
  where rid = 1
),
hist_start_curr as (
  select t.opp_sfid ,t.created_date ,program_of_interest_id,t.program_of_interest_name, cast(t.start_date as DATE) as start_date,
  from (
  select t1.* ,ROW_NUMBER() OVER(partition by opp_sfid order by created_date desc ,opp_hist_sfid desc) as rid
  from `rpt_crm_mart.t_opp_start_history` t1
  inner join ( select * from opp_param where period = '(a) Current') t2 on cast(t1.created_date_est as DATE) < t2.cutoff_date
  ) t
  where rid = 1
),
hist_product_curr_update as (
select l.opp_sfid,
  case when r.program_of_interest_name is not null then r.program_of_interest_name else l.program_of_interest_name end as program_of_interest_name,
  case when r.created_date is not null then r.created_date else l.created_date end as created_date,
  start_date
  from hist_product_curr l
  left join hist_start_curr r on l.opp_sfid=r.opp_sfid
),

tmp_leads_curr as (
  select t1.*
  ,t2.start_date as intended_start_date
  ,case when DATE_DIFF(DATE_ADD(DATE'1900-01-01',INTERVAL 1+DATE_DIFF(t2.start_date,DATE'1900-01-01',month ) month),t2.start_date,day)>=7 or EXTRACT(MONTH FROM t2.start_date) in (3,6,9,12) then DATE_ADD(DATE'1900-01-01',INTERVAL DATE_DIFF(t2.start_date,DATE'1900-01-01',month ) month)
    when t2.start_date is not null then DATE_ADD(DATE'1900-01-01',INTERVAL 1+DATE_DIFF(t2.start_date,DATE'1900-01-01',month ) month)
    else null end as intended_start_date2
  from leads_curr t1
  left join hist_start_curr t2 on t1.opp_sfid = t2.opp_sfid
)
select t1.*,
t2.program_name,
t2.concentration_description as specialization,
--t2.program_name_grid ,
'NA' as program_name_grid,
t2.level_description as level ,
t2.college_code,
t2.sq_flag
from tmp_leads_curr t1
left join `rpt_academics.t_wldn_product_map` t2 on t1.curr_product_sfid = t2.product_sfid
);

CREATE
OR REPLACE temp TABLE leads_prev_update1 AS (
with
hist_product_prev as (
  select opp_sfid ,created_date ,program_of_interest_id,program_of_interest_name
  from (
  select t1.* ,ROW_NUMBER() OVER(partition by opp_sfid order by created_date desc ,opp_hist_sfid desc) as rid
  from `rpt_crm_mart.t_opp_program_history` t1
  inner join ( select * from opp_param where period = '(b) Previous') t2 on cast(t1.created_date_est as DATE) < t2.Cutoff_date
  ) t
  where rid = 1
),
hist_start_prev as (
  select t.opp_sfid ,t.created_date ,program_of_interest_id,t.program_of_interest_name, cast(t.start_date as DATE) as start_date,
  from (
  select t1.* ,ROW_NUMBER() OVER(partition by opp_sfid order by created_date desc ,opp_hist_sfid desc) as rid
  from `rpt_crm_mart.t_opp_start_history` t1
  inner join ( select * from opp_param where period = '(b) Previous') t2 on cast(t1.created_date_est as DATE) < t2.Cutoff_date
  ) t
  where rid = 1
),
hist_product_prev_update as (
select l.opp_sfid,
  case when r.program_of_interest_name is not null then r.program_of_interest_name else l.program_of_interest_name end as program_of_interest_name,
  case when r.created_date is not null then r.created_date else l.created_date end as created_date,
  start_date
  from hist_product_prev l
  left join hist_start_prev r on l.opp_sfid=r.opp_sfid
),
leads_prev_update as (
select
l.institution,
l.institution_id,
l.source_system_name,
l.opp_sfid,
l.created_date,
l.inq_month_day1,
l.month_index,
l.groupnewid,
l.brand_profile_sfid,
l.contact_sfid,
l.banner_id,
l.owner_sfid,
l.international_flag,
l.billing_country,
l.country_name,
l.state,
l.city,
l.activity_id,
l.channel,
case when r.program_of_interest_id is not null then r.program_of_interest_id else l.curr_product_sfid end as curr_product_sfid,
case when r.program_of_interest_name is not null then r.program_of_interest_name else l.curr_product_nbr end as curr_product_nbr,
l.is_tempo_flag,
l.military_flag,
l.inq_date,
l.enr_date,
l.date_rs,
l.date_admitted,
l.date_appcomplete,
l.date_app,
l.date_qualified,
l.date_active,
l.date_open,
l.date_closed_lost,
l.date_pause,
l.date_pre_opportunity,
l.es_curr_sfid,
l.es_curr_name,
l.es_curr_manager_name,
l.es_curr_director_name,
l.es_curr_division,
l.es_curr_site,
l.es_orig_sfid,
l.es_orig_name,
l.es_orig_manager_name,
l.es_orig_director_name,
l.es_orig_division,
l.es_orig_site,
l.partner_flag,
l.base_month,
l.cutoff_date,
l.created_by_sfid,
l.Region_name,
l.cohort_age,
l.cohort_month_id ,
l.cohort_month ,
l.calendar_year_id,
from leads_prev l
left join hist_product_prev r on l.opp_sfid=r.opp_sfid
),
tmp_leads_prev as (
  select t1.*
  ,t2.start_date as intended_start_date
  ,case when DATE_DIFF(DATE_ADD(DATE'1900-01-01',INTERVAL 1+DATE_DIFF(t2.start_date,DATE'1900-01-01',month ) month),t2.start_date,day)>=7 or EXTRACT(MONTH FROM t2.start_date) in (3,6,9,12) then DATE_ADD(DATE'1900-01-01',INTERVAL DATE_DIFF(t2.start_date,DATE'1900-01-01',month ) month)
    when t2.start_date is not null then DATE_ADD(DATE'1900-01-01',INTERVAL 1+DATE_DIFF(t2.start_date,DATE'1900-01-01',month ) month)
    else null end as intended_start_date2
  from leads_prev_update t1
  left join hist_start_prev t2 on t1.opp_sfid = t2.opp_sfid
)
select t1.*,
t2.program_name,
t2.concentration_description as specialization,
--t2.program_name_grid,
'NA' as program_name_grid,
t2.level_description as level,
t2.college_code,
t2.sq_flag
from tmp_leads_prev t1
left join `rpt_academics.t_wldn_product_map` t2 on t1.curr_product_sfid = t2.product_sfid
);
CREATE
OR REPLACE temp TABLE leads_curr_update2 AS (
with
hist_status_curr as (
select opp_sfid ,cast(created_date as date) as created_date ,stage_name ,disposition,created_date_est
from (
select t1.* ,ROW_NUMBER() OVER(partition by opp_sfid order by created_date desc ,opp_hist_sfid desc) as rid
from `rpt_crm_mart.t_opp_status_history` t1
inner join (select * from opp_param where period = '(a) Current' ) t2 on cast(t1.created_date_est as date) < t2.cutoff_date
) t
where rid = 1
),

app_RS_WD_history as (
select l.opp_sfid ,concat(r.credential_id,'-',r.application_number,'-',r.academic_period) as application_id ,r.latest_decision_date as decision_effective_timestamp ,r.latest_decision as status_cd ,r.application_status_desc as status_desc
,case when r.latest_decision in ('RC','RI','RS') then 'Pre-enroll'
when r.latest_decision = 'WD' then 'Closed Lost'
else null end as stage_name
from hist_status_curr l
left join (
  select a.*,m.oppsfid
  from `rpt_academics.v_admissions_application` a
  inner join `rpt_academics.v_wldn_map_sales_opp_app` m on concat(a.credential_id,'-',a.application_number,'-',a.academic_period)=m.application_id
  )r on l.opp_sfid = r.oppsfid
where r.latest_decision_date >= '2016-01-01'
and r.latest_decision in ( 'RC','RI','RS','WD' )
),
app_status_curr as (
select opp_sfid ,decision_effective_timestamp ,stage_name
from (
select t1.* ,ROW_NUMBER() OVER(partition by opp_sfid order by decision_effective_timestamp desc) as rid
from app_RS_WD_history t1
inner join ( select * from opp_param where period = '(a) Current' ) t2 on t1.decision_effective_timestamp < t2.cutoff_date
) t
where rid = 1
),
hist_status_curr_update1 as (
  select l.opp_sfid,
  case when r.decision_effective_timestamp > l.created_date_est then r.decision_effective_timestamp else l.created_date_est end as created_date,
  case when r.decision_effective_timestamp > cast(l.created_date_est as date) then r.stage_name else l.stage_name end as stage_name,
  case when r.decision_effective_timestamp > cast(l.created_date_est as date) then 'none' else l.disposition end as disposition,
  from hist_status_curr l
  left join app_status_curr r on l.opp_sfid = r.opp_sfid
),
tmp_leads_cur as (
select t1.* ,t2.stage_name as last_stage ,t2.disposition as last_disposition
from leads_curr_update1 t1
left join hist_status_curr_update1 t2 on t1.opp_sfid = t2.opp_sfid
)

select *
,case when DATE_DIFF(intended_start_date2, base_month, month) between diff0 and diff1 then intended_start_date
else null end as matched_start_date

,case when DATE_DIFF( intended_start_date2, base_month,month) < diff0 then CONCAT('PrevStart: ',CAST(-(diff0) AS  STRING), ' months ago')
when DATE_DIFF( intended_start_date2, base_month,month) between diff0 and -1 then CONCAT('PrevStart_',CAST(DATE_DIFF( base_month, intended_start_date2,month) AS  STRING),FORMAT_DATE("%B", intended_start_date2))
when DATE_DIFF( intended_start_date2, base_month,month) between 0 and diff1 then CONCAT('FutureStart_',CAST(DATE_DIFF( intended_start_date2, base_month,month) AS  STRING),FORMAT_DATE("%B", intended_start_date2))
when DATE_DIFF( intended_start_date2, base_month,month) > diff1 then CONCAT('FutureStart: ',CAST(diff1 AS  STRING), ' months later')
else 'Start Date Missing' end as start_date_group

,case when es_curr_name is null then es_orig_name else es_curr_name end as ea_name
,case when es_curr_name is null then es_orig_manager_name else es_curr_manager_name end as ea_manager
,case when es_curr_name is null then es_orig_site else es_curr_site end as ea_location
,case when es_curr_name is null then es_orig_director_name else es_curr_director_name end as ea_director
,case when es_curr_name is null then es_orig_division else es_curr_division end as ea_division
from tmp_leads_cur
);

CREATE
OR REPLACE temp TABLE leads_prev_update2 AS (
with
hist_status_prev as (
select opp_sfid ,cast(created_date as date) as created_date ,stage_name ,disposition,created_date_est
from (
select t1.* ,ROW_NUMBER() OVER(partition by opp_sfid order by created_date desc ,opp_hist_sfid desc) as rid
from `rpt_crm_mart.t_opp_status_history` t1
inner join (select * from opp_param where period = '(b) Previous') t2 on cast(t1.created_date_est as date) < t2.cutoff_date
) t
where rid = 1
),
app_RS_WD_history as (
select l.opp_sfid ,concat(r.credential_id,'-',r.application_number,'-',r.academic_period) as application_id ,r.latest_decision_date as decision_effective_timestamp ,r.latest_decision as status_cd ,r.application_status_desc as status_desc
,case when r.latest_decision in ('RC','RI','RS') then 'Pre-enroll'
when r.latest_decision = 'WD' then 'Closed Lost'
else null end as stage_name
from hist_status_prev l
left join (
  select a.*,m.oppsfid
  from `rpt_academics.v_admissions_application` a
  inner join `rpt_academics.v_wldn_map_sales_opp_app` m on concat(a.credential_id,'-',a.application_number,'-',a.academic_period)=m.application_id
  )r on l.opp_sfid = r.oppsfid
where r.latest_decision_date >= '2016-01-01'
and r.latest_decision in ( 'RC','RI','RS','WD' )
),
app_status_prev as (
select opp_sfid ,decision_effective_timestamp ,stage_name
from (
select t1.* ,ROW_NUMBER() OVER(partition by opp_sfid order by decision_effective_timestamp desc) as rid
from app_RS_WD_history t1
inner join ( select * from opp_param where period = '(b) Previous' ) t2 on t1.decision_effective_timestamp < t2.cutoff_date
) t
where rid = 1
),
hist_status_prev_update1 as (
  select l.opp_sfid,
  case when r.decision_effective_timestamp > l.created_date_est then r.decision_effective_timestamp else l.created_date_est end as created_date,
  case when r.decision_effective_timestamp > cast(l.created_date_est as date) then r.stage_name else l.stage_name end as stage_name,
  case when r.decision_effective_timestamp > cast(l.created_date_est as date) then 'none' else l.disposition end as disposition,
  from hist_status_prev l
  left join app_status_prev r on l.opp_sfid = r.opp_sfid
),
tmp_leads_prev as (
select t1.* ,t2.stage_name as last_stage ,t2.disposition as last_disposition
from leads_prev_update1 t1
left join hist_status_prev_update1 t2 on t1.opp_sfid = t2.opp_sfid
),
tmp_leads_prev_update_1 as(
select *
,case when DATE_DIFF(intended_start_date2, base_month, month) between diff0 and diff1 then intended_start_date
else null end as matched_start_date

,case when DATE_DIFF( intended_start_date2, base_month,month) < diff0 then CONCAT('PrevStart: ',CAST(-(diff0) AS  STRING), ' months ago')
when DATE_DIFF( intended_start_date2, base_month,month) between diff0 and -1 then CONCAT('PrevStart_',CAST(DATE_DIFF( base_month, intended_start_date2,month) AS  STRING),FORMAT_DATE("%B", intended_start_date2))
when DATE_DIFF( intended_start_date2, base_month,month) between 0 and diff1 then CONCAT('FutureStart_',CAST(DATE_DIFF( intended_start_date2, base_month,month) AS  STRING),FORMAT_DATE("%B", intended_start_date2))
when DATE_DIFF( intended_start_date2, base_month,month) > diff1 then CONCAT('FutureStart: ',CAST(diff1 AS  STRING), ' months later')
else 'Start Date Missing' end as start_date_group

,case when es_curr_name is null then es_orig_name else es_curr_name end as ea_name
,case when es_curr_name is null then es_orig_manager_name else es_curr_manager_name end as ea_manager
,case when es_curr_name is null then es_orig_site else es_curr_site end as ea_location
,case when es_curr_name is null then es_orig_director_name else es_curr_director_name end as ea_director
,case when es_curr_name is null then es_orig_division else es_curr_division end as ea_division
from tmp_leads_prev
),
start_opp as (
select opp_sfid ,created_date ,cutoff_date
	,intended_start_date ,intended_start_date2 ,matched_Start_date
	,last_stage ,last_disposition
from tmp_leads_prev_update_1
where intended_start_date = '2019-05-28'
),
hist_start_prev_new as (
select opp_sfid ,created_date ,program_of_interest_name ,start_date
from (
select t1.* ,ROW_NUMBER() OVER(partition by t1.opp_sfid order by t1.created_date desc ,t1.opp_hist_sfid desc) as rid
from `rpt_crm_mart.t_opp_start_history` t1
inner join start_opp t2 on t1.opp_sfid = t2.opp_sfid and cast(t1.created_date_est as date) < DATE_ADD(t2.cutoff_date,INTERVAL -7 day)
) t
where rid = 1
),
hist_status_prev_new as (
select opp_sfid ,created_date ,Stage_name ,disposition
from (
select t1.* ,ROW_NUMBER() OVER(partition by t1.opp_sfid order by t1.created_date desc ,t1.opp_hist_sfid desc) as rid
from `rpt_crm_mart.t_opp_status_history` t1
inner join start_opp t2 on t1.opp_sfid = t2.opp_sfid and cast(t1.created_date_est as date) < DATE_ADD(t2.cutoff_date,INTERVAL -7 day)
) t
where rid = 1
),
Opp_null as (
select opp_sfid
from start_opp
EXCEPT DISTINCT
select opp_sfid
from hist_start_prev_new
where Start_date = '2019-05-28'
),
opp_status as (
select t1.opp_sfid ,last_stage, last_disposition ,t3.stage_name ,t3.Disposition
from start_opp t1
inner join (
	select opp_sfid
	from hist_start_prev_new
	where Start_date = '2019-05-28'
) t2 on t1.opp_sfid = t2.opp_sfid
left join hist_status_prev_new t3 on t1.opp_sfid = t3.opp_sfid
where ifnull(t1.last_stage, 'null') <> ifnull(t3.stage_name, 'null')
	or ifnull(t1.last_disposition, 'null') <> ifnull(t3.disposition, 'null')
)

select
t1.institution,
t1.institution_id,
t1.source_system_name,
t1.opp_sfid,
t1.created_date,
t1.inq_month_day1,
t1.month_index,
t1.groupnewid,
t1.brand_profile_sfid,
t1.contact_sfid,
t1.banner_id,
t1.owner_sfid,
t1.international_flag,
t1.billing_country,
t1.country_name,
t1.state,
t1.city,
t1.activity_id,
t1.channel,
t1.curr_product_sfid,
t1.curr_product_nbr,
t1.is_tempo_flag,
t1.military_flag,
t1.inq_date,
t1.enr_date,
t1.date_rs,
t1.date_admitted,
t1.date_appcomplete,
t1.date_app,
t1.date_qualified,
t1.date_active,
t1.date_open,
t1.date_closed_lost,
t1.date_pause  ,
t1.date_pre_opportunity,
t1.es_curr_sfid  ,
t1.es_curr_name ,
t1.es_curr_manager_name,
t1.es_curr_director_name,
t1.es_curr_division,
t1.es_curr_site  ,
t1.es_orig_sfid  ,
t1.es_orig_name ,
t1.es_orig_manager_name,
t1.es_orig_director_name,
t1.es_orig_division,
t1.es_orig_site ,
t1.partner_flag,
t1.base_month ,
case when t4.opp_sfid is not null then DATE_ADD(t1.cutoff_date,INTERVAL -7 day) else t1.cutoff_date end as cutoff_date,
created_by_sfid,
t1.Region_name ,
t1.cohort_age,
t1.cohort_month_id,
t1.cohort_month,
t1.calendar_year_id,
case when t2.opp_sfid is not null then null else intended_start_date end as intended_start_date,
case when t2.opp_sfid is not null then null else intended_start_date2 end as intended_start_date2,
t1.program_name,
t1.specialization,
t1.program_name_grid,
t1.level,
t1.college_code,
t1.sq_flag,
case when t3.opp_sfid is not null then t3.stage_name else t1.last_stage end as last_stage,
case when t3.opp_sfid is not null then t3.Disposition else t1.last_disposition end as last_disposition,
case when t2.opp_sfid is not null then null else matched_Start_date end as matched_Start_date,
case when t2.opp_sfid is not null then null else start_date_group end as start_date_group,
t1.ea_name,
t1.ea_manager,
t1.ea_location,
t1.ea_director,
t1.ea_division,
from tmp_leads_prev_update_1 t1
left join opp_null t2
on t1.opp_sfid = t2.opp_sfid
left join opp_status t3
on t1.opp_sfid = t3.opp_sfid
left join (select opp_sfid from tmp_leads_prev_update_1 where intended_start_date = '2019-05-28') t4
on t1.opp_sfid = t4.opp_sfid
);
CREATE
OR REPLACE temp TABLE Walden_pipeline_data AS(
select 'Current' as period, t1.*
  ,case when DATE_DIFF(t1.date_closed_lost,t1.inq_Date,day) <= 7 and t1.date_closed_lost < t2.cutoff_date then 1 else 0 end as closed_7day
  ,case when DATE_DIFF(t1.date_closed_lost,t1.inq_Date,day) <= 30 and t1.date_closed_lost < t2.cutoff_date then 1 else 0 end as closed_30day
  ,case when DATE_DIFF(t1.date_closed_lost,t1.inq_Date,day) <= 90 and t1.date_closed_lost < t2.cutoff_date then 1 else 0 end as closed_90day
  ,case when DATE_DIFF(coalesce(t1.date_app,t1.date_appcomplete,t1.date_admitted,t1.date_rs),t1.inq_date,day) <= 7
    and coalesce(t1.date_app,t1.date_appcomplete,t1.date_admitted,t1.date_rs) < t2.cutoff_date then 1 else 0 end as app_7day
  ,case when DATE_DIFF(coalesce(t1.date_app,t1.date_appcomplete,t1.date_admitted,t1.date_rs),t1.inq_date,day) <= 30
    and coalesce(t1.date_app,t1.date_appcomplete,t1.date_admitted,t1.date_rs) < t2.cutoff_date then 1 else 0 end as app_30day
  ,case when DATE_DIFF(coalesce(t1.date_app,t1.date_appcomplete,t1.date_admitted,t1.date_rs),t1.inq_date,day) <= 90
    and coalesce(t1.date_app,t1.date_appcomplete,t1.date_admitted,t1.date_rs) < t2.cutoff_date then 1 else 0 end as app_90day,

from leads_curr_update2 t1
inner join ( select * from opp_param where period = '(a) Current' ) t2 on 1=1

UNION ALL

select 'Previous' as period, t1.*
  ,case when DATE_DIFF(t1.date_closed_lost,t1.inq_Date,day) <= 7 and t1.date_closed_lost < t2.cutoff_date then 1 else 0 end as closed_7day
  ,case when DATE_DIFF(t1.date_closed_lost,t1.inq_Date,day) <= 30 and t1.date_closed_lost < t2.cutoff_date then 1 else 0 end as closed_30day
  ,case when DATE_DIFF(t1.date_closed_lost,t1.inq_Date,day) <= 90 and t1.date_closed_lost < t2.cutoff_date then 1 else 0 end as closed_90day
  ,case when DATE_DIFF(coalesce(t1.date_app,t1.date_appcomplete,t1.date_admitted,t1.date_rs),t1.inq_date,day) <= 7
    and coalesce(t1.date_app,t1.date_appcomplete,t1.date_admitted,t1.date_rs) < t2.cutoff_date then 1 else 0 end as app_7day
  ,case when DATE_DIFF(coalesce(t1.date_app,t1.date_appcomplete,t1.date_admitted,t1.date_rs),t1.inq_date,day) <= 30
    and coalesce(t1.date_app,t1.date_appcomplete,t1.date_admitted,t1.date_rs) < t2.cutoff_date then 1 else 0 end as app_30day
  ,case when DATE_DIFF(coalesce(t1.date_app,t1.date_appcomplete,t1.date_admitted,t1.date_rs),t1.inq_date,day) <= 90
    and coalesce(t1.date_app,t1.date_appcomplete,t1.date_admitted,t1.date_rs) < t2.cutoff_date then 1 else 0 end as app_90day

from leads_prev_update2 t1
inner join ( select * from opp_param where period = '(b) Previous') t2 on 1=1
);

SET base_month_curr = (select base_month from Walden_pipeline_data where period = 'Current' LIMIT 1);
SET diff_curr = (select DATE_DIFF('2020-07-01', base_month_curr,month ));
SET start_date_group_curr = ( case when diff_curr >= 0 then CONCAT('FutureStart_', CAST(diff_curr AS  STRING), 'July')
							else CONCAT('PrevStart_', CAST(-diff_curr AS  STRING), 'July') end );

SET base_month_prev = (select base_month from Walden_pipeline_data where period = 'Previous' LIMIT 1);
SET diff_prev = (select DATE_DIFF('2020-07-01', base_month_prev,month ));
SET start_date_group_prev = ( case when diff_prev >= 0 then CONCAT('FutureStart_', CAST(diff_prev AS  STRING), 'July')
							else CONCAT('PrevStart_', CAST(-diff_prev AS  STRING), 'July') end
							);

CREATE OR REPLACE temp TABLE temp_src
  as (
select DISTINCT
l.period,
l.institution,
l.institution_id,
l.source_system_name,
l.opp_sfid,
l.created_date,
l.inq_month_day1,
l.month_index,
l.groupnewid,
l.brand_profile_sfid,
l.contact_sfid,
l.banner_id,
l.owner_sfid,
l.international_flag,
l.country_name,
l.state,
l.city,
l.activity_id,
l.channel,
l.curr_product_sfid,
l.curr_product_nbr,
COALESCE(r.is_tempo_c,l.is_tempo_flag) as is_tempo_flag,
--case when l.is_tempo_flag = true and l.program_name not like 'CBE%' then false else is_tempo_flag end as is_tempo_flag,
l.military_flag,
l.inq_date,
l.enr_date,
l.date_rs,
l.date_admitted,
l.date_appcomplete,
l.date_app,
l.date_qualified,
l.date_active,
l.date_open,
l.date_closed_lost,
l.date_pause  ,
l.date_pre_opportunity,
l.es_curr_sfid  ,
l.es_curr_name ,
l.es_curr_manager_name,
l.es_curr_director_name,
l.es_curr_division,
l.es_curr_site  ,
l.es_orig_sfid  ,
l.es_orig_name ,
l.es_orig_manager_name,
l.es_orig_director_name,
l.es_orig_division,
l.es_orig_site ,
l.partner_flag,
l.base_month ,
l.cutoff_date,
l.Region_name ,
CAST(TRUNC(l.cohort_age) as INT64) as cohort_age,
l.cohort_month_id,
l.cohort_month,
l.calendar_year_id,
l.intended_start_date,
case when l.matched_start_date = '2020-06-29' then DATE'2020-07-01'
	 else l.intended_start_date2 end as intended_start_date2,
case when r.banner_program_code_c = 'MS_EDUC_1CR' then 'MS Education 1 Credit' else m.program_name end as program_name,
case when m.concentration_description is not null then m.concentration_description else l.specialization end as specialization,
l.program_name_grid,
case when m.level_description is not null then m.level_description else l.level end as level,
case when m.college_code is not null then m.college_code else l.college_code end as college_code,
case when m.sq_flag is not null then m.sq_flag else l.sq_flag end as sq_flag,
l.last_stage,
l.last_disposition,
l.matched_Start_date,
case when l.matched_start_date = '2020-06-29' and l.period = 'Previous' then start_date_group_prev
	 when l.matched_start_date = '2020-06-29' and l.period = 'Current' then start_date_group_curr
	 else l.start_date_group end as start_date_group,
l.ea_name as es_name,
l.ea_manager as es_manager,
l.ea_location as es_location,
l.ea_director as es_director,
l.ea_division as es_division,
l.closed_7day,
l.closed_30day,
l.closed_90day,
l.app_7day,
l.app_30day,
l.app_90day,
case when l.created_by_sfid in (select id from `rpt_crm_mart.v_wldn_manual_user`) then False else True end as manual_flag
from Walden_pipeline_data l
left join `raw_b2c_sfdc.product_2` r on l.curr_product_sfid=r.id and r.is_deleted=false and institution_c='a0ko0000002BSH4AAO'
left join `rpt_academics.t_wldn_product_map` m on l.curr_product_sfid = m.product_sfid
);

CREATE OR REPLACE temp TABLE cutoff_date2_table
 as (
select max(cutoff_date) as cutoff_date
	,date_add(max(cutoff_date), interval -7 day) as cutoff_date2
	from temp_src where period='Previous'
);
CREATE OR REPLACE temp TABLE special_start_date_CBE
 as (
select distinct matched_start_date ,start_date_group FROM temp_src
where period='Previous' and matched_start_date in ( select safe_cast(start_date as date) as start_date
  from `trans_crm_mart.v_walden_pipeline_start_date_cbe`
  where date_diff(safe_cast(start_date_next_year as date),safe_cast(start_date as date),day) in (370, 371)
) and program_name  like 'CBE%'
);
CREATE OR REPLACE temp TABLE special_start_date
 as (
select distinct matched_start_date ,start_date_group FROM temp_src
where period='Previous' and matched_start_date in ( select safe_cast(start_date as date) as start_date
  from `trans_crm_mart.v_walden_pipeline_start_date_regular`
  where date_diff(safe_cast(start_date_next_year as date),safe_cast(start_date as date),day) in (370, 371)
) and program_name not like 'CBE%'
);
CREATE OR REPLACE temp TABLE Status_by_cutoff2
 as (
select *
from (
	select opp_sfid,opp_hist_sfid,created_date,stage_name,disposition
		,row_number() over(partition by opp_sfid order by created_date desc, opp_hist_sfid desc) as rid
	from `rpt_crm_mart.t_opp_status_history`
	where cast(created_date_est as date) < (select cutoff_date2 from cutoff_date2_table)
) t
where rid = 1
);
CREATE OR REPLACE temp TABLE Start_by_cutoff2
 as (
select *
from (
	select opp_sfid,opp_hist_sfid,created_date,start_date
		,row_number() over(partition by opp_sfid order by created_date desc, opp_hist_sfid desc) as rid
	from `rpt_crm_mart.t_opp_start_history`
	where cast(created_date_est as date) < (select cutoff_date2 from cutoff_date2_table)
) t
where rid = 1 and Start_Date in
	(select matched_start_date from special_start_date
		union ALL
	 select matched_start_date from special_start_date_CBE)
);
CREATE OR REPLACE temp TABLE Status_Start_by_cutoff2
 as (
select t1.*, t2.Start_Date, (select cutoff_date2 from cutoff_date2_table) as cutoff_date
from Status_by_cutoff2 t1
inner join Start_by_cutoff2 t2 on t1.Opp_Sfid = t2.Opp_SfId
inner join temp_src t3 on t3.period='Previous' and t1.Opp_SfId = t3.Opp_SFid
where ( t2.Start_Date in (select matched_start_date from special_start_date)
			and t3.program_name not like 'CBE%' )
		or ( t2.Start_Date in (select matched_start_date from special_start_date_CBE)
			and t3.program_name like 'CBE%' )
);
CREATE OR REPLACE temp TABLE dup_opp
 as (
select *
from temp_src
where period='Previous'
	and opp_sfid in ( select opp_sfid from Status_Start_by_cutoff2 )
	and ( ( matched_start_date not in ( select matched_start_date from special_start_date )
		and program_name not like 'CBE%' ) OR
		  ( matched_start_date not in ( select matched_start_date from special_start_date_CBE )
		and program_name like 'CBE%' )
	)
);
CREATE OR REPLACE temp TABLE dup_opp_update
as (
select
t1.period,
t1.institution,
t1.institution_id,
t1.source_system_name,
t1.opp_sfid,
t1.created_date,
t1.inq_month_day1,
t1.month_index,
t1.groupnewid,
t1.brand_profile_sfid,
t1.contact_sfid,
t1.banner_id,
t1.owner_sfid,
t1.international_flag,
t1.country_name,
t1.state,
t1.city,
t1.activity_id,
t1.channel,
t1.curr_product_sfid,
t1.curr_product_nbr,
t1.is_tempo_flag,
t1.military_flag,
t1.inq_date,
t1.enr_date,
t1.date_rs,
t1.date_admitted,
t1.date_appcomplete,
t1.date_app,
t1.date_qualified,
t1.date_active,
t1.date_open,
t1.date_closed_lost,
t1.date_pause  ,
t1.date_pre_opportunity,
t1.es_curr_sfid  ,
t1.es_curr_name ,
t1.es_curr_manager_name,
t1.es_curr_director_name,
t1.es_curr_division,
t1.es_curr_site  ,
t1.es_orig_sfid  ,
t1.es_orig_name ,
t1.es_orig_manager_name,
t1.es_orig_director_name,
t1.es_orig_division,
t1.es_orig_site ,
t1.partner_flag,
t1.base_month ,
(select cutoff_date2 from cutoff_date2_table) as cutoff_date,
t1.Region_name ,
t1.cohort_age,
t1.cohort_month_id,
t1.cohort_month,
t1.calendar_year_id,
t1.intended_start_date,
t1.intended_start_date2,
t1.program_name,
t1.specialization,
t1.program_name_grid,
t1.level,
t1.college_code,
t1.sq_flag,
t2.stage_name as last_stage,
t2.Disposition as last_disposition,
t2.start_date as matched_Start_date,
case when t1.program_name like 'CBE%' then t4.start_date_group else t3.start_date_group END as start_date_group,
t1.es_name,
t1.es_manager,
t1.es_location,
t1.es_director,
t1.es_division,
t1.closed_7day,
t1.closed_30day,
t1.closed_90day,
t1.app_7day,
t1.app_30day,
t1.app_90day,
t1.manual_flag
from dup_opp t1
inner join Status_Start_by_cutoff2 t2 on t1.Opp_SfId = t2.Opp_SfId
inner join special_start_date t3 on t2.start_date = t3.matched_start_date
inner join special_start_date_CBE t4 on t2.start_date = t4.matched_start_date
);
CREATE OR REPLACE temp TABLE non_dup_opp
AS
 (
select
t1.period,
t1.institution,
t1.institution_id,
t1.source_system_name,
t1.opp_sfid,
t1.created_date,
t1.inq_month_day1,
t1.month_index,
t1.groupnewid,
t1.brand_profile_sfid,
t1.contact_sfid,
t1.banner_id,
t1.owner_sfid,
t1.international_flag,
t1.country_name,
t1.state,
t1.city,
t1.activity_id,
t1.channel,
t1.curr_product_sfid,
t1.curr_product_nbr,
t1.is_tempo_flag,
t1.military_flag,
t1.inq_date,
t1.enr_date,
t1.date_rs,
t1.date_admitted,
t1.date_appcomplete,
t1.date_app,
t1.date_qualified,
t1.date_active,
t1.date_open,
t1.date_closed_lost,
t1.date_pause  ,
t1.date_pre_opportunity,
t1.es_curr_sfid  ,
t1.es_curr_name ,
t1.es_curr_manager_name,
t1.es_curr_director_name,
t1.es_curr_division,
t1.es_curr_site  ,
t1.es_orig_sfid  ,
t1.es_orig_name ,
t1.es_orig_manager_name,
t1.es_orig_director_name,
t1.es_orig_division,
t1.es_orig_site ,
t1.partner_flag,
t1.base_month ,
(select cutoff_date2 from cutoff_date2_table) as cutoff_date,
t1.Region_name ,
t1.cohort_age,
t1.cohort_month_id,
t1.cohort_month,
t1.calendar_year_id,
t1.intended_start_date,
t1.intended_start_date2,
t1.program_name,
t1.specialization,
t1.program_name_grid,
t1.level,
t1.college_code,
t1.sq_flag,
t2.stage_name as last_stage,
t2.Disposition as last_disposition,
t2.start_date as matched_Start_date,
case when t1.program_name like 'CBE%' then t4.start_date_group else t3.start_date_group END as start_date_group,
t1.es_name,
t1.es_manager,
t1.es_location,
t1.es_director,
t1.es_division,
t1.closed_7day,
t1.closed_30day,
t1.closed_90day,
t1.app_7day,
t1.app_30day,
t1.app_90day,
t1.manual_flag
from temp_src t1
inner join Status_Start_by_cutoff2 t2 on t1.Opp_SfId = t2.Opp_SfId
inner join special_start_date t3 on t2.start_date = t3.matched_start_date
inner join special_start_date_CBE t4 on t2.start_date = t4.matched_start_date
where t1.period='Previous' and t1.Opp_SfId not in ( select Opp_SfId from dup_opp_update)
);
CREATE OR REPLACE temp TABLE temp_src_update
AS
 (
select DISTINCT
o.period,
o.institution,
o.institution_id,
o.source_system_name,
o.opp_sfid,
o.created_date,
o.inq_month_day1,
o.month_index,
o.groupnewid,
o.brand_profile_sfid,
o.contact_sfid,
o.banner_id,
o.owner_sfid,
o.international_flag,
o.country_name,
o.state,
o.city,
o.activity_id,
o.channel,
o.curr_product_sfid,
o.curr_product_nbr,
o.is_tempo_flag,
o.military_flag,
o.inq_date,
o.enr_date,
o.date_rs,
o.date_admitted,
o.date_appcomplete,
o.date_app,
o.date_qualified,
o.date_active,
o.date_open,
o.date_closed_lost,
o.date_pause  ,
o.date_pre_opportunity,
o.es_curr_sfid  ,
o.es_curr_name ,
o.es_curr_manager_name,
o.es_curr_director_name,
o.es_curr_division,
o.es_curr_site  ,
o.es_orig_sfid  ,
o.es_orig_name ,
o.es_orig_manager_name,
o.es_orig_director_name,
o.es_orig_division,
o.es_orig_site ,
o.partner_flag,
o.base_month ,
o.cutoff_date,
o.Region_name ,
o.cohort_age,
o.cohort_month_id,
o.cohort_month,
o.calendar_year_id,
case when (period='Previous'
	and opp_sfid not in ( select opp_sfid from Status_Start_by_cutoff2 )
	and ( (matched_start_date in ( select matched_start_date from special_start_date )
		and program_name not like 'CBE%' ) OR
		  (matched_start_date in ( select matched_start_date from special_start_date_CBE )
		and program_name like 'CBE%' )
	)) then null else intended_start_date end as intended_start_date,
case when (period='Previous'
	and opp_sfid not in ( select opp_sfid from Status_Start_by_cutoff2 )
	and ( (matched_start_date in ( select matched_start_date from special_start_date )
		and program_name not like 'CBE%' ) OR
		  (matched_start_date in ( select matched_start_date from special_start_date_CBE )
		and program_name like 'CBE%' )
	)) then null else intended_start_date2 end as intended_start_date2,
o.program_name,
o.specialization,
o.program_name_grid,
o.level,
o.college_code,
o.sq_flag,
o.last_stage,
o.last_disposition,
case when (period='Previous'
	and opp_SfId not in ( select opp_sfid from Status_Start_by_cutoff2 )
	and ( (matched_start_date in ( select matched_start_date from special_start_date )
		and program_name not like 'CBE%' ) OR
		  (matched_start_date in ( select matched_start_date from special_start_date_CBE )
		and program_name like 'CBE%' )
	)) then null else matched_Start_date end as matched_Start_date,
case when (period='Previous'
	and opp_SfId not in ( select opp_sfid from Status_Start_by_cutoff2 )
	and ( (matched_start_date in ( select matched_start_date from special_start_date )
		and program_name not like 'CBE%' ) OR
		  (matched_start_date in ( select matched_start_date from special_start_date_CBE )
		and program_name like 'CBE%' )
	)) then null else start_date_group end as start_date_group,
o.es_name,
o.es_manager,
o.es_location,
o.es_director,
o.es_division,
o.closed_7day,
o.closed_30day,
o.closed_90day,
o.app_7day,
o.app_30day,
o.app_90day,
o.manual_flag
from temp_src o
);
CREATE OR REPLACE temp TABLE temp_walden_opp_pipeline
AS
with
src as (
select DISTINCT
o.period,
o.institution,
o.institution_id,
o.source_system_name,
o.opp_sfid,
o.created_date,
utility.udf_convert_UTC_to_EST(o.created_date) as created_date_est,
o.inq_month_day1,
o.month_index,
o.groupnewid,
o.brand_profile_sfid,
o.contact_sfid,
o.banner_id,
o.owner_sfid,
o.international_flag,
o.country_name,
o.state,
o.city,
o.activity_id,
o.channel,
o.curr_product_sfid,
o.curr_product_nbr,
o.is_tempo_flag,
o.military_flag,
o.inq_date,
o.enr_date,
o.date_rs,
o.date_admitted,
o.date_appcomplete,
o.date_app,
o.date_qualified,
o.date_active,
o.date_open,
o.date_closed_lost,
o.date_pause  ,
o.date_pre_opportunity,
o.es_curr_sfid  ,
o.es_curr_name ,
o.es_curr_manager_name,
o.es_curr_director_name,
o.es_curr_division,
o.es_curr_site  ,
o.es_orig_sfid  ,
o.es_orig_name ,
o.es_orig_manager_name,
o.es_orig_director_name,
o.es_orig_division,
o.es_orig_site ,
o.partner_flag,
o.base_month ,
COALESCE(d.cutoff_date,n.cutoff_date,o.cutoff_date) as cutoff_date,
o.Region_name ,
o.cohort_age,
o.cohort_month_id,
o.cohort_month,
o.calendar_year_id,
o.intended_start_date,
o.intended_start_date2,
o.program_name,
o.specialization,
o.program_name_grid,
o.level,
o.college_code,
o.sq_flag,
COALESCE(d.last_stage,n.last_stage,o.last_stage) as last_stage,
COALESCE(d.last_disposition,n.last_disposition,o.last_disposition) as last_disposition,
COALESCE(d.matched_Start_date,n.matched_Start_date,o.matched_Start_date) as matched_Start_date,
COALESCE(d.start_date_group,n.start_date_group,o.start_date_group) as start_date_group,
o.es_name,
o.es_manager,
o.es_location,
o.es_director,
o.es_division,
o.closed_7day,
o.closed_30day,
o.closed_90day,
o.app_7day,
o.app_30day,
o.app_90day,
o.manual_flag
from temp_src_update o
left join non_dup_opp n on o.opp_sfid=n.opp_sfid
left join dup_opp_update d on o.opp_sfid=d.opp_sfid
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
END;
