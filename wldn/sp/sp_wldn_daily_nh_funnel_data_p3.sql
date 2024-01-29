CREATE OR REPLACE PROCEDURE `daas-cdw-dev.trans_crm_mart.sp_wldn_daily_nh_funnel_data_p3`(IN v_audit_key STRING, OUT result STRING)
begin

    declare institution string default 'WLDN';
    declare institution_id int64 default 5;
    declare dml_mode string default 'delete-insert';
    declare target_dataset string default 'rpt_crm_mart';
    declare target_tablename string default 't_wldn_daily_nh_funnel_data_p3';
    declare source_tablename string default 'temp_walden_daily_nh_funnel_data_p3';
    declare load_source string default 'trans_crm_mart.sp_wldn_daily_nh_funnel_data_p3';
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

	declare last_enr_dt date default (select max(student_start_date) from `trans_crm_mart.prc_wldn_student_retention_details` where reconciled_flag in ('Y', '1'));
	declare cur_cutoff_dt datetime default date_add(DATE'1900-01-01',INTERVAL date_diff(CAST(utility.udf_convert_UTC_to_EST(CURRENT_TIMESTAMP()) AS DATE),DATE'1900-01-01',day)day);
	declare adj_enr int64 default date_diff( last_enr_dt, cur_cutoff_dt,day )+5;
	declare today date default date_add(DATE'1900-01-01',INTERVAL date_diff(CAST(utility.udf_convert_UTC_to_EST(CURRENT_TIMESTAMP()) AS DATE),DATE'1900-01-01',day)day) ;
	declare yesterday date default date_add(today,INTERVAL -1 day ) ;
	declare cur_begin date default DATE_ADD( DATE'1900-01-01',INTERVAL DATE_DIFF(yesterday,DATE'1900-01-01',month)-11 month) ;
	declare cur_end date default today ;
	declare cur_status_cutoff date default today ;
	declare cur_enr_cutoff date default today + (adj_enr);
	declare c1 BIGNUMERIC default 0.0075;
	declare c2 BIGNUMERIC default 0.02;
	declare c3 BIGNUMERIC default 0.05;
	declare c4 BIGNUMERIC default 0.10;
	declare adj_opp int64 default 0;

    BEGIN
    SET additional_attributes= [("audit_load_key", v_audit_key),
              ("load_method",load_method),
              ("load_source",load_source),
              ("job_type", job_type)];
    /* end common across */

---Start of Table---
create or replace temp table param as (
with
tmp_param as(
select
  'Current' as Period
	,cur_begin as Begin_dttm
	,cur_end as End_dttm
	,cur_status_cutoff as Status_cutoff_dttm
	,cur_enr_cutoff as Enr_cutoff_dttm
UNION ALL
select
  'PrevPrev' as Period
	,DATE_ADD( cur_begin,INTERVAL -2 YEAR  ) as Begin_dttm
	,DATE_ADD( cur_end,INTERVAL -2 YEAR  ) as End_dttm
	,DATE_ADD( cur_status_cutoff,INTERVAL -2 YEAR  ) as Status_cutoff_dttm
	,DATE_ADD( cur_enr_cutoff,INTERVAL -2 YEAR  ) as Enr_cutoff_dttm
)
select *
  ,cast(Begin_dttm as date ) as begin_date
  ,cast(End_dttm as date ) as end_date
  ,cast(Status_cutoff_dttm as date ) as status_cutoff_date
  ,cast(Enr_cutoff_dttm as date ) as enr_cutoff_date from tmp_param
  where Period <> 'Current'
);

create or replace temp table leads_reinq as (
with reinq as (
select opp_sfid,
brand_profile_sfid,
contact_sfid,
created_date,
onyx_incident_key,
year as year,
month as month,
year_month,
banner_id,
billing_country,
billing_state,
billing_city,
billing_country_text,
billing_state_text,
international_flag,
country_name,
state,
state_cd,
city,
activity_id,
channel,
curr_product_sfid as product_sfid,
curr_product_nbr as product_nbr,
curr_program_name as program_name,
curr_level as level,
curr_college_code as college_name,
sq_flag,
mailing_country,
is_tempo_flag,
inq_date,
enr_date,
date_rs,
date_admitted,
date_appcomplete as date_appcomplete,
date_appSubmitted,
date_app as date_app,
date_qualified,
date_active,
date_open,
date_closed_lost,
date_pause,
date_pre_opportunity,
stage_name,
disposition,
current_stage_disposition_timestamp,
selected_program_start_date,
military_flag,
term_cd,
second_term_retention,
third_term_retention,
start_date_term2,
start_date_term3,
es_curr_name,
es_curr_manager_name,
es_curr_site,
es_orig_name,
es_orig_manager_name,
es_orig_site,
es_curr_director_name,
es_curr_division,
es_orig_director_name,
es_orig_division,
partner_flag,
curr_customer_friendly_poi_name,
created_by_sfid,
ROW_NUMBER () OVER (partition by brand_profile_sfid order by inq_date) - 1 as reinquiry_id
from `rpt_crm_mart.t_wldn_opp_snapshot`
),
reinq2 as (
select t1.* ,t2.channel as channel_Initial
	,t2.program_name as program_initial ,t2.college_name as college_initial ,t2.level as level_initial
	,t2.inq_date as inq_date_Initial
	,DATE_DIFF(t1.inq_date, t2.inq_date, day ) as day_diff_initial
	,t2.opp_sfid as OppSFId_initial
from reinq t1
left join reinq t2 on t1.brand_profile_sfid = t2.brand_profile_sfid and t1.reinquiry_id >= 1 and t2.reinquiry_id = 0
),
reinq3 as (
select t1.* ,t2.channel as channel_prev
	,t2.program_name as program_prev ,t2.college_name as college_prev ,t2.level as level_prev
	,t2.inq_date as inq_date_prev
	,DATE_DIFF(t1.inq_date, t2.inq_date, day ) as day_diff_prev
	,t2.opp_sfid as OppSFid_prev
from reinq2 t1
left join reinq t2 on t1.brand_profile_sfid = t2.brand_profile_sfid and t1.reinquiry_id - 1 = t2.reinquiry_id
),
reinq4 as (
select *
	,case when 0 <= day_diff_initial and day_diff_initial < 7 then '(a) < 7 days'
		when day_diff_initial < 30 then '(b) >= 7 and < 30 days'
		when day_diff_initial < 90 then '(c) >= 30 and < 90 days'
		when day_diff_initial < 180 then '(d) >= 90 and < 180 days'
		when day_diff_initial >= 180 then '(e) >= 180 days'
		else null end as type_diff_initial
	,case when 0 <= day_diff_prev and day_diff_prev < 7 then '(a) < 7 days'
		when day_diff_prev < 30 then '(b) >= 7 and < 30 days'
		when day_diff_prev < 90 then '(c) >= 30 and < 90 days'
		when day_diff_prev < 180 then '(d) >= 90 and < 180 days'
		when day_diff_prev >= 180 then '(e) >= 180 days'
		else null end as type_diff_prev
from reinq3
),
leads1 as (
select t2.Period
	,DATE_ADD( DATE'1900-01-01' ,INTERVAL date_diff(inq_date, DATE'1900-01-01',month ) month) as leadsmonth
	,1 as ind_Inq
	,case when enr_date < enr_cutoff_date then 1 else 0 end as ind_Enr
	,case when date_rs < status_cutoff_date then 1 else 0 end as ind_RS
	,case when date_admitted < status_cutoff_date then 1 else 0 end as ind_Admitted
	,case when date_appcomplete < status_cutoff_date then 1 else 0 end as ind_AppComplete
	,case when Date_AppSubmitted < Status_cutoff_date then 1 else 0 end as ind_AppSubmitted
	,case when date_app < status_cutoff_date then 1 else 0 end as ind_App
	,case when date_qualified < status_cutoff_date then 1 else 0 end as ind_Qualified
	,case when date_active < status_cutoff_date then 1 else 0 end as ind_Active
	,case when date_open < status_cutoff_date then 1 else 0 end as ind_Open
	,case when date_closed_lost < status_cutoff_date then 1 else 0 end as ind_Closed_Lost
	,case when date_pause < status_cutoff_date then 1 else 0 end as ind_Pause
	,case when date_pre_opportunity < status_cutoff_date then 1 else 0 end as ind_Pre_Opportunity
	,opp_sfid,
  brand_profile_sfid,
  contact_sfid,
  t1.created_date,
  onyx_incident_key,
  year as year,
  month as month,
  year_month,
  banner_id,
  billing_country,
  billing_state,
  billing_city,
  billing_country_text,
  billing_state_text,
  international_flag,
  country_name,
  state,
  state_cd,
  city,
  activity_id,
  channel,
  curr_product_sfid as product_sfid,
  curr_product_nbr as product_nbr,
  curr_program_name as program_name,
  curr_level as level,
  curr_college_code as college_name,
  sq_flag,
  mailing_country,
  is_tempo_flag,
  inq_date,
  enr_date,
  date_rs,
  date_admitted,
  date_appcomplete as date_appcomplete,
  date_appSubmitted,
  date_app as date_app,
  date_qualified,
  date_active,
  date_open,
  date_closed_lost,
  date_pause,
  date_pre_opportunity,
  stage_name,
  disposition,
  current_stage_disposition_timestamp,
  selected_program_start_date,
  military_flag,
  term_cd,
  second_term_retention,
  third_term_retention,
  start_date_term2,
  start_date_term3,
  es_curr_name,
  es_curr_manager_name,
  es_curr_site,
  es_orig_name,
  es_orig_manager_name,
  es_orig_site,
  es_curr_director_name,
  es_curr_division,
  es_orig_director_name,
  es_orig_division,
  partner_flag,
  curr_customer_friendly_poi_name,
  created_by_sfid,
  substring(t3.billing_postal_code_c,1,5) as zip
from `rpt_crm_mart.t_wldn_opp_snapshot` t1
inner join param t2 on t2.begin_date <= t1.inq_date and t1.inq_date < t2.end_date
left join raw_b2c_sfdc.brand_profile_c t3 on t1.brand_profile_sfid=t3.id and t3.is_deleted=false and institution_c='a0ko0000002BSH4AAO'

),

leads2 as(
select
  (SELECT MAX(v) AS max FROM UNNEST([ind_Enr, ind_RS, ind_Admitted, ind_AppComplete,ind_AppSubmitted, ind_App]) AS v) as flag_App
  ,(SELECT MAX(v) AS max FROM UNNEST([ind_Enr, ind_RS, ind_Admitted, ind_AppComplete,ind_AppSubmitted]) AS v) as flag_AppSubmitted
  ,(SELECT MAX(v) AS max FROM UNNEST([ind_Enr, ind_RS, ind_Admitted, ind_AppComplete]) AS v) as flag_AppComplete_raw
  ,(SELECT MAX(v) AS max FROM UNNEST([ind_Enr, ind_RS, ind_Admitted]) AS v) as flag_Admitted_raw
  ,(SELECT MAX(v) AS max FROM UNNEST([ind_Enr, ind_RS]) AS v) as flag_RS_raw
	,t1.*
from leads1 t1
)
select t1.*
	,Reinquiry_ID
	,Channel_Initial
	,Channel_Prev
	,type_diff_initial
	,type_diff_prev
	,Inq_date_Initial
	,Inq_date_Prev
	,OppSFId_initial
	,OppSFid_prev
	,Program_Initial ,College_Initial ,Level_Initial
	,Program_Prev ,College_Prev ,Level_Prev
from leads2 t1
left join reinq4 t2 on t1.opp_sfid = t2.opp_sfid
);
create or replace temp table app2opp as (
with
app as (
select concat(a.credential_id,'-',a.application_number,'-',a.academic_period) as application_id
,a.credential_id
,a.start_date
,a.product_nbr
,a.application_date
,cast (a.academic_period as integer) as term_cd
,o.oppsfid as opp_sfid

 from `rpt_academics.v_admissions_application` a
 left join `rpt_academics.v_wldn_map_sales_opp_app` o on concat(a.credential_id,'-',a.application_number,'-',a.academic_period)=o.application_id
),
app2 as (
  select a.*
,p.program_name as program_name
,p.college_code as college_name
,p.SQ_flag as sq_flag
,p.level_description as level_desc,
 from app a
 left join `rpt_academics.t_wldn_product_map` p on a.product_nbr = p.product_nbr
),
map_person as (
select distinct Person_BNR_Id ,BrandProfileSFId as brand_profile_sfid
from `rpt_academics.v_wldn_map_person`
),

app3 as (
  select t1.*
	,t2.brand_profile_sfid
	,t3.opp_sfid as opp_OppSFId
	,t3.curr_level as opp_level ,t3.curr_college_code as opp_college ,'NA' as opp_program
	,t3.curr_product_name as opp_ProductNbr ,replace(replace(replace(t4.full_banner_program_code_c, '_11', ''  ), '_2', ''), '_1', '') as opp_Banner_Pgm_Cd
	,t3.Inq_date ,t3.Enr_date
	,t3.date_app as date_app ,t3.date_appcomplete as date_appcomplete ,t3.date_admitted ,t3.date_rs as date_rs
from app2 t1
left join map_person t2 on t1.credential_id = t2.Person_BNR_Id
left join `rpt_crm_mart.t_wldn_opp_snapshot` t3 on t2.brand_profile_sfid = t3.brand_profile_sfid
left join `raw_b2c_sfdc.product_2`  t4 on t3.curr_product_sfid = t4.id and t4.is_deleted=false
where t1.application_date >= t3.Inq_date
and institution_c = 'a0ko0000002BSH4AAO'
union all
select t1.*
	,t3.brand_profile_sfid
	,t3.opp_sfid as opp_OppSFId
	,t3.curr_level as opp_level ,t3.curr_college_code as opp_college ,'NA' as opp_program
	,t3.curr_product_name as opp_ProductNbr ,t4.banner_program_code_c as opp_Banner_Pgm_Cd
	,t3.Inq_date ,t3.Enr_date
	,t3.date_app as date_app ,t3.date_appcomplete as date_appcomplete ,t3.date_admitted ,t3.date_rs as date_rs
from app2 t1
left join `rpt_crm_mart.t_wldn_opp_snapshot` t3 on t1.opp_sfid = t3.opp_sfid
left join `raw_b2c_sfdc.product_2` t4 on t3.curr_product_sfid = t4.id and t4.is_deleted=false
where t1.application_date >= t3.Inq_date
and institution_c = 'a0ko0000002BSH4AAO'
),
app4 as (
select *
	,case when product_nbr = opp_Banner_pgm_cd OR product_nbr = opp_ProductNbr then 0
		when PROGRAM_NAME = opp_program then 1
		when level_desc = opp_level and college_name = opp_college then 2
		when level_desc = opp_level then 3
		else 9 end as product_match_index
from app3
)
select *
from (
	select *
		,ROW_NUMBER () OVER ( partition by application_id
			order by case when opp_sfid = opp_oppSFId then 0 else 1 end
				,product_match_index
				,case when date_rs IS NOT NULL then 0
					when date_admitted IS NOT NULL then 0
					when date_appcomplete IS NOT NULL then 0
					when date_app IS NOT NULL then 0
					else 9 end
				,Inq_date desc ) as rid
	from app4
) t
where rid = 1
);
create or replace temp table banner_status_map (
	Status_cd STRING,
	Status_desc STRING,
	Stage STRING,
	Disposition STRING
);
insert banner_status_map (Status_cd,Status_desc,Stage,Disposition)
values
('AC','Admissions Committee Review','Applicant','Admissions Review in Progress'),
('AD','Admitted','Applicant','Admitted'),
('AN','Admissions for Non Degree','Applicant','Admitted'),
('CA','Contingent Admit','Applicant','Admitted'),
('CD','Conditional Low GPA','Applicant','Admitted'),
('CG','Contingent/Conditional Low GPA','Applicant','Admitted'),
('CN','Contingent/Conditional NRD','Applicant','Admitted'),
('CX','CA/CD Low GPA NRD','Applicant','Admitted'),
('GN','Conditional Low GPA/Non-Degree','Applicant','Admitted'),
('ND','Conditional NRD','Applicant','Admitted'),
('OL','Ops Conditional Admit Offer','Applicant','Admitted'),
('OR','Ops Regular Admit Offer','Applicant','Admitted'),
('PA','Provisional','Applicant','Admitted'),
('PC','Provisional/Contingent','Applicant','Admitted'),
('PD','Provisional/Conditional LowGPA','Applicant','Admitted'),
('PP','Pending Prep Progrm Completion','Applicant','Admitted'),
('PX','PA/CA/CD Low GPA','Applicant','Admitted'),
('CC','Complete with Concerns','Applicant','Admissions Review in Progress'),
('CO','Complete Application','Applicant','Admissions Review in Progress'),
('FD','Foward to Academics','Applicant','Admissions Review in Progress'),
('AP','Application Submitted','Applicant','New'),
('IN','Incomplete File Return','Applicant','In Process'),
('RC','Resrvd Registration Complete','Pre-enroll','Registered'),
('RI','Resrvd Registration Incomplete','Pre-enroll','Reserved'),
('RS','Reserved','Pre-enroll','Reserved'),
('DA',	'Denied Non Degree Admission',	'Closed Lost',	'App Denied'),
('DN',	'Denied',	'Closed Lost',	'App Denied'),
('NQ',	'Not Qualified',	'Closed Lost',	'App Denied'),
('NR',	'No Response to Offer',	'Closed Lost',	'App Abandoned'),
('WD',	'Withdrawn Application',	'Closed Lost',	'App Withdrawn')
;
create or replace temp table app_status_history as (
with status_history as (
select  concat(credential_id,'-',application_number,'-',academic_period) as application_id,
case when latest_decision_date >= '2015-12-07' and latest_decision = 'AP' then 'CO' else latest_decision end as Status_Cd,
case when latest_decision_date >= '2015-12-07' and latest_decision = 'AP' then 'Complete Application' else latest_decision_desc end as Status_Desc,
latest_decision_date as Decision_Effective_Timestamp
from `rpt_academics.v_admissions_application`
where concat(credential_id,'-',application_number,'-',academic_period) <> 'A00603801-4-201570'
)

select t1.*
	,t2.Stage ,t2.Disposition
	,t3.stage_rank ,t3.disposition_rank ,t3.overall_rank
from status_history t1
inner join banner_status_map t2 on t1.Status_Cd = t2.Status_cd
left join `raw_wldn_manualfiles.manual_opp_stage_disp_ranking` t3
	on t2.Stage = t3.stage_name and t2.Disposition = t3.Disposition and t3.institution_id = 5
);
create or replace temp table seg_campaign as (
select *
		,case when lower(Mark_Campaign) in ( "", "unknown" ) then 1 else 0 end as ind_missing_MarkCampaign
		,case when lower(Mark_Campaign_Sub) in ( "", "unknown" ) then 1 else 0 end as ind_missing_MarkCampaignSub
		,case when lower(Placement) in ( "", "unknown" ) then 1 else 0 end as ind_missing_Placement
	from `raw_wldn_manualfiles.marketing_funnel_seg`
	order by Activity_Id ,ind_missing_MarkCampaign ,ind_missing_MarkCampaignSub ,ind_missing_Placement
);
create or replace temp table seg_geo as (
select CountryName, MarketGeo
	from `trans_crm_mart.v_seg_geo`
	where CountryName <> ""
  order by CountryName
);
create or replace temp table leads_reinq6 as (
with
app_status_history2 as (
select t1.opp_sfid ,t1.period ,t4.status_cutoff_date ,t4.status_cutoff_dttm ,
row_number() over(partition by t1.opp_sfid order by t3.overall_rank desc) as rid,
t3.*
from leads_reinq t1
inner join ( select distinct Opp_OppSFid ,application_id from app2opp ) t2 on t1.opp_sfid = t2.Opp_OppSFid
inner join app_status_history t3 on t2.application_id = t3.application_id
inner join param t4 on t1.period = t4.period
where t3.decision_effective_timestamp < t4.status_cutoff_dttm
order by t1.opp_sfid ,t3.overall_rank desc
),
app_status_history3 as (
  select * from app_status_history2
  where rid=1
),
app_status_history4 as (
select *
		,case when ( stage = "Applicant" and disposition in ( "Admissions Review in Progress", "Admitted" ) )
				OR stage = "Pre-enroll" then 1 else 0 end as Banner_CO_flag
		,case when ( stage = "Applicant" and disposition in ( "Admitted" ) ) OR stage = "Pre-enroll" then 1 else 0 end as Banner_AD_flag
		,case when stage = "Pre-enroll" then 1 else 0 end as Banner_RS_flag
	from app_status_history3
),
leads_reinq2 as (
select t1.*
		,case when t2.Banner_CO_flag = 1 then 1 else t1.flag_AppComplete_raw end as flag_AppComplete
		,case when t2.Banner_AD_flag = 1 then 1 else t1.flag_Admitted_raw end as flag_Admitted
		,case when t2.Banner_RS_flag = 1 then 1 else t1.flag_RS_raw end as flag_RS
	from leads_reinq t1
	left join app_status_history4 t2 on t1.opp_sfid = t2.opp_sfid
),
prev_status_history_p1 as (
	select t1.opp_sfid ,t1.status_date ,t1.stage_name ,t1.disposition ,t2.period,
  (select distinct overall_rank from `raw_wldn_manualfiles.manual_opp_stage_disp_ranking` t2 where t1.stage_name = t2.stage_name and t1.disposition = t2.disposition) as overall_rank
	from `rpt_crm_mart.t_opp_status_history` t1
	inner join leads_reinq t2 on t1.opp_sfid = t2.OppSFid_prev
	inner join param t3 on t2.period = t3.period
	where t1.status_date < t3.status_cutoff_date
),
prev_status_history_p2 as (
	select t1.OppSFid_prev as opp_sfid ,t1.period ,t3.*
	from (select * from leads_reinq where OppSFid_prev is not null) t1
	inner join (select distinct Opp_OppSFid ,application_id from app2opp ) t2 on t1.OppSFid_prev = t2.Opp_OppSFid
	inner join app_status_history t3 on t2.application_id = t3.application_id
	inner join param t4 on t1.period = t4.period
	where t3.decision_effective_timestamp < t4.status_cutoff_dttm
),
prev_status_history as (
	select opp_sfid ,status_date ,stage_name ,disposition ,overall_rank ,period
	from prev_status_history_p1
	union all
	select opp_sfid ,decision_effective_timestamp as status_date ,Stage ,Disposition ,overall_rank ,period
	from prev_status_history_p2
),

prev_status_history2 as (
select * from (select *, ROW_NUMBER() OVER(partition by opp_sfid order by overall_rank desc ) as rid from prev_status_history)
where rid=1
),
leads_reinq3 as (
	select t1.*
		,t2.stage_name as Stage_prev
		,t2.disposition as Disposition_prev
	from leads_reinq2 t1
	left join prev_status_history2 t2 on t1.OppSFid_prev = t2.opp_sfid
),
 leads_reinq4 as (
	select t1.* ,t2.Name as ActivityName,t3.DMA_ID ,t3.DMA_Name
	from leads_reinq3 t1
	left join (select activity_id_c,Name from `raw_b2c_sfdc.campaign`
  where lower(activity_id_c) not in ('unknown','-99999999') and is_deleted=false and institution_c = 'a0ko0000002BSH4AAO'
  ) t2
		on t1.activity_id = t2.activity_id_c
  left join `raw_wldn_manualfiles.zip_dma_mapping` t3 on safe_cast(t1.zip as int64)=t3.Zip
 )

/*Rajeev - 10/10/2023 - adding code for communication_channel*/
select t1.*
		,t2.Mark_Campaign as MarkCampaign ,t2.Mark_Campaign_Sub as MarkCampaignSub ,t2.Placement
		,t3.MarketGeo
		,case when lead_source in ('Phone call','Warm Transfer') then 'Call'
      when lead_source = 'Email' then 'Email'
      when lead_source='Chat' or comments_c like '%Lead Originated via 8x8 Chat%' then 'Chat'
      else 'RFI' end as  communication_channel
	from leads_reinq4 t1
	left join seg_campaign t2 on t1.activity_id = t2.Activity_Id
	left join seg_geo t3 on t1.country_name = t3.CountryName
	left join raw_b2c_sfdc.opportunity o on  t1.opp_sfid=o.id
	where o.institution_c='a0ko0000002BSH4AAO' and o.is_deleted = false
);
/*
create or replace temp table opp_score as (
select t2.opp_sfid,t2.curr_level as level,t2.channel,t2.inq_date,t2.international_flag as Intl_flag
	,prediction as Score
	,1+ case when Prediction>c1 then 1 else 0 end + case when Prediction>c2 then 1 else 0 end + case when Prediction>c3 then 1 else 0 end + case when Prediction>c4 then 1 else 0 end as Score_Group
from `trans_crm_mart.t_wldn_top_of_funnel_cohort_2017`  t1
inner join `rpt_crm_mart.t_wldn_opp_snapshot` t2 on t1.OppSfId = t2.opp_sfid
);

set adj_opp = date_diff((select max(inq_date) from leads_reinq6 ), (select max(inq_date) from opp_score), DAY);
*/
CREATE
OR REPLACE temp TABLE `temp_walden_daily_nh_funnel_data_p3`
AS
with
leads_reinq7 as (
	select t1.*
	--t3.Score ,t3.Score_group,
	,null as Score ,null as Score_group,
  case when t1.activity_id in (
		'WAL-1017574'
		,'WAL-1017574'
		,'WAL-1017586'
		,'WAL-1017587'
		,'WAL-1017588'
		,'WAL-1017589'
		,'WAL-1017590') then 'Y' else 'N' end as reclassified_flag
    ,t2.status_cutoff_date ,t2.enr_cutoff_date
		,case when t1.inq_date <= date_app and date_app < status_cutoff_date then 1 else null end as ct_inq_app
		,case when t1.inq_date <= date_app and date_app < status_cutoff_date then date_diff(date_app,t1.inq_date,day) else null end as days_inq_app
		,case when t1.inq_date <= date_appComplete and date_appComplete < status_cutoff_date then 1 else null end as ct_inq_appComplete
		,case when t1.inq_date <= date_appComplete and date_appComplete < status_cutoff_date then date_diff(date_appComplete,t1.inq_date,day) else null end as days_inq_appComplete
		,case when t1.inq_date <= date_RS and date_RS < status_cutoff_date then 1 else null end as ct_inq_RS
		,case when t1.inq_date <= date_RS and date_RS < status_cutoff_date then date_diff(date_RS,t1.inq_date,day) else null end as days_inq_RS
		,case when t1.inq_date - 7 <= enr_date and enr_date < enr_cutoff_date then 1 else null end as ct_inq_enr
		,case when t1.inq_date - 7 <= enr_date and enr_date < enr_cutoff_date then date_diff(enr_date,t1.inq_date,day) else null end as days_inq_enr
		,case when DATE'1900-01-01'< date_app and date_app <= date_appComplete and date_appComplete < status_cutoff_date then 1 else null end as ct_app_appComplete
		,case when DATE'1900-01-01'< date_app and date_app <= date_appComplete and date_appComplete < status_cutoff_date then date_diff(date_appComplete,date_app,day) else null end as days_app_appComplete
		,case when DATE'1900-01-01'< date_RS - 7 and date_RS - 7 <= enr_date and enr_date < enr_cutoff_date then 1 else null end as ct_RS_enr
		,case when DATE'1900-01-01'< date_RS - 7 and date_RS - 7 <= enr_date and enr_date < enr_cutoff_date then date_diff(enr_date,date_RS,day) else null end as days_RS_enr

		,case when t1.inq_date <= date_appSubmitted and  date_appSubmitted < status_cutoff_date then 1 else null end as ct_inq_appSubmitted
		,case when t1.inq_date <= date_appSubmitted and  date_appSubmitted < status_cutoff_date then date_diff(date_appSubmitted, t1.inq_date,day) else null end as days_inq_appSubmitted
		,case when DATE'1900-01-01' < date_app and  date_app <= date_appSubmitted and date_appSubmitted < status_cutoff_date then 1 else null end as ct_app_appSubmitted
		,case when DATE'1900-01-01' < date_app and  date_app <= date_appSubmitted and date_appSubmitted < status_cutoff_date then date_diff(date_appSubmitted, date_app, day) else null end as days_app_appSubmitted
    ,case when t1.created_by_sfid in (select id from `rpt_crm_mart.v_wldn_manual_user`) then False else True end as manual_flag
	from leads_reinq6 t1
	left join param t2 on t2.period = t1.period
	--left join opp_score t3 on t1.opp_sfid = t3.opp_sfid and t1.inq_date < t2.end_date - adj_opp

),
leads_reinq8 as (
  select l.* except(days_inq_enr,days_RS_enr,program_name)
  ,case when days_inq_enr < 0 and days_inq_enr > -21 then 0 else days_inq_enr end as days_inq_enr
  ,case when days_RS_enr < 0 and days_RS_enr > -21 then 0 else days_RS_enr end as days_RS_enr
  ,case when r.banner_program_code_c = 'MS_EDUC_1CR' then 'MS Education 1 Credit' else l.program_name end as program_name,
   from leads_reinq7 l
  left join `raw_b2c_sfdc.product_2` r on l.product_sfid=r.id and r.is_deleted=false and institution_c='a0ko0000002BSH4AAO'
),
inq_start_history as (
	select t1.opp_sfid,t1.start_date,t1.created_date as created_date_rid, t2.inq_date, t2.period ,t3.status_cutoff_dttm,
	from `rpt_crm_mart.t_opp_start_history`  t1
	inner join leads_reinq8 t2 on t1.opp_sfid = t2.opp_sfid
	inner join param t3 on t2.period = t3.period
	where cast(t1.created_date_est as DATE) < t3.status_cutoff_dttm
	--order by t1.opp_sfid ,t1.created_date desc
),
inq_start_history2 as (
select *
  ,ROW_NUMBER() OVER(partition by opp_sfid order by created_date_rid desc, start_date desc) as rid_1
  ,case when extract(day from start_date) >= 24 then DATE_ADD(start_date, INTERVAL 1 MONTH)
  when extract(month from start_date) in (3,6,9,12) then start_date
  when start_date = DATE'2020-06-29' then DATE'2020-07-01' else start_date  end as start_date2

from inq_start_history
),
inq_start_history2a as (
select *
,case when 0 <= date_diff(start_date2,inq_date,month) and date_diff(start_date2,inq_date,month) <= 12 then cast(start_date as STRING)
  when date_diff(start_date2,inq_date,month) > 3 then '3 months later'
	else '' end as Start_Date_Label
  ,start_date as start_date_yoy
	from inq_start_history2
),
inq_start_history3 as (
  select * from inq_start_history2a
  where rid_1=1
),
leads_reinq11 as(
	select t1.* ,t2.Start_date_Label,start_date_yoy
	from leads_reinq8 t1
	left join inq_start_history3 t2 on t1.opp_sfid = t2.opp_sfid
),
src as (
  select distinct
'WLDN'AS institution,
5 AS institution_id,
'WLDN_SF' as source_system_name,

flag_App,
flag_AppSubmitted,
flag_AppComplete_raw,
flag_Admitted_raw,
flag_RS_raw,
Period,
leadsmonth as leads_month,
ind_Inq,
ind_Enr,
ind_RS,
ind_Admitted,
ind_AppComplete,
ind_AppSubmitted,
ind_App,
ind_Qualified,
ind_Active,
ind_Open,
ind_Closed_Lost,
ind_Pause,
ind_Pre_Opportunity,
opp_sfid,
'NA' as group_new_id,
brand_profile_sfid,
contact_sfid,
created_date,
utility.udf_convert_UTC_to_EST(created_date) as created_date_est,
safe_cast(onyx_incident_key as BIGNUMERIC) as onyx_incident_key,
Year,
Month,
year_month,
banner_id,
billing_country,
billing_state,
billing_city,
billing_country_text,
billing_state_text,
international_flag,
Country_Name,
'NA' as Country_Name_source,
State,
State_cd,
City,
activity_id,
Channel,
product_sfid,
product_nbr,
Program_Name,
Level,
College_Name,
SQ_flag,
mailing_country,
is_tempo_flag as is_tempo,
Inq_date,
Enr_Date,
Date_RS,
Date_Admitted,
Date_AppComplete,
Date_AppSubmitted,
Date_App,
Date_Qualified,
Date_Active,
Date_Open,
Date_Closed_Lost,
Date_Pause,
Date_Pre_Opportunity,
stage_name,
Disposition,
current_stage_disposition_timestamp as stage_disp_date,
selected_program_start_date,
military_flag,
term_cd as enroll_term_cd,
second_term_retention,
third_term_retention,
start_date_term2,
start_date_term3,
es_curr_name as ES_Name_Curr,
es_curr_manager_name as ES_Manager_Curr,
es_curr_site as Enrollment_Site_Curr,
es_orig_name as ES_Name_Orig,
es_orig_manager_name as ES_Manager_Orig,
es_orig_site as Enrollment_Site_Orig,
es_curr_director_name as ES_Director_Curr,
es_curr_division as ES_Division_Curr,
es_orig_director_name as ES_Director_Orig,
es_orig_division as ES_Division_Orig,
partner_flag,
Zip,
Reinquiry_ID,
Channel_Initial,
Channel_Prev,
type_diff_initial,
type_diff_prev,
Inq_date_Initial,
Inq_date_Prev,
OppSFId_initial,
OppSFid_prev,
Program_Initial,
College_Initial,
Level_Initial,
Program_Prev,
College_Prev,
Level_Prev,
flag_AppComplete,
flag_Admitted,
flag_RS,
Stage_prev,
Disposition_prev,
ActivityName as activity_name,
DMA_ID,
DMA_Name,
MarkCampaign as mark_campaign,
MarkCampaignSub as mark_campaign_sub,
Placement,
MarketGeo,
communication_channel,
--Score,
--Score_Group,
manual_flag,
reclassified_flag,
Status_cutoff_date,
Enr_cutoff_date,
ct_inq_app,
days_inq_app,
ct_inq_appComplete,
days_inq_appComplete,
ct_inq_RS,
days_inq_RS,
ct_inq_enr,
days_inq_enr,
ct_app_appComplete,
days_app_appComplete,
ct_RS_enr,
days_RS_enr,
ct_inq_appSubmitted,
days_inq_appSubmitted,
ct_app_appSubmitted,
days_app_appSubmitted,
Start_Date_Label,
start_date_yoy,
curr_customer_friendly_poi_name as customer_friendly_poi_name
from leads_reinq11
union all
select * except(etl_created_date,etl_updated_date,etl_resource_name,etl_ins_audit_key,etl_upd_audit_key,etl_pk_hash,etl_chg_hash) from `rpt_crm_mart.t_wldn_daily_nh_funnel_data`
)
---End of Table---
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