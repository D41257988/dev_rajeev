with writing_period as (
select upper(d.cu_online_writing_period) as writing_period, min(d.cal_dt) as wp_begin_date, max(d.cal_dt) as wp_end_date from mdm.dim_date
       d where d.cu_online_writing_period is not null group by d.cu_online_writing_period
)
, activity_tasks as (
select distinct who_id as whoid,
			what_id as whatid,
			created_date as activity_created_date,
			case when skill_c like ('RN_%') or skill_c like ('Grad_%') or
	   skill_c like ('Pre_%') then 1 else 0 end as dialer_touched

		 from raw_b2c_sfdc.task where
       is_deleted=False
			and extract(date from created_date) > date_add(current_date, interval -8 year)
			and what_id in (select id from raw_b2c_sfdc.opportunity where is_deleted=false AND institution_c in ('a0kDP000008l7bvYAA'))
)

select distinct prospect_id, inquiry_id, inquiry_created_date, location_code, program_code, program_group_code, campaign_id, prospect_owner_id, prospect_status,
		inquiry_scoring_tier, response_score, hdyhau, attendance_preference, prospect_type, address_country_inquiry, address_state_inquiry, address_postal_code_inquiry,
		writing_period, case when max(dialer_touched)=1 then 'TRUE' when max(dialer_touched)=0 then 'FALSE' else null end as dialer_touched_ind

		from (

select up.*, t.dialer_touched from (
(select distinct prospect_id, inquiry_id, contact_id, opportunity_id, inquiry_created_date, location_code, program_code, program_group_code, campaign_id, prospect_owner_id,
		prospect_status, inquiry_scoring_tier, response_score, hdyhau, attendance_preference, prospect_type, address_country_inquiry, address_state_inquiry,
		address_postal_code_inquiry, writing_period, wp_begin_date, wp_end_date from (
		-- Linking only to the first inquiry within the writing period
		select distinct np.*, writing_period, wp_begin_date, wp_end_date, row_number() over (partition by prospect_id, writing_period order by inquiry_created_date,
			   ifnull(opp_create_date, timestamp(parse_date('%d/%m/%Y','01/01/9999')) ) asc) as rank
		from rpt_crm_mart.v_cu_kpi_unique_prospects np
		-- Get writing period
		left join writing_period d
			on extract(date from cast(datetime(inquiry_created_date, "US/Central") as datetime)) between wp_begin_date and wp_end_date
		where modality_type='ONLINE' and program_group_code not in ('BSN', 'BSN_ONLINE')
) where rank=1 ) up
-- Get all the tasks with dialer activities and falls within the writing period
left join activity_tasks t
    on up.prospect_id = t.whoid and extract(date from cast(datetime(t.activity_created_date, "US/Central") as datetime)) between wp_begin_date and wp_end_date
)

union all

select up.*, t.dialer_touched from (
(select distinct prospect_id, inquiry_id, contact_id, opportunity_id, inquiry_created_date, location_code, program_code, program_group_code, campaign_id, prospect_owner_id,
		prospect_status, inquiry_scoring_tier, response_score, hdyhau, attendance_preference, prospect_type, address_country_inquiry, address_state_inquiry,
		address_postal_code_inquiry, writing_period, wp_begin_date, wp_end_date from (
		-- Linking only to the first inquiry within the writing period
		select distinct np.*, writing_period, wp_begin_date, wp_end_date, row_number() over (partition by prospect_id, writing_period order by inquiry_created_date,
			   ifnull(opp_create_date, timestamp(parse_date('%d/%m/%Y','01/01/9999')) ) asc) as rank
		from rpt_crm_mart.v_cu_kpi_unique_prospects np
		-- Get writing period
		left join writing_period d
			on extract(date from cast(datetime(inquiry_created_date, "US/Central") as datetime)) between wp_begin_date and wp_end_date
		where modality_type='ONLINE' and program_group_code not in ('BSN', 'BSN_ONLINE')
) where rank=1 ) up
-- Get all the tasks with dialer activities and falls within the writing period
left join activity_tasks t
      on up.contact_id=t.whoid and extract(date from cast(datetime(t.activity_created_date, "US/Central") as datetime)) between wp_begin_date and wp_end_date
)

union all

select up.*, t.dialer_touched from (
(select distinct prospect_id, inquiry_id, contact_id, opportunity_id, inquiry_created_date, location_code, program_code, program_group_code, campaign_id, prospect_owner_id,
		prospect_status, inquiry_scoring_tier, response_score, hdyhau, attendance_preference, prospect_type, address_country_inquiry, address_state_inquiry,
		address_postal_code_inquiry, writing_period, wp_begin_date, wp_end_date FROM (
		-- Linking only to the first inquiry within the writing period
		select distinct np.*, writing_period, wp_begin_date, wp_end_date, row_number() over (partition by prospect_id, writing_period order by inquiry_created_date,
			   ifnull(opp_create_date, timestamp(parse_date('%d/%m/%Y','01/01/9999')) ) asc) as rank
		from rpt_crm_mart.v_cu_kpi_unique_prospects np
		-- Get writing period
		left join writing_period d
			on extract(date from cast(datetime(inquiry_created_date, "US/Central") as datetime)) between wp_begin_date and wp_end_date
		where modality_type='ONLINE' and program_group_code not in ('BSN', 'BSN_ONLINE')
) where rank=1 ) up
-- Get all the tasks with dialer activities and falls within the writing period
left join activity_tasks t
      on up.opportunity_id = t.whatid and extract(date from cast(datetime(t.activity_created_date, "US/Central") as datetime)) between wp_begin_date and wp_end_date
)


) group by prospect_id, inquiry_id, inquiry_created_date, location_code, program_code, program_group_code, campaign_id, prospect_owner_id, prospect_status, inquiry_scoring_tier,
response_score, hdyhau, attendance_preference, prospect_type, address_country_inquiry, address_state_inquiry, address_postal_code_inquiry, writing_period