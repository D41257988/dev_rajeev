create or replace view rpt_crm_mart.v_cu_kpi_total_attempted_contacts_net_prospects as
with contacted_tasks as
(
select
	id as task_id,
	who_id as whoid,
	what_id as whatid,
	owner_id as task_owner_id,
	created_date as activity_created_date,
	   case when skill_c like ('RN_%')
                  or skill_c like ('Grad_%')
                  or skill_c like ('Pre_%')
          then true else false end as dialer_touched_ind,

     case when upper(skill_c) like ('%_ASC_%') then 'ASC'
          when upper(skill_c) like ('%_CAMPUS_%') then 'CAMPUS'     else null end as asc_touched
  from raw_b2c_sfdc.task
where is_deleted = False
      and upper(subject)
                in ('ATTEMPTED CONTACT', 'LEFT VOICEMAIL', 'ASC ATTEMPTED CONTACT', 'CONTACTED', 'DIALER CONTACTED', 'ASC CONTACTED')
      and extract(date from created_date) > date_add(current_date, interval -8 year)
      and what_id in (select id from raw_b2c_sfdc.opportunity where is_deleted=false AND institution_c in ('a0kDP000008l7bvYAA')) -- rs - filter for CU
)

select distinct
	prospect_id,
	inquiry_id,
	opportunity_id,
	task_id,
	task_owner_id,
	campaign_id,
	inquiry_scoring_tier,
	inquiry_created_date,
	task_created_date,
	original_intended_start_date,
	session_start_date,
	approved_application_date,
	prospect_status,
	prospect_owner_id,
	location_code,
	modality_type,
	program_group_code,
	dialer_touched_ind, ifnull(asc_touched, null) as asc_touched_ind from (
select *, row_number() over (partition by prospect_id, inquiry_id order by inquiry_created_date, task_created_date, ifnull(task_id,'zzz') asc) as inquiry_task_rank from

-- Main KPI starts here
-- If there are duplicates coming from both Lead & Account, select the one that has minimum date
(select distinct prospect_id, inquiry_id, min(ifnull(opportunity_id,'zzz')) over (partition by prospect_id, inquiry_id, program_group_code) as opportunity_id, task_id,
		task_owner_id, campaign_id, inquiry_scoring_tier, inquiry_created_date,
		case when cast(task_created_date as string)='01/01/9999' then null else task_created_date end as task_created_date,
		approved_application_date, session_start_date, original_intended_start_date, prospect_status, prospect_owner_id, location_code, modality_type, program_group_code,
		case when (count(*) over (partition by task_id))>1 and min(task_created_date) over (partition by prospect_id, inquiry_id, task_id) = task_created_date then 1
		     when (count(*) over (partition by task_id))=1 then 1 else 0 end as select_flag,
		dialer_touched_ind, asc_touched, group_flag from

-- Pick the minimum between ACTIVITY_CREATED_DATE & ATTEMPTED_CONTACT_DATE & ATTEMPTED_CONTACT_DATE from Lead
(select distinct prospect_id, inquiry_id, opportunity_id, task_id, task_owner_id, campaign_id, inquiry_scoring_tier, inquiry_created_date
		,least(
					ifnull(l_attempted_contact_date, timestamp(parse_date('%d/%m/%Y','01/01/9999'))), -- rs - converted timestamp into date for consistency
					ifnull(attempted_contact_date, timestamp(parse_date('%d/%m/%Y','01/01/9999'))),
			   	ifnull(task_created_date, timestamp(parse_date('%d/%m/%Y','01/01/9999')))
				 ) as task_created_date
		,approved_application_date, session_start_date, original_intended_start_date, prospect_status, prospect_owner_id, location_code, modality_type, program_group_code,
		dialer_touched_ind, asc_touched, group_flag from

-- Assign the tasks to the appropriate prospect/inquiry; Check whether Activity created date & Contact date if exists are within the 2 inquiries
(select distinct prospect_id, inquiry_id, opportunity_id, campaign_id, inquiry_scoring_tier, inquiry_created_date, attempted_contact_date, l_attempted_contact_date,
		case when activity_created_date is null then null else task_id end as task_id, case when activity_created_date is null then null else task_owner_id end as task_owner_id,
		activity_created_date as task_created_date, approved_application_date, session_start_date, original_intended_start_date, prospect_status, prospect_owner_id, location_code,
		modality_type, program_group_code, dialer_touched_ind, asc_touched, group_flag from

 -- Identify ACTIVITY_CREATED_DATE & ATTEMPTED_CONTACT_DATE lies between which 2 inquiries
(select distinct prospect_id, inquiry_id, opportunity_id, task_id, task_owner_id, campaign_id, inquiry_scoring_tier, inquiry_created_date, next_inquiry_date, approved_application_date,
		session_start_date, original_intended_start_date
 		,case when activity_created_date is not null and (activity_created_date >= inquiry_created_date ) then activity_created_date else null end as activity_created_date
		,case when attempted_contact_date is not null and (attempted_contact_date >= inquiry_created_date ) then attempted_contact_date else null end as attempted_contact_date
		,case when l_attempted_contact_date is not null and (l_attempted_contact_date >= inquiry_created_date ) then l_attempted_contact_date else null end as l_attempted_contact_date
		,prospect_status, prospect_owner_id, location_code, modality_type, program_group_code, dialer_touched_ind, asc_touched, group_flag FROM (

(
	select distinct prospect_id, inquiry_id, opportunity_id, task_id, task_owner_id,
    campaign_id, inquiry_created_date,
    ri_attempted_contact_date as attempted_contact_date,
		l_attempted_contact_date, opp_attempted_contact_date,
		session_start_date, original_intended_start_date,
    activity_created_date, next_inquiry_date, approved_application_date,
		prospect_status, prospect_owner_id, inquiry_scoring_tier, location_code,
    modality_type, program_group_code, dialer_touched_ind, asc_touched,
    assign_opp, 'LEAD' as group_flag
from rpt_crm_mart.v_cu_kpi_unique_prospects up
inner join
contacted_tasks t
on t.whoid=up.prospect_id and whoid is not null
where coalesce(ri_attempted_contact_date, l_attempted_contact_date, activity_created_date) is not null
)

union all

(
	select distinct prospect_id, inquiry_id, opportunity_id, task_id, task_owner_id,
    campaign_id, inquiry_created_date,
    ri_attempted_contact_date as attempted_contact_date,
		l_attempted_contact_date, opp_attempted_contact_date, session_start_date, original_intended_start_date,
    activity_created_date, next_inquiry_date, approved_application_date,
		prospect_status, prospect_owner_id, inquiry_scoring_tier, location_code,
    modality_type, program_group_code, dialer_touched_ind, asc_touched,
    assign_opp, 'CONTACT' as group_flag from
 rpt_crm_mart.v_cu_kpi_unique_prospects up
inner join
contacted_tasks t
on t.whoid=up.contact_id and whoid is not null
where coalesce(ri_attempted_contact_date, l_attempted_contact_date, activity_created_date) is not null
)

union all

(
	select distinct prospect_id, inquiry_id, opportunity_id, task_id, task_owner_id,
    campaign_id, inquiry_created_date,
    opp_attempted_contact_date as attempted_contact_date,
    l_attempted_contact_date, opp_attempted_contact_date, session_start_date, original_intended_start_date,
    activity_created_date, next_inquiry_date, approved_application_date,
    prospect_status, prospect_owner_id, inquiry_scoring_tier, location_code,
    modality_type, program_group_code, dialer_touched_ind, asc_touched,
		assign_opp, 'ACCOUNT' as group_flag from
 rpt_crm_mart.v_cu_kpi_unique_prospects up
inner join
contacted_tasks t
on t.whatid=up.opportunity_id and whatid is not null
where assign_opp=1 and coalesce(opp_attempted_contact_date, l_attempted_contact_date, activity_created_date) is not null
)


))

) where coalesce(attempted_contact_date, l_attempted_contact_date, task_created_date) is not null
) ) where select_flag=1  ) where inquiry_task_rank=1
