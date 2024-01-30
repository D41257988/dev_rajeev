CREATE OR REPLACE FUNCTION `daas-cdw-dev.rpt_crm_mart.f_cu_crm_net_prospects_task`(subject1 STRING, subject2 STRING, subject3 STRING) AS (
array(
with contacted_tasks as (
select 
distinct id as task_id, 
        who_id as whoid, 
        what_id as whatid, 
        owner_id as ownerid, 
        created_date as activity_created_date,
        case when '' like ('RN_%') or '' like ('Grad_%') or '' like ('Pre_%') then true else false end as dialer_touched_ind, -- replace the '' with skill_c
        case when upper('') like ('%_ASC_%') then 'ASC' when upper('') like ('%_CAMPUS_%') then 'CAMPUS' else null end as asc_touched, -- replace the '' with skill_c
        subject,
        '' as missed_appointment, -- missing from Task
        NULL as missed_appointment_date_time, -- missing from Task
        NULL as appointment_scheduled_time_local_text -- missing from Task

from   raw_b2c_sfdc.task  
where is_deleted = False
and extract(date from created_date) > date_add(current_date, interval -8 year)
and upper(subject) in (subject1, subject2, subject3)
and what_id in (select id from raw_b2c_sfdc.opportunity where is_deleted=false AND institution_c in ('a0kDP000008l7bvYAA')) -- rs - filter for CU 
)

-----------------------------------------------------------------------------------------------------------------------------------------------------------------
select as struct prospect_id, inquiry_id, trim(opportunity_id) as opportunity_id, task_id, task_owner_id, campaign_id, inquiry_scoring_tier, inquiry_created_date,
	   task_created_date, original_intended_start_date, session_start_date, approved_application_date, prospect_status, prospect_owner_id, location_code,
	   modality_type, program_group_code, dialer_touched_ind, ifnull(asc_touched, null) as asc_touched_ind, drips_state from (
select *, row_number() over (partition by prospect_id, inquiry_id order by inquiry_created_date, task_created_date, ifnull(task_id,'zzz') asc) as inquiry_task_rank from

-- Main KPI starts here
-- If there are duplicates coming from both Lead & Account, select the one that has minimum date
(select distinct prospect_id, inquiry_id, min(ifnull(opportunity_id,'zzz')) over (partition by prospect_id, inquiry_id, program_group_code) as opportunity_id, task_id,
		task_owner_id, campaign_id, inquiry_scoring_tier, inquiry_created_date, prospect_owner_id,
		case when cast(task_created_date as string)='01/01/9999' then null else task_created_date end as task_created_date,
		approved_application_date, session_start_date, original_intended_start_date, prospect_status, location_code, modality_type, program_group_code, dialer_touched_ind,
		asc_touched, count(*) over (partition by task_id) as cnt,
		case when (count(*) over (partition by task_id))>1 and min(task_created_date) over (partition by prospect_id, inquiry_id, task_id) = task_created_date then 1
		     when (count(*) over (partition by task_id))=1 then 1 else 0 end as select_flag, group_flag, drips_state from

 -- Pick the minimum between ACTIVITY_CREATED_DATE & CONTACT_DATE & CONTACT_DATE from Lead
(select distinct prospect_id, inquiry_id, opportunity_id, task_id, task_owner_id, campaign_id, inquiry_scoring_tier, inquiry_created_date, prospect_owner_id
	,least(ifnull(l_contact_date, timestamp(parse_date('%d/%m/%Y','01/01/9999'))), ifnull(contact_date, timestamp(parse_date('%d/%m/%Y','01/01/9999'))),
	ifnull(task_created_date, timestamp(parse_date('%d/%m/%Y','01/01/9999'))) ) as task_created_date, approved_application_date, session_start_date, original_intended_start_date,
	prospect_status, location_code, modality_type, program_group_code, dialer_touched_ind, asc_touched, group_flag, drips_state from

-- Assign the tasks to the appropriate prospect/inquiry; Check whether Activity created date & Contact date if exists are within the 2 inquiries
(select distinct prospect_id, inquiry_id, opportunity_id, campaign_id, inquiry_scoring_tier, inquiry_created_date, contact_date, l_contact_date, prospect_owner_id,
		case when activity_created_date is null then null else task_id end as task_id, case when activity_created_date is null then null else ownerid end as task_owner_id,
		activity_created_date as task_created_date, approved_application_date, session_start_date, original_intended_start_date, prospect_status, location_code, modality_type,
		program_group_code, dialer_touched_ind, asc_touched, group_flag, drips_state from

-- Identify ACTIVITY_CREATED_DATE & CONTACT_DATE lies between which 2 inquiries
(select distinct prospect_id, inquiry_id, opportunity_id, task_id, ownerid, campaign_id, inquiry_scoring_tier, inquiry_created_date, next_inquiry_date, approved_application_date, 		  session_start_date, original_intended_start_date
		,case when activity_created_date is not null and (activity_created_date >= inquiry_created_date ) then activity_created_date else null end as activity_created_date
		,case when contact_date is not null and (contact_date >= inquiry_created_date ) then contact_date else null end as contact_date
		,case when l_contact_date is not null and (l_contact_date >= inquiry_created_date ) then l_contact_date else null end as l_contact_date
		,prospect_status, prospect_owner_id, location_code, modality_type, program_group_code, dialer_touched_ind, asc_touched, group_flag, drips_state from (

-- To get all Prospects with valid Inquiries and all related Contacted Tasks linked to Lead Id
(select * from (select distinct prospect_id, inquiry_id, max(opportunity_id) over (partition by prospect_id) as opportunity_id, task_id, contact_id, ownerid, campaign_id,
		inquiry_scoring_tier, prospect_created_date, inquiry_created_date, activity_created_date, next_inquiry_date, approved_application_date, session_start_date,
		original_intended_start_date, ri_contact_date as contact_date, l_contact_date, opp_contact_date, prospect_status, prospect_owner_id, location_code, modality_type,
		program_group_code, dialer_touched_ind, asc_touched, missed_appointment, missed_appointment_date_time, appointment_scheduled_time_local_text, subject, assign_opp,
		'LEAD' as group_flag, drips_state
from rpt_crm_mart.v_cu_kpi_unique_prospects up
left join
 -- Get all Contacted Tasks
contacted_tasks t1 on t1.whoid = up.prospect_id and whoid is not null
) where coalesce(contact_date, l_contact_date, activity_created_date) is not null  )

union all

-- To get all Prospects with valid Inquiries and all related Contacted Tasks linked to Contact Id
(select * from (select distinct prospect_id, inquiry_id, max(opportunity_id) over (partition by prospect_id) as opportunity_id, task_id, contact_id, ownerid, campaign_id,
		inquiry_scoring_tier, prospect_created_date, inquiry_created_date, activity_created_date, next_inquiry_date, approved_application_date, session_start_date,
		original_intended_start_date, ri_contact_date as contact_date, l_contact_date, opp_contact_date, prospect_status, prospect_owner_id, location_code, modality_type,
		program_group_code, dialer_touched_ind, asc_touched, missed_appointment, missed_appointment_date_time, appointment_scheduled_time_local_text, subject, assign_opp, 'CONTACT' AS GROUP_FLAG, drips_state
FROM rpt_crm_mart.v_cu_kpi_unique_prospects UP
left join
 -- Get all Contacted Tasks
contacted_tasks t2 on t2.whoid = up.contact_id and whoid is not null
) where coalesce(contact_date, l_contact_date, activity_created_date) is not null )

union all

-- To get all prospects with valid Inquiries and all related Contacted Tasks linked to Opportunity Id /Account Id
(select * from (select distinct prospect_id, inquiry_id, max(opportunity_id) over (partition by prospect_id) as opportunity_id, task_id, contact_id, ownerid, campaign_id,
		inquiry_scoring_tier, prospect_created_date, inquiry_created_date, activity_created_date, next_inquiry_date, approved_application_date, session_start_date,
		original_intended_start_date, max(opp_contact_date) over (partition by prospect_id) as contact_date, l_contact_date, opp_contact_date, prospect_status, prospect_owner_id,
		location_code, modality_type, opp_program_group_code as program_group_code, dialer_touched_ind, asc_touched, missed_appointment, missed_appointment_date_time,
		appointment_scheduled_time_local_text, subject, assign_opp, 'ACCOUNT' as group_flag, drips_state
from rpt_crm_mart.v_cu_kpi_unique_prospects up
left join
 -- Get all Contacted Tasks
contacted_tasks t3 on t3.whatid = up.opportunity_id and whatid is not null
) where assign_opp=1 and coalesce(contact_date, l_contact_date, activity_created_date) is not null
)


))
) where coalesce(contact_date, l_contact_date, task_created_date) is not null
) ) where select_flag=1  )

where inquiry_task_rank=1

)
);
