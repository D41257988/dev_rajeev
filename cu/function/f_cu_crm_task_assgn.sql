CREATE OR REPLACE FUNCTION `daas-cdw-prod.rpt_crm_mart.f_cu_crm_task_assgn`(taskname STRING, subject1 STRING, subject2 STRING) AS (
array(
with contacted_tasks as (
select distinct id as task_id, whoid, whatid, ownerid, createddate as activity_created_date, case when calling_queue like ('RN_%') or calling_queue like ('Grad_%') or
	   calling_queue like ('Pre_%') then 'TRUE' else 'FALSE' end as dialer_touched_ind, case when calling_queue like ('Pre_%') then 'Y' else 'N' end as asc_touched from
       rpt_crm_mart.t_cu_crm_activity where is_current=true
	   and extract(date from createddate) > date_add(current_date, interval -8 year)
	   and sfdc_object='TASK' and upper(assignment_status) = taskname and (upper(subject) = subject1 or upper(subject) = subject2 )
)

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

select as struct prospect_id, inquiry_id, opportunity_id, task_id, task_owner_id, campaign_id, inquiry_scoring_tier, inquiry_created_date, task_created_date
       ,original_intended_start_date, session_start_date, approved_application_date, location_code, modality_type, program_group_code, dialer_touched_ind, asc_touched
       ,row_number() over (partition by prospect_id order by inquiry_created_date, task_created_date asc) as inquiry_rank from

-- main kpi starts here
(select distinct prospect_id, contact_id, inquiry_id, opportunity_id, task_id, ownerid as task_owner_id, campaign_id, inquiry_scoring_tier, prospect_created_date
        ,inquiry_created_date, task_created_date, approved_application_date
        ,row_number() over (partition by contact_id, task_id order by prospect_created_date desc) as prospect_rank
        ,session_start_date, original_intended_start_date, location_code, modality_type, program_group_code, dialer_touched_ind, asc_touched from

-- If there are duplicates coming from both Lead & Account, select the one that has minimum date
(select distinct prospect_id, contact_id, inquiry_id, min(ifnull(opportunity_id,'zzz')) over (partition by prospect_id, inquiry_id, program_group_code) as opportunity_id, task_id,
		ownerid, campaign_id, inquiry_scoring_tier, prospect_created_date, inquiry_created_date,
		task_created_date, approved_application_date, session_start_date, original_intended_start_date, location_code, modality_type, program_group_code, dialer_touched_ind,
		count(*) over (partition by task_id) as cnt,  case when (count(*) over (partition by task_id))>1 and min(task_created_date) over (partition by prospect_id, inquiry_id,
		task_id) = task_created_date then 1 when (count(*) over (partition by task_id))=1 then 1 else 0 end as select_flag, asc_touched, group_flag from

-- Assign the tasks to the appropriate prospect/inquiry; Check whether Activity created date & Contact date if exists are within the 2 inquiries
(select distinct prospect_id, contact_id, inquiry_id, opportunity_id,
		case when activity_created_date between inquiry_created_date and datetime_sub(next_inquiry_date, interval 1 second) then task_id else null end as task_id,
		ownerid, campaign_id, inquiry_scoring_tier, prospect_created_date, inquiry_created_date, contact_date, next_inquiry_date, approved_application_date, session_start_date,
		case when activity_created_date between inquiry_created_date and datetime_sub(next_inquiry_date, interval 1 second) then activity_created_date else null end as task_created_date,
		original_intended_start_date, location_code, modality_type, program_group_code, dialer_touched_ind, asc_touched, group_flag from (

 -- To get all Prospects with valid Inquiries and all related Contacted Tasks linked to Lead Id
(select distinct prospect_id, contact_id, inquiry_id, opportunity_id, task_id, ownerid, campaign_id, inquiry_scoring_tier, inquiry_created_date, ri_contact_date as contact_date,
  	    activity_created_date, next_inquiry_date, prospect_created_date, approved_application_date, session_start_date, original_intended_start_date, location_code, modality_type,
 		program_group_code, dialer_touched_ind, asc_touched, assign_opp, 'LEAD' as group_flag
-- Get the APPROVED_APPLICATION_DATE for each inquiry, NULL is not found
from rpt_crm_mart.v_cu_kpi_unique_prospects up
inner join
-- Get all Contacted Tasks
contacted_tasks t on t.whoid=up.prospect_id and whoid is not null
where ((approved_application_date is not null and activity_created_date <= approved_application_date) or approved_application_date is null)
)

union all

 -- To get all Prospects with valid Inquiries and all related Contacted Tasks linked to Contact Id
(select distinct prospect_id, contact_id, inquiry_id, opportunity_id, task_id, ownerid, campaign_id, inquiry_scoring_tier, inquiry_created_date, ri_contact_date as contact_date,
 		 activity_created_date, next_inquiry_date, prospect_created_date, approved_application_date, session_start_date, original_intended_start_date, location_code,
		modality_type, program_group_code, dialer_touched_ind, asc_touched, assign_opp, 'CONTACT' as group_flag
from rpt_crm_mart.v_cu_kpi_unique_prospects up
inner join
-- Get all Contacted Tasks
contacted_tasks t on t.whoid=up.contact_id and whoid is not null
where ((approved_application_date is not null and activity_created_date <= approved_application_date) or approved_application_date is null)
)

union all

 -- To get all prospects with valid Inquiries and all related Contacted Tasks linked to Opportunity Id /Account Id
(select * from (select distinct prospect_id, contact_id, inquiry_id, opportunity_id, task_id, ownerid, campaign_id, inquiry_scoring_tier, inquiry_created_date,
		opp_contact_date as contact_date, activity_created_date, next_inquiry_date, prospect_created_date, approved_application_date, session_start_date,
		original_intended_start_date, location_code, modality_type, program_group_code, dialer_touched_ind, asc_touched, assign_opp, 'ACCOUNT' as group_flag
from rpt_crm_mart.v_cu_kpi_unique_prospects up
inner join
-- Get all Contacted Tasks
contacted_tasks t on t.whatid=up.opportunity_id and whatid is not null
where (approved_application_date is not null and activity_created_date <= approved_application_date) or approved_application_date is null
) where assign_opp=1 )

) ) where (task_id is not null or (task_id is null and contact_date is not null))
) where select_flag=1 ) where prospect_rank=1

)
);