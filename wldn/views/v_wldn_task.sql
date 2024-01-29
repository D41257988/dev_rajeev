
select
id,
record_type_id,
who_id,
what_id,
who_count,
what_count,
subject,
activity_date,
status,
priority,
is_high_priority,
owner_id,
description,
type,
is_deleted,
account_id,
is_closed,
created_date,
created_by_id,
last_modified_date,
last_modified_by_id,
system_modstamp,
call_duration_in_seconds,
call_type,
call_disposition,
call_object,
reminder_date_time,
is_reminder_set,
recurrence_activity_id,
is_recurrence,
recurrence_start_date_only,
recurrence_end_date_only,
recurrence_time_zone_sid_key,
recurrence_type,
recurrence_interval,
recurrence_day_of_week_mask,
recurrence_day_of_month,
recurrence_instance,
recurrence_month_of_year,
recurrence_regenerated_type,
task_subtype,
--task_template_id_c,
call_answered_time_c,
call_disconnected_time_c,
call_initiated_time_c,
call_wrapup_time_c,
talk_time_c,
wrapup_time_in_seconds_c,
hold_time_in_seconds_c,
ring_time_in_seconds_c,
talk_time_in_seconds_c,
country_c,
call_notes_c,
-- db_activity_type_c, -- rs - need to find the substitue column from b2c dataset
eligibility_program_of_interest_2_c,
eligibility_program_of_interest_3_c,
eligibility_program_of_interest_c,
-- notes_c, -- rs - need to find the substitue column from b2c dataset
phone_text_c,
phone_c,
status_c,
tracking_id_c,
-- two_way_contact_occurred_c, -- rs - need to find the substitue column from b2c dataset
type_c,
home_phone_c,
mobile_phone_c,
state_c,
timezone_c,
work_phone_c,
-- z_test_c, -- rs - need to find the substitue column from b2c dataset
reminder_date_c,
click_to_dial_click_to_call_phone_c,
-- g_4_s_glance_duration_c, -- rs - need to find the substitue column from b2c dataset
-- g_4_s_glance_end_time_c, -- rs - need to find the substitue column from b2c dataset
-- g_4_s_glance_guests_c,-- rs - need to find the substitue column from b2c dataset
-- g_4_s_glance_location_c,-- rs - need to find the substitue column from b2c dataset
-- g_4_s_glance_session_key_c,-- rs - need to find the substitue column from b2c dataset
-- g_4_s_glance_session_type_c,-- rs - need to find the substitue column from b2c dataset
-- g_4_s_glance_start_time_c,-- rs - need to find the substitue column from b2c dataset
most_recent_reinquiry_date_c,
campaign_code_c,
referral_discussed_c,
tempo_task_id_c,
icrt_ae_guide_name_c,
marketo_event_guid_c,
_fivetran_synced,
do_not_run_obm_process_c,
wait_time_c,
called_number_c,
caller_number_c,
transactionid_c,
gsfs_action_outcome_c,
gsfs_further_investigation_c,
gsfs_action_c,
sst_type_action_c,
next_call_date_c,
opportunity_tracking_id_c,
previously_scheduled_call_c,
infinity_activity_id_c,
infinity_call_reference_id_c,
infinity_installation_id_c,
opportunity_tracking_c,
completed_date_time,
forcebrain_phone_reminder_c,
forcebrain_created_by_sumo_c,
forcebrain_email_reminder_c,
forcebrain_second_text_reminder_c,
forcebrain_appointment_id_c,
forcebrain_notify_provider_c,
forcebrain_text_reminder_c,
chat_queue_c,
agent_average_response_time_c,
agent_message_count_c,
agent_maximum_response_c,
-- created_date_custom_c, -- rs - need to find the substitue column from b2c dataset
is_visible_in_self_service,
from `raw_b2c_sfdc.task`
where is_deleted = false