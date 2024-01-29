create or replace view rpt_crm_mart.v_wldn_email_message as
select
id,
parent_id,
activity_id,
created_by_id,
created_date,
last_modified_date,
last_modified_by_id,
system_modstamp,
text_body,
html_body,
headers,
subject,
from_name,
from_address,
validated_from_address,
to_address,
cc_address,
bcc_address,
incoming,
has_attachment,
status,
message_date,
is_deleted,
reply_to_email_message_id,
is_externally_visible,
message_identifier,
thread_identifier,
is_client_managed,
related_to_id,
email_message_id_c,
additional_to_c,
_fivetran_synced,
gsfs_action_outcome_c,
gsfs_action_c,
first_opened_date,
last_opened_date,
is_opened,
email_template_id,
is_tracked,
is_bounced
from `raw_b2c_sfdc.email_message`
where
is_deleted=false and
(lower(to_address) like '%walden%' OR lower(to_address) like '%laureate%' OR lower(to_address) like '%liverpool%' OR lower(to_address) like '%roehampton%'
OR lower(from_address) like '%walden%' OR lower(from_address) like '%laureate%' OR lower(from_address) like '%liverpool%' OR lower(from_address) like '%roehampton%'
OR lower(validated_from_address) like '%walden%' OR lower(validated_from_address) like '%laureate%' OR lower(validated_from_address) like '%liverpool%' OR lower(validated_from_address) like '%roehampton%')