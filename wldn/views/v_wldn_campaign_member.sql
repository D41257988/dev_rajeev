CREATE or replace VIEW `rpt_crm_mart.v_wldn_campaign_member` AS
select distinct cm.id,
cm.is_deleted,
cm.campaign_id,
cm.lead_id,
cm.contact_id,
cm.status,
cm.has_responded,
cm.created_date,
cm.created_by_id,
cm.last_modified_date,
cm.last_modified_by_id,
cm.system_modstamp,
cm.first_responded_date,
cm.salutation,
cm.name,
cm.first_name,
cm.last_name,
cm.title,
cm.street,
cm.city,
cm.state,
cm.postal_code,
cm.country,
cm.email,
cm.phone,
cm.fax,
cm.mobile_phone,
cm.description,
cm.do_not_call,
cm.has_opted_out_of_email,
cm.has_opted_out_of_fax,
cm.lead_source,
cm.company_or_account,
cm.type,
cm.lead_or_contact_id,
cm.lead_or_contact_owner_id,
cm.date_received_c,
cm.date_reviewed_c,
cm.status_c,
cm._fivetran_synced
from raw_b2c_sfdc.campaign_member cm
left join
(select distinct id, institution_code_c from raw_b2c_sfdc.contact where is_deleted=false) c on  cm.campaign_id  = c.id
-- left join
-- (select distinct id, institution_code_c from raw_b2c_sfdc.contact where is_deleted=false) on cm.contact_id  = c.id
where
cm.is_deleted=false AND
c.institution_code_c in ('Walden')