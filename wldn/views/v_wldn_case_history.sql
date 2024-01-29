CREATE or REPLACE VIEW `rpt_crm_mart.v_wldn_case_history` AS
select
ch.id,
ch.is_deleted,
ch.case_id,
ch.created_by_id,
ch.created_date,
ch.field,
ch.old_value,
ch.new_value,
ch._fivetran_synced,
ch.data_type
from `raw_b2c_sfdc.case_history` ch
left join (select distinct id, opportunity_c, contact_id  from `raw_b2c_sfdc.case` where is_deleted = false) c on ch.case_id = c.id
left join (select distinct id, 	institution_code_c  from `raw_b2c_sfdc.contact` where is_deleted = false) co on c.contact_id = co.id
where ch.is_deleted=false
and lower(institution_code_c)='walden'