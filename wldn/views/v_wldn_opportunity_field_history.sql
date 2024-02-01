create or replace view rpt_crm_mart.v_wldn_opportunity_field_history as
select
oh.id,
is_deleted,
opportunity_id,
created_by_id,
created_date,
field,
old_value,
new_value,
_fivetran_synced,
data_type
from `raw_b2c_sfdc.opportunity_field_history` oh
left join (select id,  institution_c from `raw_b2c_sfdc.opportunity` where is_deleted = false ) o on oh.opportunity_id = o.id

where oh.is_deleted = false
 and o.institution_c='a0ko0000002BSH4AAO'
