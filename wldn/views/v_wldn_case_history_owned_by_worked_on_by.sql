select
   ch.id,
   ch.is_deleted,
   ch.case_id,
   ch.created_by_id,
   ch.created_date,
   ch.field,
   ch.old_value,
   ch.new_value,
   coalesce (u_old_value.department_c, u_old_value.department, queue_old_value.departmentname) as old_value_dept,
   coalesce(u_new_value.department_c, u_new_value.department, queue_new_value.departmentname) as new_value_dept,
   ifnull(u_created_by.department_c, u_created_by.department) as created_by_dept,
   u_old_value.division as old_value_div,
   u_new_value.division as new_value_div,
   u_created_by.division as created_by_div
from
   `raw_b2c_sfdc.case_history`  ch
   left join `raw_b2c_sfdc.user`  u_old_value  on upper(ch.old_value) = upper(u_old_value.id)
   left join `rpt_crm_mart.v_wldn_queue` queue_old_value on upper(ch.old_value) = upper(queue_old_value.queuesfid)
   left join `raw_b2c_sfdc.user`  u_new_value on upper(ch.new_value) = upper(u_new_value.id)
   left join `rpt_crm_mart.v_wldn_queue` queue_new_value on upper(ch.new_value) = upper(queue_new_value.queuesfid)
   left join `raw_b2c_sfdc.user`  u_created_by on upper(ch.created_by_id) = upper(u_created_by.id)
   left join (select distinct id, opportunity_c, contact_id, institution_brand_c  from `raw_b2c_sfdc.case` where is_deleted = false) c on ch.case_id = c.id

where
   upper(ch.field) = 'OWNER'
   and extract(date from ch.created_date) > '2019-01-01'
   and old_value like '00%'
   and ch.is_deleted=false
   and c.institution_brand_c = 'a0ko0000002BSH4AAO';
