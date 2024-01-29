CREATE or REPLACE VIEW `rpt_crm_mart.v_wldn_case_retention`
OPTIONS(
  description="case retention",
  labels=[("source", "salesforce"), ("institution", "wldn"), ("type", "case"), ("sub-type", "")]
)
AS
SELECT c.*,u.name as collector,u.username,m.name as manager_name,u.alias,EXTRACT(YEAR from c.last_modified_date) as year,EXTRACT(MONTH from c.last_modified_date) as month
 FROM `raw_b2c_sfdc.case` c
left join `raw_b2c_sfdc.user` u on c.owner_id=u.id
left join `raw_b2c_sfdc.user` m on upper(u.manager_id) = upper(m.id)
left join `raw_b2c_sfdc.opportunity` o on c.opportunity_c = o.id -- RS - new condition added to filter in Walden data

where
c.is_deleted = false
and c.record_type_id in ('012o00000012ZrkAAE')
and o.institution_c='a0ko0000002BSH4AAO'