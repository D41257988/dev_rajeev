create or replace view rpt_crm_mart.v_wldn_case_service as
SELECT c.*,u.name as collector,u.username,m.name as manager_name,u.alias,EXTRACT(YEAR from c.last_modified_date) as year,EXTRACT(MONTH from c.last_modified_date) as month
 FROM `raw_b2c_sfdc.case` c
left join `raw_b2c_sfdc.user` u on c.owner_id=u.id
left join `raw_b2c_sfdc.user` m on upper(u.manager_id) = upper(m.id)
left join (select distinct id, 	institution_code_c  from `raw_b2c_sfdc.contact` where is_deleted = false) co on c.contact_id = co.id
where
c.is_deleted=false
and (c.institution_brand_c in ('a0ko0000002BSH4AAO') OR c.institution_code_c = 3.0)
and record_type_id in
   (
    '012o00000012bKoAAI'
,'012o00000012ZrjAAE'
,'0121N0000019B0JQAU'
,'0121N000000uGPJQA2'
,'0121N000001AQG8QAO'
,'0121N000000qslUQAQ'
,'0121N000000qslTQAQ'
   )
