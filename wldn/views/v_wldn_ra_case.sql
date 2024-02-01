create or replace view rpt_crm_mart.v_wldn_ra_case as
SELECT sc.*
FROM `raw_b2c_sfdc.case` sc
inner JOIN `stg_l1_salesforce.record_type` srt
on sc.record_type_id = srt.id
AND srt.developer_name  like '%Recommended_Admit%'
where institution_brand_c = 'a0ko0000002BSH4AAO'
