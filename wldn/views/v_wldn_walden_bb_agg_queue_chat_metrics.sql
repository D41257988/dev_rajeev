create or replace view rpt_crm_mart.v_wldn_walden_bb_agg_queue_chat_metrics as
select 'Blackboard FS' as content_area_1
	,'Blackboard FS' as content_area_2
	,'Blackboard FS' as queue_name
	,'chat' as type
    ,cast(created_date as date) as date
    ,null as offered
    ,sum(1) as accepted
    ,null as abandoned

from raw_b2c_sfdc.case c
  left join (select id, institution_c
              from raw_b2c_sfdc.opportunity
              where is_deleted = false
              ) o on c.opportunity_c = o.id

where
  lower(origin) = 'bb - chat'
  and c.is_deleted = false
  and o.institution_c='a0ko0000002BSH4AAO'
group by date
