select a.*, ea.division,ea.location_c as location ,em.name as ea_manager_name,ea.name as ea_name ,ea.is_active , ed.name as ea_director_name
from
(
    --Presented
select ot.id as opp_tracking_sfid, null as task_sfid,null as task_type,
ot.opportunity_c,datetime(cast(ot.created_date  as timestamp),"US/Eastern") created_date_time,
date(datetime(cast(ot.created_date  as timestamp),"US/Eastern")) created_date,
 'PRESENTED' as event,next_task_due_date_c, ot.clicked_by_c
,ot.recommendation_rejected_reason_c ,recommendation_action_c, recommendation_text_c,rule_name_c, null as name

from raw_b2c_sfdc.opportunity_tracking_c ot
where
 lower(event_name_c) ='einstein recall list'
and lower(event_type_c) ='opportunity accessed'
and lower(disposition_c) not like 'uncontacted%'

union all

--Accept/Reject
select ot_recmd.id as opp_tracking_sfid, null as task_sfid,null as task_type,
ot_recmd.opportunity_c,datetime(cast(ot_recmd.created_date  as timestamp),"US/Eastern") created_date_time,
date(datetime(cast(ot_recmd.created_date  as timestamp),"US/Eastern")) created_date,
case when lower(ot_recmd.recommendation_action_c) = 'accepted' then 'ACCEPTED' when lower(ot_recmd.recommendation_action_c) = 'rejected' then 'REJECTED' end as event,
ot_recall.next_task_due_date_c , ot_recall.clicked_by_c
,ot_recmd.recommendation_rejected_reason_c ,ot_recmd.recommendation_action_c, ot_recmd.recommendation_text_c,ot_recmd.rule_name_c ,null as name

from raw_b2c_sfdc.opportunity_tracking_c ot_recall
join (select * from raw_b2c_sfdc.opportunity_tracking_c
         where lower(event_name_c) = 'business rules recommendation'
         and lower(event_type_c) ='business rules recommendation response'
         and lower(disposition_c) not like 'uncontacted%'
         ) ot_recmd
on ot_recall.opportunity_c = ot_recmd.opportunity_c
and date(ot_recall.created_date) = date(ot_recmd.created_date)
and ot_recall.created_date < ot_recmd.created_date
where lower(ot_recall.event_name_c) ='einstein recall list'
and lower(ot_recall.event_type_c) ='opportunity accessed'
and lower(ot_recall.disposition_c) not like 'uncontacted%'

union all

select ot.id as opptracking_id, ta.id as task_sfid, ta.type_c,
ot.opportunity_c,datetime(cast(ot.created_date  as timestamp),"US/Eastern") created_date_time,
date(datetime(cast(ot.created_date  as timestamp),"US/Eastern")) created_date,
'OR_ATTEMPTED' as event, next_task_due_date_c  ,ot.clicked_by_c
  ,ot.recommendation_rejected_reason_c , recommendation_action_c, recommendation_text_c,rule_name_c, null as name

from raw_b2c_sfdc.opportunity_tracking_c ot
join ( select id, created_date, created_by_id, what_id, type_c,
  row_number() over(partition by what_id, date(datetime(cast(ta.created_date  as timestamp),"US/Eastern"))
                    order by what_id, date(datetime(cast(ta.created_date  as timestamp),"US/Eastern"))) as rownum
  from raw_b2c_sfdc.task ta
  where lower(ta.type_c) in ('text','email','phone call')
and substr(what_id,0,3)='006'
 --     and what_id ='0062g00000ercokqa2'
  ) ta
on ot.opportunity_c = ta.what_id
and date(ot.created_date) = date(ta.created_date)
and ot.created_date < ta.created_date --   ( task should be created that day after opp tracking created date )
and rownum = 1
and lower(ta.type_c) in ('text','email','phone call')
where lower(event_name_c) ='einstein recall list'
and lower(event_type_c) ='opportunity accessed'
and lower(disposition_c) not like 'uncontacted%'

union all

select opptracking_id, task_sfid, task_type ,opportunity_c,created_date_time,created_date,event,next_task_due_date_c,clicked_by_c
  ,recommendation_rejected_reason_c ,recommendation_action_c, recommendation_text_c,rule_name_c,recommendation
from(
select ot.id as opptracking_id,  ta_r.id as task_sfid, ta_r.type_c as task_type,
ot.opportunity_c,datetime(cast(ot.created_date  as timestamp),"US/Eastern") created_date_time,
date(datetime(cast(ot.created_date  as timestamp),"US/Eastern")) created_date,
'OR_ATTEMPTED_NBA' as event,
einstein_best_contact_method_c,einstein_earliest_call_time_est_c, einstein_latest_call_time_est_c, next_task_due_date_c,einstein_day_to_call_c,
ot.clicked_by_c,
row_number() over(partition by what_id, date(datetime(cast(ta_r.created_date  as timestamp),"US/Eastern"))
         order by what_id, date(datetime(cast(ot.created_date  as timestamp),"US/Eastern"))) as rownum
,ot_rcmd.recommendation_rejected_reason_c , ot_rcmd.recommendation_action_c, ot_rcmd.recommendation_text_c,ot_rcmd.rule_name_c, ot_rcmd.recommendation

from raw_b2c_sfdc.opportunity_tracking_c ot
 join (select o.id,o.opportunity_c, ot_rcmd.name as recommendation ,o.created_date ,o.recommendation_action_c ,recommendation_rejected_reason_c
            ,recommendation_text_c ,ot_rcmd.rule_name_c
        from raw_b2c_sfdc.opportunity_tracking_c o
        join raw_b2c_sfdc.recommendation ot_rcmd
        on o.recommendation_c = ot_rcmd.id
        and lower(event_name_c) = 'business rules recommendation'
         and lower(event_type_c) ='business rules recommendation response'
         and lower(disposition_c) not like 'uncontacted%'
         and lower(description) like 'business rule%' ) ot_rcmd
 on ot_rcmd.opportunity_c = ot.opportunity_c
left join raw_b2c_sfdc.task ta_r
on ot_rcmd.opportunity_c = ta_r.what_id
and date(ot.created_date) = date(ta_r.created_date)
and ot.created_date < ta_r.created_date ---- task created date is greater than opp tracking created date ( task should be created that day after opp tracking created date )
and lower(ta_r.type_c) in ('text','email','phone call')
and substr(what_id,0,3)='006'
where lower(event_name_c) ='einstein recall list'
and lower(event_type_c) ='opportunity accessed'
and lower(disposition_c) not like 'uncontacted%'
--and ot.opportunity_c in  ( '0067c00000E0srhAAB' , '0067c00000DEWBlAAP' , '0062G00000hLxgBQAS') --= '0067c00000DEVyJAAX'
and case when lower(ot_rcmd.recommendation) like '%business rule  phone%' and lower(ta_r.type_c) = 'phone call' then 1
         when lower(ot_rcmd.recommendation) like 'business rule  email%' and lower(ta_r.type_c) = 'email'
              and case when strpos(ta_r.description,':') > 0
                        then lower(trim(replace(replace(substr(ta_r.description, strpos(ta_r.description,':') + 2, length(ta_r.description) - strpos(ta_r.description,':')),' ',''),'-','')))
                         = lower(trim(replace(replace(substr(ot_rcmd.recommendation_text_c, strpos( ot_rcmd.recommendation_text_c,':') + 2, length(ot_rcmd.recommendation_text_c) - strpos( ot_rcmd.recommendation_text_c,':')),' ',''),'-','')))
                    else null end then 1
         when lower(ot_rcmd.recommendation) like 'business rule  sms%' and lower(ta_r.type_c) = 'text'
              and (
                  case when strpos(ta_r.description,':') > 0
                        then case when lower(ta_r.description) like 'documents received' then 'documents received' else right(replace(lower(ta_r.description),' ',''),strpos(reverse(replace(ta_r.description,' ','')),'-')-1) end
                        = case when lower(ot_rcmd.recommendation_text_c) like 'documents received' then 'documents received' else right(replace(lower(ot_rcmd.recommendation_text_c),' ',''),strpos(reverse(replace(ot_rcmd.recommendation_text_c,' ','')),'-')-1) end
                  else null end  ) then 1
			else 0 end = 1

) or_attempted_nba
where or_attempted_nba.rownum = 1
) a
inner join rpt_crm_mart.v_wldn_loe_enrollment_advisors ea
    on lower(a.clicked_by_c) = lower(ea.id)
left join raw_b2c_sfdc.user em
    on lower(em.id) = lower(ea.manager_id)
left join raw_b2c_sfdc.user ed
    on lower(ed.id) = lower(ea.director_c)