
select a.*, ea.division,ea.location_c as location ,em.name as ea_manager_name,ea.name as ea_name ,ea.is_active
from
(
select ot.id as opp_tracking_sfid, null as task_sfid,null as task_type,
--null as task_attempted_sfid, null as task_attempted_nba_sfid,
ot.opportunity_c,datetime(cast(ot.created_date  as timestamp),"US/Eastern") created_date_time,
date(datetime(cast(ot.created_date  as timestamp),"US/Eastern")) created_date,
 'PRESENTED' as event,
einstein_best_contact_method_c,einstein_earliest_call_time_est_c, einstein_latest_call_time_est_c,
next_task_due_date_c,einstein_day_to_call_c, ot.clicked_by_c
,ot.recommendation_rejected_reason_c ,null as name

from raw_b2c_sfdc.opportunity_tracking_c ot
where lower(event_name_c) ='einstein recall list'
and lower(event_type_c) ='opportunity accessed'
--and substr(lower(ot.einstein_day_to_call_c),1,3) = lower(format_date('%a', date(ot.created_date)))  -- only look at opp where recommendation day was same as created date.
  and regexp_contains(lower(ot.einstein_day_to_call_c),lower(format_date('%a', date(ot.created_date)))) --- modified to accomdate change to the day to call multiple values like 'monday:thursday:friday:tuesday:wednesday'

union all

select ot_recmd.id as opp_tracking_sfid, null as task_sfid,null as task_type,
--null as task_attempted_sfid, null as task_attempted_nba_sfid,
ot_recmd.opportunity_c,datetime(cast(ot_recmd.created_date  as timestamp),"US/Eastern") created_date_time,
date(datetime(cast(ot_recmd.created_date  as timestamp),"US/Eastern")) created_date,
case when lower(ot_recmd.recommendation_action_c) = 'accepted' then 'ACCEPTED' when lower(ot_recmd.recommendation_action_c) = 'rejected' then 'REJECTED' end as event,
ot_recall.einstein_best_contact_method_c,ot_recall.einstein_earliest_call_time_est_c, ot_recall.einstein_latest_call_time_est_c,
ot_recall.next_task_due_date_c,ot_recall.einstein_day_to_call_c, ot_recall.clicked_by_c
,ot_recmd.recommendation_rejected_reason_c , null as name

from raw_b2c_sfdc.opportunity_tracking_c ot_recall
join (select * from raw_b2c_sfdc.opportunity_tracking_c
         where lower(event_name_c) = 'einstein recommendation'
         and lower(event_type_c) ='recommendation response'
         ) ot_recmd
on ot_recall.opportunity_c = ot_recmd.opportunity_c
and date(ot_recall.created_date) = date(ot_recmd.created_date)
and ot_recall.created_date < ot_recmd.created_date
where lower(ot_recall.event_name_c) ='einstein recall list'
and lower(ot_recall.event_type_c) ='opportunity accessed'
and regexp_contains(lower(ot_recall.einstein_day_to_call_c),lower(format_date('%a', date(ot_recall.created_date))))

union all

select ot.id as opptracking_id, ta.id as task_sfid, ta.task_type,
--ta.id as task_attempted_sfid, null as task_attempted_nba_sfid,
ot.opportunity_c,datetime(cast(ot.created_date  as timestamp),"US/Eastern") created_date_time,
date(datetime(cast(ot.created_date  as timestamp),"US/Eastern")) created_date, 'OR_ATTEMPTED' as event,
einstein_best_contact_method_c,einstein_earliest_call_time_est_c, einstein_latest_call_time_est_c, next_task_due_date_c,einstein_day_to_call_c,ot.clicked_by_c
  ,ot.recommendation_rejected_reason_c , null as name

from raw_b2c_sfdc.opportunity_tracking_c ot
join ( select id, created_date, created_by_id, what_id, two_way, task_type,
  row_number() over(partition by what_id, date(datetime(cast(ta.created_date  as timestamp),"US/Eastern"))
                    order by what_id, date(datetime(cast(ta.created_date  as timestamp),"US/Eastern"))) as rownum
  from rpt_crm_mart.v_wldn_loe_task_category ta
  where lower(ta.task_type) in ('text','email','chat','phone')
 --     and what_id ='0062g00000ercokqa2'
  ) ta
on ot.opportunity_c = ta.what_id
and date(ot.created_date) = date(ta.created_date)
and ot.created_date < ta.created_date --   ( task should be created that day after opp tracking created date )
and rownum = 1
and lower(ta.task_type) in ('text','email','chat','phone')
where lower(event_name_c) ='einstein recall list'
and lower(event_type_c) ='opportunity accessed'
--and substr(lower(ot.einstein_day_to_call_c),1,3) = lower(format_date('%a', date(ot.created_date))) -- only look at opp where recommendation day was same as created date.
  and regexp_contains(lower(ot.einstein_day_to_call_c),lower(format_date('%a', date(ot.created_date)))) --- modified to accomdate change to the day to call multiple values like 'monday:thursday:friday:tuesday:wednesday'

  union all

  select opptracking_id, task_sfid, task_type ,opportunity_c,created_date_time,created_date,event,einstein_best_contact_method_c,
einstein_earliest_call_time_est_c,einstein_latest_call_time_est_c,next_task_due_date_c,einstein_day_to_call_c,clicked_by_c
  ,recommendation_rejected_reason_c ,name
from(
select ot.id as opptracking_id,  ta_r.id as task_sfid, ta_r.task_type,
--null as task_attempted_sfid, ta_r.id as task_attempted_nba_sfid,
ot.opportunity_c,datetime(cast(ot.created_date  as timestamp),"US/Eastern") created_date_time,
date(datetime(cast(ot.created_date  as timestamp),"US/Eastern")) created_date,
'OR_ATTEMPTED_NBA' as event,
einstein_best_contact_method_c,einstein_earliest_call_time_est_c, einstein_latest_call_time_est_c, next_task_due_date_c,einstein_day_to_call_c,
ot.clicked_by_c,
row_number() over(partition by what_id, date(datetime(cast(ta_r.created_date  as timestamp),"US/Eastern"))
         order by what_id, date(datetime(cast(ot.created_date  as timestamp),"US/Eastern"))) as rownum
,ot.recommendation_rejected_reason_c , ot_rcmd.name

from raw_b2c_sfdc.opportunity_tracking_c ot
 join (select o.id,o.opportunity_c, ot_rcmd.name ,o.created_date ,o.recommendation_action_c
        from raw_b2c_sfdc.opportunity_tracking_c o
        join raw_b2c_sfdc.recommendation ot_rcmd
        on o.recommendation_c = ot_rcmd.id
       -- and to_date(o.created_date) = to_date(ot_rcmd.created_date)
       -- and o.created_date < ot_rcmd.created_date
        and lower(o.event_name_c) = 'einstein recommendation' and lower(o.event_type_c) ='recommendation response' ) ot_rcmd
 on ot_rcmd.opportunity_c = ot.opportunity_c
join rpt_crm_mart.v_wldn_loe_task_category ta_r
on ot.opportunity_c = ta_r.what_id
and date(ot.created_date) = date(ta_r.created_date)
and ot.created_date < ta_r.created_date ---- task created date is greater than opp tracking created date ( task should be created that day after opp tracking created date )
--and lower(format_date('%a', date(ta_r.created_date))) =substr(lower(ot.einstein_day_to_call_c),1,3)
and regexp_contains(lower(ot.einstein_day_to_call_c),lower(format_date('%a', date(ot.created_date)))) --- modified to accomdate change to the day to call multiple values like 'monday:thursday:friday:tuesday:wednesday'
and ta_r.task_type in ('text','email','chat','phone')
where lower(event_name_c) ='einstein recall list'
and lower(event_type_c) ='opportunity accessed'
and case when date(ot.created_date) < '2021-03-24'
	then case when lower(ot.einstein_best_contact_method_c) = 'phone'
		and lower(ta_r.task_type) = 'phone'
                and  extract(hour from datetime(cast(ot.created_date  as timestamp),"US/Eastern"))  between cast(left(einstein_earliest_call_time_est_c, strpos(einstein_earliest_call_time_est_c,':')-1) as int64)
                and cast(left(einstein_latest_call_time_est_c, strpos(einstein_latest_call_time_est_c,':')-1) as int64) then 1
        --	 when lower(ot.einstein_best_contact_method_c) = 'email' and ta_r.task_type = 'email' then 1
         	when  ifnull(lower(ot.einstein_best_contact_method_c), 'aaa') != 'phone' and (lower(ta_r.task_type) = 'phone' or lower(ta_r.task_type) = 'email') then 1 --any other method include null, match on task type =phone
         	else 0 end = 1
	else case when lower(ot_rcmd.name) like 'phone%' and lower(ta_r.task_type) = 'phone'
                and  extract(hour from datetime(cast(ot.created_date  as timestamp),"US/Eastern"))  between cast(left(einstein_earliest_call_time_est_c, strpos(einstein_earliest_call_time_est_c,':')-1) as int64)
                and cast(left(einstein_latest_call_time_est_c, strpos(einstein_latest_call_time_est_c,':')-1) as int64) then 1
        	when lower(ot_rcmd.name) like 'email%' and lower(ta_r.task_type) = 'email' then 1
         	when lower(ot_rcmd.name) like 'text%' and lower(ta_r.task_type) = 'text' then 1
			when lower(ot_rcmd.name) like 'chat%' and lower(ta_r.task_type) = 'chat' then 1
         	else 0 end = 1
	end
	--and substr(lower(ot.einstein_day_to_call_c),1,3) = lower(format_date('%a', date(ot.created_date))) -- only look at opp where recommendation day was same as created date.
	  and regexp_contains(lower(ot.einstein_day_to_call_c),lower(format_date('%a', date(ot.created_date)))) --- modified to accomdate change to the day to call multiple values like 'monday:thursday:friday:tuesday:wednesday'
) or_attempted_nba
  where or_attempted_nba.rownum = 1
) a
inner join rpt_crm_mart.v_wldn_loe_enrollment_advisors ea
    on lower(a.clicked_by_c) = lower(ea.id)
inner join raw_b2c_sfdc.user em
    on lower(em.id) = lower(ea.manager_id)