CREATE or REPLACE VIEW `rpt_crm_mart.v_wldn_loe_task_category` AS

select
id,
created_date,
created_by_id,
what_id,
case
  when lower(subject) like '%prospect re-inquiry%' or  lower(a.status_c) like '%scheduled%' then 0
  when (lower(a.type_c) = 'email' and lower(a.status_c) = 'received')
  or (lower(a.type_c) = 'chat' and lower(a.status_c) = 'received')
  or (lower(a.type_c) = 'skype call' and lower(a.status_c) = 'talked to')
  or (lower(a.type_c) = 'text' and lower(a.status_c) = 'received')
  or (lower(a.type_c) = 'whatsapp' and lower(a.status_c) = 'talked to') then 1
  when lower(a.type_c) = 'phone call' and lower(a.status_c) = 'talked to'  then 1
  when lower(a.type_c) = 'phone call' and lower(call_type)='internal'  then 0
  when lower(a.type_c) = 'phone call' and call_duration_in_seconds >= 90  and lower(a.status_c)='--none--' then 1
  when lower(a.type_c) = 'phone call' and call_duration_in_seconds >= 90  and lower(a.status_c)='completed' then 1
  when lower(a.type_c) = 'phone call' and call_duration_in_seconds >= 90  and lower(a.status_c)='none' then 1
  when lower(a.type_c) = 'phone call' and call_duration_in_seconds >= 90  and a.status_c is null then 1
  when lower(a.type_c) = 'phone call' and lower(a.status_c)='voicemail received' then 1
  when lower(a.type_c) = 'phone call' and lower(a.status_c)='received' and call_duration_in_seconds >20 then 1
  when lower(a.type_c) = 'phone call' and lower(a.status_c)='completed' and lower(call_type)='inbound' and call_duration_in_seconds >20 then 1 /*temp*/
  when a.type_c is null  and lower(a.status_c)='talked to' then 1
  else 0 end
as two_way,
  -- datediff(day,b.created_date,a.created_date) as days_oppcreate_to_task,
case
  --for chat, there are voice calls, two scenarios:
  when lower(a.type_c)='chat' and lower(subject) like '%voicecall%' and  lower(a.description) not like '%chat%' then 'phone'
  when lower(a.type_c)='chat'   then 'chat'
  --for phone call or skype call, or email, go with the category
  when lower(a.type_c)='phone call' then 'phone'
  when lower(a.type_c)='skype call' then 'phone'
  when lower(a.type_c)='email' then 'email'
  --for null type_c, different scenarios:
  when a.type_c is null and   lower(subject) like '%call%'  then 'phone'
  when a.type_c is null and   lower(subject) like '%phone with%'  then 'phone'
  when a.type_c is null and   lower(a.status_c) like '%voicemail%'  then 'phone'
  --for chat, there are voice calls, two scenarios:
  when lower(a.type_c)='chat' and lower(subject) like '%voicecall%' and  lower(a.description) not like '%chat%' then 'phone'
  when lower(a.type_c)='chat'   then 'chat'
  --for phone call or skype call, or email, go with the category
  when lower(a.type_c)='phone call' then 'phone'
  when lower(a.type_c)='skype call' then 'phone'
  when lower(a.type_c)='email' then 'email'
  --for null type_c, different scenarios:
  when a.type_c is null and   lower(subject) like '%call%'  then 'phone'
  when a.type_c is null and   lower(subject) like '%phone with%'  then 'phone'
  when a.type_c is null and   lower(a.status_c) like '%voicemail%'  then 'phone'
  when a.type_c is null and  lower(subject)='chat' then 'chat'
  when a.type_c is null and lower(subject)='email' then 'email'
  when a.type_c is null and   lower(subject) like '%email%'  then 'email'
  --for file review, different scenarios:
  when lower(a.type_c)='file review' and lower(call_type) ='outbound' and lower(subject) like '%call%'  then 'phone'
  when lower(a.type_c)='file review' and lower(call_type) ='inbound' and lower(subject) like '%call%'   then 'phone'
  else lower(a.type_c)
  end
as task_type
from
raw_b2c_sfdc.task a
where substr(what_id,0,3)='006'
and is_deleted=false
and cast(created_date as date) >= '2020-10-01'
AND what_id in (select id from raw_b2c_sfdc.opportunity
where is_deleted=false AND institution_c in ('a0ko0000002BSH4AAO'))