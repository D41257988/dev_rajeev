with chat
as(
select
   s.developer_name as queue_name,
lct.id as chat_sfid,
lct.email_id_c as requestor_email_id,
 datetime(cast(lct.request_time  as timestamp),"US/Eastern") as chat_request_time,
  datetime(cast(lct.start_time  as timestamp),"US/Eastern") as chat_accept_time,
   datetime(cast(lct.end_time  as timestamp),"US/Eastern") as chat_end_time,

lct.live_chat_deployment_id as chat_deployment_sfid,
lct.live_chat_button_id as chat_button_sfid,

lct.live_chat_visitor_id,
   datetime(cast(lct.created_date  as timestamp),"US/Eastern") as transcript_created_date,
lct.status as chat_status

from raw_b2c_sfdc.live_chat_transcript lct
 join raw_b2c_sfdc.skill s on   lct.skill_id = s.id
 left join (select * from raw_b2c_sfdc.case where is_deleted = false) c on lct.case_id = c.id

where lct.created_date >= '2020-01-01'
and institution_brand_c = 'a0ko0000002BSH4AAO'
and s.master_label in ('Walden CCT Advising' , 'Walden Student Support', 'Student Support-Financial Aid(SST- FA)')
)
select  case when lower(queue_name) in ('walden_cct_advising') then 'CCT Advising Support'
             when lower(queue_name) in ('student_support_financial_aid_sst_fa') then 'CCT Financial Services Support'
             when lower(queue_name) in ('walden_student_support') then 'CCT Technical Support'
             when lower(queue_name) in ('chat_cct_fe_nurs') then 'CCT Nurs FE Support'
         else 'Uncategorized'    end as content_area_1
       ,case when lower(queue_name) in ('walden_cct_advising') then 'CCT Advising Support'
             when lower(queue_name) in ('student_support_financial_aid_sst_fa') then 'CCT Financial Services Support'
             when lower(queue_name) in ('walden_student_support') then 'CCT Technical Support'
             when lower(queue_name) in ('chat_cct_fe_nurs') then 'CCT Nurs FE Support'
         else 'Uncategorized'    end as content_area_2
       ,queue_name
	   ,'chat' as type
       ,extract(date from cast(transcript_created_date as timestamp)) as date
       ,count(1) as chatsoffered
       ,sum(case when chat_accept_time is not null then 1 else 0 end) as chatsaccepted
       ,sum(case when lower(chat_status) = 'missed' then 1 else 0 end) as chatsabandoned

from chat
group by queue_name,extract(date from cast(transcript_created_date as timestamp))