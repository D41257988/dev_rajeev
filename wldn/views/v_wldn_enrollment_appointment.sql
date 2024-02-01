create or replace view rpt_crm_mart.v_wldn_enrollment_appointment as
select * ,case when msi_email_tsk.msi_email_date is null then 'No' else 'Yes' end  as msi_email
from(
 select *,row_number() over (partition by  appointment_number order by case when SLA = 'Met' then 1 end desc,new_task_created_date)  as rn
 from (
select Id,
appointment_number,
parent_record_id,
created_date,
status as appointment_status,
cancel_reason_c,
service_note,
Sla_start,
Sla_end,
new_task_created_date,
phone,
caller_number_c,
call_answered_time_c,
case when lower(status) ='canceled' or lower(status) ='cancelled' then 'N/A'
       when  new_task_created_date is not null and  (new_task_created_date not between Sla_start and Sla_end)  then 'Not Met'
       when  new_task_created_date is null  then 'Not Called'
        when ifnull(phone,'aa')!=ifnull(caller_number_c,'aa') and new_task_created_date between Sla_start and Sla_end then 'Wrong Number Called'
       when ifnull(phone,'aa')=ifnull(caller_number_c,'aa') and new_task_created_date between Sla_start and Sla_end then 'Met'
  End as SLA,
  case when call_answered_time_c is null then 'Yes' else 'No' end as Manual_call,
  task_id  ,
  ES,
  EM,
  Director,
  task_status as Task_Outcome ,
  billing_country ,
  scheduled_start_time ,
  cancellation_reason ,
  program_of_interest_c ,
  program_level_c,
  customer_friendly_poi_name_c,
   college_description_c,
   Stage,
   Disposition,
   appointment_type,
  case when self_scheduled is false then 'No' else 'Yes' end self_scheduled
  from
(select appt.id,
appt.appointment_number,
appt.appointment_type,
appt.parent_record_id,
appt.email,
datetime(appt.created_date ,"America/New_York") as created_date ,
appt.cancel_reason_c,
appt.cancellation_reason,
appt.status,
appt.service_note,
datetime(sched_start_time,"America/New_York") sched_start_time,
datetime(sched_end_time,"America/New_York") sched_end_time,
typ.block_time_before_appointment,
typ.block_time_after_appointment ,
opp.program_of_interest_c ,
P.program_level_c,
P.college_description_c  ,
datetime(appt.sched_start_time,"America/New_York") as scheduled_start_time ,
datetime(TIMESTAMP_SUB(sched_start_time, INTERVAL 5 MINUTE),"America/New_York") as Sla_start ,
datetime(TIMESTAMP_ADD(sched_start_time, INTERVAL 15 MINUTE),"America/New_York") as Sla_end ,
right( replace(replace(Replace(Replace(appt.phone,')',''),'(',''),'-',''),' ' ,'') , 10 ) as phone,
appt.email,
 datetime(tas.created_date ,"America/New_York")   as task_created_date,
tas.status_c as task_status,
datetime( tas.next_call_date_c  ,"America/New_York")   as next_call_date_c,
tas.id as task_id,
call_answered_time_c ,
call_duration_in_seconds ,
call_disposition ,
case when cast(datetime(appt.created_date ,"America/New_York") as date) > '2022-07-13' -- history from 8x8
then right ( replace(replace(Replace(Replace(called_number_c,')',''),'(',''),'-',''),' ' ,'')   , 10 )
else right ( replace(replace(Replace(Replace(caller_number_c,')',''),'(',''),'-',''),' ' ,'')   , 10 ) end as caller_number_c,
tas.what_id ,
datetime( timestamp_sub(tas.created_date, INTERVAL ifnull(tas.call_duration_in_seconds,0) SECOND),"America/New_York") as new_task_created_date,
usr.name as ES ,
m.name as EM ,
d.name as  director,
bp.billing_country_text_c billing_country,
opp.customer_friendly_poi_name_c,
p.customer_friendly_poi_name_c as customer_friendly_poi_name_c1,
appt.opportunity_stage_c Stage ,
appt.opportunity_disposition_c Disposition,
appt.self_scheduled_c as self_scheduled,
P.college_description_c as College
from raw_b2c_sfdc.service_appointment appt
  left join raw_b2c_sfdc.opportunity opp on appt.parent_record_id = opp.id
  left join raw_b2c_sfdc.brand_profile_c BP on opp.brand_profile_c = bp.id
  left join raw_b2c_sfdc.work_type typ on typ.id = appt.work_type_id
  left join raw_b2c_sfdc.task tas on tas.what_id = appt.parent_record_id
        and cast(tas.created_date as date) = cast(appt.sched_start_time as date) and lower(tas.type_c) ='phone call'
  left join raw_b2c_sfdc.assigned_resource asg_res on appt.id = asg_res.service_appointment_id
  left join raw_b2c_sfdc.service_resource ser_res on asg_res.service_resource_id =ser_res.id
  left join raw_b2c_sfdc.user usr on usr.id =ser_res.related_record_id
  left join raw_b2c_sfdc.user M on usr.manager_id = M.id
  left join raw_b2c_sfdc.user D on usr.director_c = D.id
  left join raw_b2c_sfdc.product_2 P  on opp.Program_of_Interest_c = p.id
where lower(typ.department_c) ='enrollment' and appt.is_deleted = false

) a
 )b ) c
left join (
select  what_id,cast(datetime(created_date,"America/New_York") as date) as msi_email_date  from `raw_b2c_sfdc.task`
where lower(subject) ='sales email sent'
and cast(created_date as date) >'2021-08-01'
group by what_id,cast(datetime(created_date,"America/New_York") as date)
)msi_email_tsk
on c.parent_record_id = msi_email_tsk.what_id
and cast(c.new_task_created_date  as date) = cast(msi_email_tsk.msi_email_date as date )
where rn=1
and msi_email_tsk.what_id in (select id from raw_b2c_sfdc.opportunity where is_deleted=false AND institution_c in ('a0ko0000002BSH4AAO')) -- rs - walden filter condition
