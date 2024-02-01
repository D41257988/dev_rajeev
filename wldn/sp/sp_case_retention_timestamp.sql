CREATE OR REPLACE PROCEDURE `trans_crm_mart.sp_case_retention_timestamp`(IN v_audit_key STRING, OUT result STRING)
begin


declare institution string default 'WLDN';
declare institution_id int64 default 5;
declare dml_mode string default 'delete-insert'; ----#DialyInsert/Incremental
declare target_dataset string default 'rpt_crm_mart';
declare target_tablename string default 't_case_retention_timestamp';
declare source_tablename string default 'case_retention_timestamp_temp';
declare load_source string default 'trans_crm_mart.sp_case_retention_timestamp';
declare additional_attributes ARRAY<struct<keyword string, value string>>;
declare last_refresh_time timestamp;
declare tgt_table_count int64;


/* common across */
declare job_start_dt timestamp default current_timestamp();
declare job_end_dt timestamp default current_timestamp();
declare job_completed_ind string default null;
declare job_type string default 'EDW';
declare load_method string default 'scheduled query';
declare out_sql string;


begin
SET additional_attributes= [("audit_load_key", v_audit_key),
              ("load_method",load_method),
              ("load_source",load_source),
              ("job_type", job_type)];
    /* end common across */

create or replace temp table case_timestamps as (
select case_id, new_value, min(created_date) as proposedstatustimestamp, max(created_date) as latesttimestamp
from(
select ch.case_id, ch.new_value, ch.created_date from `raw_b2c_sfdc.case_history` ch
 join `raw_b2c_sfdc.case` c on ch.case_id = c.id
  and c.record_type_id in ('012o00000012ZrkAAE')
where ch.field = 'Status'
and c.is_deleted = FALSE
and ch.is_deleted = FALSE
and ch.new_value in ('New','Escalated','Closed','In Progress','Open','Re-Open','Waiting for Response','Pending Customer Action'
                      ,'Canceled','Work Completed','Transferred')
and institution_brand_c = 'a0ko0000002BSH4AAO'
union distinct
select ch.case_id, ch.new_value, ch.created_date from `raw_b2c_sfdc.case_history` ch
 join `raw_b2c_sfdc.case` c on ch.case_id = c.id
  and c.record_type_id in ('012o00000012ZrkAAE')
where ch.field = 'Status'
and c.is_deleted = FALSE
and institution_brand_c = 'a0ko0000002BSH4AAO'
and ch.is_deleted = FALSE
and ch.new_value in ('New','Escalated','Closed','In Progress','Open','Re-Open','Waiting for Response','Pending Customer Action'
                      ,'Canceled','Work Completed','Transferred')
and ch.created_date = (select min(ch1.created_date) from `raw_b2c_sfdc.case_history` ch1
                          where ch1.case_id = ch.case_id and ch1.field = 'Status' and ch1.is_deleted = FALSE)
) group by case_id, new_value
);

create or replace temp table begincasestatus as (
  select case_id, created_date, ch.old_value, ch.new_value from `raw_b2c_sfdc.case_history` ch where field = 'Status'
   and created_date = (select min(created_date)
   from `raw_b2c_sfdc.case_history` ch1 where ch.case_id = ch1.case_id and ch1.is_deleted = FALSE)
   and ch.is_deleted = FALSE

);
CREATE or replace TEMP TABLE `case_retention_timestamp_temp` as
(
with src as(
select
c.id as casesfid, c.banner_id_c as bannerid, bp.id as brandprofilesfid, c.case_closed_reason_c as caseclosedreason
,c.case_number,c.closed_date as closeddt, c.contact_id as contactsfid, c.created_by_id as createdsfid,
coalesce(c.onyx_created_date_c,c.created_date) as createddt, c.description, c.first_name_c as firstname, c.last_name_c as lastname,
c.institution_c as institutionsfid, i.institution_code_c as institutioncd, c.institution_brand_c as institutionbrand,
c.institution_type_c as institutiontype,c.last_modified_by_id as lastmodifiedsfid, c.last_modified_date as lastmodifieddt,
c.new_duration_c as newduration, c.opportunity_c as opportunitysfid, c.origin, c.owner_id as ownersfid, c.priority,
c.product_c as product, c.reason, c.start_date_c as startdt, c.review_type_c as reviewtype, c.status, c.subject, c.subtype_c,
c.system_modstamp as systemmodstamp, c.type, c.type_action_c as typeaction, c.walden_banner_id_c as waldenbannerid,
c.original_record_type_c as originalrecordtype, c.parent_id as parentsfid, c.gsfs_action_c as gsfsaction,
c.gsfs_action_outcome_c as gsfsactionoutcome, c.channel_c as channel, c.sa_source_c as sasource,
c.code_of_conduct_type_c as codeofconducttype, c.code_of_conduct_subtype_c as codeofconductsubtype,

case when a.case_id is not null then a.latestnortimestamp
      when c.status in ('New','Open','Re-Open','Work Completed','Transferred') then c.created_date
      else null
      end as latestnortimestamp,

case when b.case_id is not null then b.proposedstatustimestamp
      when c.status = 'Closed' then c.created_date
      end as closedtimestamp, bcs.old_value as begin_case_status ,

case when c.status  in ('New' ,  'Open', 'Re-open', 'Work Completed', 'Transferred' ) then 1
      else 0
      end As isnorflag,

coalesce(case when (su.owner_department = 'Academic Advising') and su.International_c = true
                then 'Academic Advising-International'
                when (su.owner_department = 'Success Coaches') and su.International_c = true
                then 'Success Coaches-International'
                 else su.owner_department end, q.Departmentname) as ownerteam

 from `raw_b2c_sfdc.case` c
 left join (select case_id, max(latesttimestamp) as latestnortimestamp from case_timestamps
              where new_value in ( 'New','Open' , 'Re-Open', 'Work Completed', 'Transferred')
               group by case_id) a
  on c.id = a.case_id
 left join `raw_b2c_sfdc.brand_profile_c` bp on bp.contact_c = c.contact_id and bp.is_deleted = false
 left join  case_timestamps b on b.case_id = c.id and b.new_value = 'Closed'
 left join begincasestatus bcs
  on bcs.case_id = c.id
 left join `raw_b2c_sfdc.v_service_user` su on su.id = c.owner_id
 left join `rpt_crm_mart.v_wldn_queue` q on c.owner_id = q.queuesfid
 left join `raw_b2c_sfdc.institution_c` i on c.institution_c = i.id
 where c.record_type_id in ('012o00000012ZrkAAE')
 and c.is_deleted = FALSE
 and institution_brand_c = 'a0ko0000002BSH4AAO'
)
select
      src.*,
      5 as institution_id,
      'WLDN' as institution,
      'WLDN_BNR' as source_system_name,
      job_start_dt as etl_created_date,
      job_start_dt as etl_updated_date,
      load_source as etl_resource_name,
      v_audit_key as etl_ins_audit_key,
      v_audit_key as etl_upd_audit_key,
      farm_fingerprint(format('%T', (src.casesfid, src.bannerid))) AS etl_pk_hash,
      farm_fingerprint(format('%T', src )) as etl_chg_hash,
      FROM src
);
-- merge


  call utility.sp_process_elt (institution, dml_mode , target_dataset, target_tablename, null, source_tablename, additional_attributes, out_sql );

    set job_end_dt = current_timestamp();
    set job_completed_ind = 'Y';

    -- export success audit log record
    call `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);


    -- update audit refresh upon process successful completion
    --multiple driver tables need multiple inserts here, one for each driver table, In this case we only have sgbstdn
    --call `audit_cdw_log.sp_export_audit_table_refresh` (v_audit_key, 'general_student_stage',target_tablename,institution_id, job_start_dt, current_timestamp(), load_source );

 set result = 'SUCCESS';

EXCEPTION WHEN error THEN

SET job_end_dt = cast (NULL as TIMESTAMP);
SET job_completed_ind = 'N';

CALL `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);

-- insert into error_log table
insert into
`audit_cdw_log.error_log` (error_load_key, process_name, table_name, error_details, etl_create_date, etl_resource_name, etl_ins_audit_key)
values
(v_audit_key,'EDW_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;

set result = @@error.message;

END;

END;
