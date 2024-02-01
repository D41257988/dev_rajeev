CREATE OR REPLACE PROCEDURE trans_crm_mart.sp_case_service_timestamp()
begin


declare institution string default 'WLDN';
declare institution_id int64 default 5;
declare dml_mode string default 'delete-insert';
declare target_dataset string default 'rpt_crm_mart';
declare target_tablename string default 't_case_service_timestamp';
declare source_tablename string default 'wldn_case_service_timestamp';
declare load_source string default 'trans_crm_mart.sp_case_service_timestamp';
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


  CREATE OR REPLACE TEMP TABLE case_timestamps AS (
  SELECT case_id,new_value,MIN(created_date) AS proposedstatustimestamp, MAX(created_date) AS latesttimestamp
  FROM (
    SELECT ch.case_id,ch.new_value,ch.created_date
    FROM `raw_b2c_sfdc.case_history` ch
    JOIN `raw_b2c_sfdc.case` c
    ON ch.case_id = c.id
    AND c.record_type_id IN ('012o00000012bKoAAI','012o00000012ZrjAAE','0121N0000019B0JQAU','0121N000000uGPJQA2','0121N000001AQG8QAO','0121N000000qslUQAQ','0121N000000qslTQAQ')
    WHERE ch.is_deleted = false
    AND c.is_deleted = false
    and ch.field = 'Status'
    AND ch.new_value IN ('New','Escalated','Closed','In Progress','Open','Re-Open','Waiting for Response','Pending Customer Action','Canceled','Work Completed','Transferred')

    UNION DISTINCT

    SELECT ch.case_id,ch.new_value,ch.created_date
    FROM `raw_b2c_sfdc.case_history` ch
    JOIN `raw_b2c_sfdc.case` c
    ON ch.case_id = c.id
    AND c.record_type_id IN ('012o00000012bKoAAI','012o00000012ZrjAAE','0121N0000019B0JQAU','0121N000000uGPJQA2','0121N000001AQG8QAO','0121N000000qslUQAQ','0121N000000qslTQAQ')
    WHERE ch.is_deleted = false
    and c.is_deleted = false
    AND ch.field = 'Status'
    AND ch.new_value IN ('New','Escalated','Closed','In Progress','Open','Re-Open','Waiting for Response','Pending Customer Action','Canceled','Work Completed','Transferred')
    AND ch.created_date = (
      SELECT MIN(ch1.created_date)
      FROM `raw_b2c_sfdc.case_history` ch1
      WHERE ch1.case_id = ch.case_id
      AND ch1.field = 'Status'
      AND ch1.is_deleted = false)
    )
  GROUP BY case_id,new_value
  );


  CREATE OR REPLACE TEMP TABLE begincasestatus AS
  (
  SELECT case_id,created_date,ch.old_value,ch.new_value
  FROM `raw_b2c_sfdc.case_history` ch
  WHERE field = 'Status'
  AND ch.is_deleted = false
  AND created_date = (
    SELECT MIN(created_date)
    FROM `raw_b2c_sfdc.case_history` ch1
    WHERE ch.case_id = ch1.case_id
    and ch1.is_deleted = false)
  );


CREATE or replace TEMP TABLE `wldn_case_service_timestamp` as
(
with src as(
  SELECT
    c.id AS casesfid,
    c.banner_id_c AS bannerid,
    bp.id AS brandprofilesfid,
    c.case_closed_reason_c AS caseclosedreason,
    c.case_number,
    c.closed_date AS closeddt,
    c.contact_id AS contactsfid,
    c.created_by_id AS createdsfid,
    COALESCE(c.onyx_created_date_c,c.created_date) AS createddt,
    c.description,
    c.first_name_c AS firstname,
    c.last_name_c AS lastname,
    c.institution_c AS institutionsfid,
    i.institution_code_c AS institutioncd,
    c.institution_brand_c AS institutionbrand,
    c.institution_type_c AS institutiontype,
    c.last_modified_by_id AS lastmodifiedsfid,
    c.last_modified_date AS lastmodifieddt,
    c.new_duration_c AS newduration,
    c.opportunity_c AS opportunitysfid,
    c.origin,
    c.owner_id AS ownersfid,
    c.priority,
    c.product_c AS product,
    c.reason,
    c.start_date_c AS startdt,
    c.review_type_c AS reviewtype,
    c.status,
    c.subject,
    c.subtype_c,
    c.system_modstamp AS systemmodstamp,
    c.type,
    c.type_action_c AS typeaction,
    c.walden_banner_id_c AS waldenbannerid,
    c.original_record_type_c AS originalrecordtype,
    c.parent_id AS parentsfid,
    c.gsfs_action_c AS gsfsaction,
    c.gsfs_action_outcome_c AS gsfsactionoutcome,
    c.channel_c AS channel,
    c.sa_source_c AS sasource,
    c.code_of_conduct_type_c AS codeofconducttype,
    c.code_of_conduct_subtype_c AS codeofconductsubtype,

  CASE WHEN a.case_id IS NOT NULL THEN a.latestnortimestamp
    WHEN c.status IN ('New','Open','Re-Open','Work Completed','Transferred') THEN c.created_date
    ELSE CAST(NULL AS TIMESTAMP)
  END AS latestnortimestamp,

  CASE WHEN b.case_id IS NOT NULL THEN b.proposedstatustimestamp
    WHEN c.status = 'Closed' THEN c.created_date
  END AS closedtimestamp,

  bcs.old_value AS begin_case_status,

  CASE WHEN c.status IN ('New', 'Open', 'Re-open', 'Work Completed', 'Transferred' ) THEN 1
    ELSE 0
  END AS isnorflag,

  COALESCE(
    CASE WHEN (su.owner_department = 'Academic Advising' ) AND su.international_c = TRUE THEN 'Academic Advising-International'
        WHEN (su.owner_department = 'Success Coaches') AND su.international_c = TRUE THEN 'Success Coaches-International'
        ELSE su.owner_department
        END , q.departmentname) AS ownerteam

FROM raw_b2c_sfdc.case c
LEFT JOIN (
  SELECT case_id, MAX(latesttimestamp) AS latestnortimestamp
  FROM case_timestamps
  WHERE new_value IN ( 'New','Open','Re-Open','Work Completed','Transferred')
  GROUP BY case_id) a
  ON c.id = a.case_id
  LEFT JOIN `raw_b2c_sfdc.brand_profile_c` bp
  ON bp.contact_c = c.contact_id AND bp.is_deleted = FALSE
  LEFT JOIN case_timestamps b
  ON b.case_id = c.id AND b.new_value = 'Closed'
  LEFT JOIN begincasestatus bcs
  ON bcs.case_id = c.id
  LEFT JOIN rpt_crm_mart.v_wldn_service_user su -- rs - This view is changed as part of consolidation
  ON su.id = c.owner_id
  LEFT JOIN `rpt_crm_mart.v_wldn_queue` q
  ON c.owner_id = q.queuesfid
  LEFT JOIN `raw_b2c_sfdc.institution_c` i
  ON c.institution_c = i.id
  LEFT JOIN (select * from `raw_b2c_sfdc.contact` where is_deleted = false) co on c.contact_id = co.id -- rs - added for the filter

  WHERE c.record_type_id IN ('012o00000012bKoAAI','012o00000012ZrjAAE','0121N0000019B0JQAU','0121N000000uGPJQA2','0121N000001AQG8QAO','0121N000000qslUQAQ','0121N000000qslTQAQ')
    and c.is_deleted = false
    and lower(co.institution_code_c)='walden'
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
