CREATE OR REPLACE PROCEDURE rpt_performance_mgmt.sp_fact_call_metrics()
begin

    declare institution string default 'WLDN';
    declare institution_id int64 default 5;
    declare dml_mode string default 'scd1';
    declare target_dataset string default 'rpt_performance_mgmt';
    declare target_tablename string default 't_fact_call_metrics';
    declare source_tablename string default 'prc_fact_call_metrics';
    declare load_source string default 'rpt_performance_mgmt.sp_fact_call_metrics';
    declare additional_attributes ARRAY<struct<keyword string, value string>>;
    declare last_refresh_time timestamp;
    declare tgt_table_count int64;


    /* common across */
    declare job_start_dt timestamp default current_timestamp();
    declare job_end_dt timestamp default current_timestamp();
    declare job_completed_ind string default null;
    declare job_type string default '8X8_CXONE_CALL';
    declare load_method string default 'scheduled query';
    declare out_sql string;

  begin

    set additional_attributes= [("audit_load_key", v_audit_key),
              ("load_method",load_method),
              ("load_source",load_source),
              ("job_type", job_type)];
    /* end common across */

    /*****************Determine last refresh for incremental runs***********************************/

    set tgt_table_count = (select count(1) from rpt_performance_mgmt.t_fact_call_metrics);

    -- use 1900 dates when target table has 0 records or no last refresh data found
    -- for multiple driver tables, use the the table value that has the min timestamp.

    set last_refresh_time = --'2022-01-23 16:59:00.208000';
                            case
                            when (tgt_table_count = 0 or audit_cdw_log.f_get_last_table_refresh_time ('contacts_completed', target_tablename, 5) is null)
                              then '1900-01-01 00:00:00.000000'
                            else
                              audit_cdw_log.f_get_last_table_refresh_time ('contacts_completed', target_tablename, 5)
                            end;



    /**********************************************************************************************/

    create or replace temp table prc_fact_call_metrics
    (institution_id INT64 options(description= 'institution id'),
    institution string options(description= 'institution name'),
    agentid STRING options(description= '8x8 or cxone agent id'),
    user_sfid STRING options(description= 'sf user id'),
    date DATE options(description= 'date of the call'),
    volumeoutbound INT64 options(description= 'number of outbound calls'),
    volumeinbound INT64 options(description= 'number of inbound calls'),
    acdcallsinbound INT64 options(description= 'the amount of time in seconds the agent was with the contact for inbound calls'),
    inboundtalktime INT64 options(description= 'duration of inbound calls'),
    outboundtalktime INT64 options(description= 'duration of outbound calls'),
    ronavolume INT64 options(description= 'rona calls from 8x8 or refused calls from cxone'),
    site STRING options(description= 'location of agent from sf'),
    vertical STRING options(description= 'divistion of agent from sf'),
    department STRING options(description= 'department of agent from sf user object'),
    title STRING options(description= 'title of agent from sf user object'),
    daacallsinbound INT64 options(description= 'count of calls where interaction type =daa for inbound or skill_id in 10779921, 10779454'),
    daainboundtalktime INT64 options(description= 'process_time or duration of calls where interaction type =daa for inbound or skill_id in 10779921, 10779454'),
    queuecallsinbound INT64 options(description= 'number of queue or skill calls - queue_name or skill_name is not null for inbound'),
    queueinboundtalktime INT64 options(description= 'queue or skill duration - queue_name or skill_name is not null for inbound'),
    queuecallsoutbound INT64 options(description= 'number of queue or skill calls - queue_name or skill_name is not null for outbound'),
    queueoutboundtalktime INT64 options(description= 'queue or skill duration - queue_name or skill_name is not null for outbound'),
    totalcalltime INT64 options(description= 'total call time'),
    source_system_name string options(description= 'source system name'),
    etl_pk_hash INT64 options(description= 'field to store hash value of primary key for etl purpose'),
    etl_chg_hash INT64 options(description= 'field to store hash value of attributes for etl purpose'),
    etl_created_date timestamp options(description= 'bi audit columns'),
    etl_updated_date timestamp options(description= 'bi audit columns'),
    etl_resource_name string options(description= 'bi audit columns'),
    etl_ins_audit_key string options(description= 'bi audit columns'),
    etl_upd_audit_key string options(description= 'bi audit columns')
    )

  as

  with
  fact_call_metrics_temp as (
   SELECT
    CAST(institution_id AS INT64) AS institution_id,
    CAST(institution AS string) AS institution,
    CAST(agentid AS STRING) AS agentid,
    CAST(user_sfid AS STRING) AS user_sfid,
    CAST(date AS DATE) AS date,
    CAST(volumeoutbound AS INT64) AS volumeoutbound,
    CAST(volumeinbound AS INT64) AS volumeinbound,
    CAST(acdcallsinbound AS INT64) AS acdcallsinbound,
    CAST(inboundtalktime AS INT64) AS inboundtalktime,
    CAST(outboundtalktime AS INT64) AS outboundtalktime,
    CAST(ronavolume AS INT64) AS ronavolume,
    case when source_system_name = '8x8_sf' then site else ifnull(ea.location_c , ea.office_location_c) end AS site,
    case when source_system_name = '8x8_sf' then vertical else ea.division end AS vertical,
    CAST(v.department AS STRING) AS department,
    CAST(v.title AS STRING) AS title,
    CAST(daacallsinbound AS INT64) AS daacallsinbound,
    CAST(daainboundtalktime AS INT64) AS daainboundtalktime,
    CAST(queuecallsinbound AS INT64) AS queuecallsinbound,
    CAST(queueinboundtalktime AS INT64) AS queueinboundtalktime,
    CAST(queuecallsoutbound AS INT64) AS queuecallsoutbound,
    CAST(queueoutboundtalktime AS INT64) AS queueoutboundtalktime,
    CAST(totalcalltime AS INT64) AS totalcalltime,

    CAST(source_system_name AS string) AS source_system_name

  FROM
    `rpt_performance_mgmt.v_fact_call_metrics` v
    left join raw_b2c_sfdc.user ea
	  on upper(ea.id) = upper(v.user_sfid)
    where v.institution_id is not null
  ),

  src as (
   select distinct
   institution_id,
   institution,
    agentid,
    user_sfid,
    date,
    volumeoutbound,
    volumeinbound,
    acdcallsinbound,
    inboundtalktime,
    outboundtalktime,
    ronavolume,
    site,
    vertical,
    department,
    title,
    daacallsinbound,
    daainboundtalktime,
    queuecallsinbound,
    queueinboundtalktime,
    queuecallsoutbound,
    queueoutboundtalktime,
    totalcalltime,

    source_system_name

  FROM
    fact_call_metrics_temp
  )


    -- all etl fields should come here. they should not be part of src
    select  src.*,
            -- farm_fingerprint(format('%T', concat(agentid ,user_sfid,department ,title ,date,institution_id))) AS etl_pk_hash,
            farm_fingerprint(format('%T', array_to_string([cast(agentid as string), user_sfid, cast(date as string), cast(institution_id as string)], ''))) as etl_pk_hash,
            farm_fingerprint(format('%T', src )) as etl_chg_hash,
            job_start_dt as etl_created_date,
            job_start_dt as etl_updated_date,
            load_source as etl_resource_name,
            v_audit_key as etl_ins_audit_key,
            v_audit_key as etl_upd_audit_key,

    from src;


    -- merge process
    call utility.sp_process_elt (institution, dml_mode , target_dataset, target_tablename, null, source_tablename, additional_attributes, out_sql );

    set job_end_dt = current_timestamp();
    set job_completed_ind = 'Y';

    -- export success audit log record
    call `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);

    -- update audit refresh upon process successful completion
    --multiple driver tables need multiple inserts here, one for each driver table, In this case we only have sgbstdn
    call `audit_cdw_log.sp_export_audit_table_refresh` (v_audit_key, 'contacts_completed',target_tablename,institution_id, job_start_dt, current_timestamp(), load_source );


    set result = 'SUCCESS';

    EXCEPTION WHEN error THEN

    SET job_end_dt = cast (NULL as TIMESTAMP);
    SET job_completed_ind = 'N';

    CALL `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key, target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);


    -- insert into error_log table
    insert into
    `audit_cdw_log.error_log` (error_load_key, process_name, table_name, error_details, etl_create_date, etl_resource_name, etl_ins_audit_key)
    values
    (v_audit_key,'8x8_CXONE_CALL_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;


    set result = @@error.message;

  end;

end
