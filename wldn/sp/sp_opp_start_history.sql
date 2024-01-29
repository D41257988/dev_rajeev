begin

    declare institution string default 'WLDN';
    declare institution_id int64 default 5;
    declare dml_mode string default 'delete-insert';
    declare target_dataset string default 'rpt_crm_mart';
    declare target_tablename string default 't_opp_start_history';
    declare source_tablename string default 'temp_opp_start_history';
    declare load_source string default 'trans_crm_mart.sp_opp_start_history';
    declare additional_attributes ARRAY<struct<keyword string, value string>>;
    declare last_refresh_time timestamp;
    declare tgt_table_count int64;

    /* common across */
    declare job_start_dt timestamp default current_timestamp();
    declare job_end_dt timestamp default current_timestamp();
    declare job_completed_ind string default null;
    declare job_type string default 'DS';
    declare load_method string default 'scheduled query';
    declare out_sql string;

	BEGIN
    SET additional_attributes= [("audit_load_key", v_audit_key),
              ("load_method",load_method),
              ("load_source",load_source),
              ("job_type", job_type)];
    /* end common across */

CREATE
OR REPLACE temp TABLE history_table AS (
with old_history as (
  select id,CreatedById as Created_By_Id,utility.udf_convert_EST_to_UTC(cast(SUBSTRING(CreatedDate,1,26) as timestamp)) as created_date, Field, NewValue as New_Value,OldValue as Old_Value, OpportunityId as Opportunity_Id from `raw_b2c_sfdc.opportunity_field_history_edw`
  where field in ('Selected_Program_Start_Date__c') and (NewValue like 'a1%' or OldValue like 'a1%')
  and utility.udf_convert_EST_to_UTC(cast(SUBSTRING(CreatedDate,1,26) as timestamp)) >= '2017-01-01 00:00:00.000000'),
new_history as (
  select oh.id,oh.created_by_id,oh.created_date,field,new_value,old_value,opportunity_id from `raw_b2c_sfdc.opportunity_field_history` oh left join raw_b2c_sfdc.opportunity o on oh.opportunity_id = o.id
  where
    field in ('Selected_Program_Start_Date__c')
    and data_type in ('EntityId')
    and o.institution_c = 'a0ko0000002BSH4AAO'
    and o.is_deleted = false

)
select * from old_history
union distinct
select * from new_history
);

CREATE OR REPLACE TEMP TABLE `temp_opp_start_history` AS
with new_table as (
  (
    with first_table as (
      select
        o.selected_program_start_date_c,
        o.created_date as created_date_,
        o.institution_c,
        h.*
      from
        `raw_b2c_sfdc.opportunity` o
        left join history_table h on o.id = h.opportunity_id
      where
        cast(o.created_date as date) > '2017-01-01'
		and o.is_deleted=false
        and o.institution_c='a0ko0000002BSH4AAO'
    ),
    grouped_table1 as (
      select
        *
      from
        first_table pivot (
          max(old_value) for field in ('Selected_Program_Start_Date__c')
        ) as PivotTable
      where
        (
          Selected_Program_Start_Date__c is not null
        )
    ),
    final_table6 as (
      select
        row_number() over (
          order by
            opportunity_id,
            created_date
        ) as RowNum,
        *
      from
        grouped_table1
    )
    select
      string(null) as id,
      opportunity_id,
      created_date_ as created_date,
      string(null) as created_by_id,
      Selected_Program_Start_Date__c as selected_program_start_date_id,
      institution_c
    from
      final_table6 t
    where
      RowNum = (
        select
          min(RowNum)
        from
          final_table6
        where
          t.opportunity_id = final_table6.opportunity_id
      )
  )
  union all
  select
    id,
    opportunity_id,
    created_date,
    created_by_id,
    Selected_Program_Start_Date__c as selected_program_start_date_id,
    institution_c
  from
    (
      select
        h.*,
        o.institution_c
      from
        history_table h
        left join `raw_b2c_sfdc.opportunity` o on h.opportunity_id = o.id
		where field in('Selected_Program_Start_Date__c') and o.institution_c='a0ko0000002BSH4AAO'
    ) pivot (
      max(new_value) for field in ('Selected_Program_Start_Date__c')
    ) as PivotTable
  where
     cast(created_date as date) > '2017-01-01'
),
new_table_1 as (
  select
    row_number() over (
      order by
        opportunity_id,
        created_date
    ) as RowNum,
    *
  from
    new_table
),
final_table4 as (
  select
    id,
    created_date,
    string(null) as created_by_id,
    selected_program_start_date_c,
    institution_c
  from
    `raw_b2c_sfdc.opportunity`
  where
    id not in (
      select
        opportunity_id
      from
        new_table_1
    )
    and cast(created_date as date) > '2017-01-01'
	and is_deleted=false
),
final_table5 as (
  (
    select
      opportunity_id as opp_sfid,
      id as opp_hist_sfid,
      created_date,
      created_by_id,
      selected_program_start_date_id,
      institution_c
    from
      new_table_1
  )
  union all
    (
      select
        id as opp_sfid,
        string(null) as opp_hist_sfid,
        created_date,
        created_by_id,
        selected_program_start_date_c as selected_program_start_date_id,
        institution_c
      from
        final_table4
    )
),
src as (
  select
    distinct case when l.institution_c = 'a0ko0000002BSH4AAO' THEN 'WLDN' ELSE 'Unknown' END AS institution,
    case when l.institution_c = 'a0ko0000002BSH4AAO' THEN 5 ELSE -1 END AS institution_id,
    'WLDN_SF' as source_system_name,
    opp_sfid,
    opp_hist_sfid,
    l.created_date,
    utility.udf_convert_UTC_to_EST(l.created_date) as created_date_est,
    l.created_by_id,
    l.selected_program_start_date_id,
	s.start_date_c as start_date,
    s.program_c as program_of_interest_id,
    r.name as program_of_interest_name

  from
    final_table5 l
    left join `raw_b2c_sfdc.start_date_c` s on l.selected_program_start_date_id = s.id and s.is_deleted=false
    left join `raw_b2c_sfdc.product_2` r on s.program_c = r.id and r.is_deleted=false
	where l.institution_c = 'a0ko0000002BSH4AAO' and r.institution_c='a0ko0000002BSH4AAO'
  order by
    opp_sfid,
    created_date
)

SELECT
    src.*,
    job_start_dt as etl_created_date,
    job_start_dt as etl_updated_date,
    load_source as etl_resource_name,
    v_audit_key as etl_ins_audit_key,
    v_audit_key as etl_upd_audit_key,
    farm_fingerprint(format('%T', concat(src.opp_sfid))) AS etl_pk_hash,
    farm_fingerprint(format('%T', src )) as etl_chg_hash,
    FROM src;

-- merge process
CALL utility.sp_process_elt (institution, dml_mode , target_dataset, target_tablename, null, source_tablename, additional_attributes, out_sql );
SET job_end_dt = current_timestamp();
SET job_completed_ind = 'Y';
-- export success audit log record
CALL `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);
SET result = 'SUCCESS';
EXCEPTION WHEN error THEN
SET job_end_dt = cast (NULL as TIMESTAMP);
SET job_completed_ind = 'N';
CALL `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);
-- insert into error_log table
INSERT INTO
`audit_cdw_log.error_log` (error_load_key, process_name, table_name, error_details, etl_create_date, etl_resource_name, etl_ins_audit_key)
VALUES
(v_audit_key,'DS_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;
SET result =  @@error.message;
raise using message = @@error.message;
END;
END