CREATE OR REPLACE PROCEDURE `daas-cdw-dev.trans_crm_mart.sp_opp_status_history`(IN v_audit_key STRING, OUT result STRING)
begin

    declare institution string default 'WLDN';
    declare institution_id int64 default 5;
    declare dml_mode string default 'delete-insert';
    declare target_dataset string default 'rpt_crm_mart';
    declare target_tablename string default 't_opp_status_history';
    declare source_tablename string default 'temp_opp_status_history';
    declare load_source string default 'trans_crm_mart.sp_opp_status_history';
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

CREATE
OR REPLACE temp TABLE history_table AS (

with old_history as (
  select id,CreatedById as Created_By_Id,utility.udf_convert_EST_to_UTC(cast(SUBSTRING(CreatedDate,1,26) as timestamp)) as created_date, Field, NewValue as New_Value,OldValue as Old_Value, OpportunityId as Opportunity_Id from `raw_b2c_sfdc.opportunity_field_history_edw`
  where field in ('Disposition__c','StageName')
  and utility.udf_convert_EST_to_UTC(cast(SUBSTRING(CreatedDate,1,26) as timestamp)) >= '2017-01-01 00:00:00.000000'),

new_history as (
  select oh.id,created_by_id,created_date,field,new_value,old_value,opportunity_id from `raw_b2c_sfdc.opportunity_field_history` oh
  left join (select id,  institution_c from `raw_b2c_sfdc.opportunity` where is_deleted = false ) o on oh.opportunity_id = o.id

  where oh.is_deleted = false
    and o.institution_c='a0ko0000002BSH4AAO'
    and field in ('Disposition__c','StageName')

)
select * from old_history
union distinct
select * from new_history
);
CREATE
OR REPLACE temp TABLE old_temp_table AS

    with first_table_temp as (
      select
        o.created_date as created_date_,
        h.opportunity_id,
        h.id,
        h.old_value,
        h.field,
        h.created_date,
        o.institution_c
      from
        `raw_b2c_sfdc.opportunity` o
        inner join history_table h on o.id = h.opportunity_id
      where
        cast(o.created_date as date) > '2017-01-01'
		and o.is_deleted=false
    and institution_c = 'a0ko0000002BSH4AAO'
    ),
    first_table as (
      select * from first_table_temp
      order by
        opportunity_id,
        created_date,
		    id desc
    ),
    grouped_table1 as (
      select
        *
      from
        first_table pivot (
          max(old_value) for field in ('Disposition__c')
        ) as PivotTable
      where
        (Disposition__c is not null)
    ),
    grouped_table2 as (
      select
        *
      from
        first_table pivot (
          max(old_value) for field in ('StageName')
        ) as PivotTable
      where
        (StageName is not null)
    ),
    grouped_table3 as(
      select
        l.opportunity_id as opp_sfid,
        string(null) as opp_hist_sfid,
        l.created_date_,
        r.created_date as created_date_r,
        l.created_date as created_date_l,
        string(null) as created_by_id,
        r.StageName as Stage_name,
        l.Disposition__c as Disposition,
        l.institution_c,
      from
        grouped_table1 l
        left join grouped_table2 r on l.opportunity_id = r.opportunity_id
    ),
    final_table6 as (
      select
        row_number() over (
          order by
            opp_sfid,
            created_date_l,
            created_date_r
        ) as RowNum,
        *
      from
        grouped_table3
    )
    select
      opp_sfid,
      opp_hist_sfid,
      created_date_ as created_date,
      created_by_id,
      Stage_name,
      Disposition,
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
          t.opp_sfid = final_table6.opp_sfid
      );
create or replace temp table new_temp_table as
 ( select
    opportunity_id as opp_sfid,
    id as opp_hist_sfid,
    created_date,
    created_by_id,
    StageName as Stage_name,
    Disposition__c as Disposition,
    institution_c
  from
    (
    select
    h.*,
    o.institution_c
    from history_table h
    inner join `raw_b2c_sfdc.opportunity` o on h.opportunity_id = o.id and institution_c = 'a0ko0000002BSH4AAO' and o.is_deleted=false)
     pivot (
      max(new_value) for field in ('Disposition__c', 'StageName')
    ) as PivotTable
  where
    (
      StageName is not null
      or Disposition__c is not null
    )
    and cast(created_date as date) > '2017-01-01'

  union all
  select
      opp_sfid,
      opp_hist_sfid,
      created_date,
      created_by_id,
      Stage_name,
      Disposition,
      institution_c
    from
      old_temp_table

);
create or replace temp table new_table_1 as
(
  select
    row_number() over (
      order by
        opp_sfid,
        created_date,
		opp_hist_sfid
    ) as RowNum,
    *
  from
    new_temp_table
);
create or replace temp table grouped_table1 as
 (
  select
    RowNum,opp_sfid,Disposition,coalesce(opp_hist_sfid,'NULL') as opp_hist_sfid,
    count(Disposition) over (
      order by
        RowNum
    ) as Disposition_temp
  from
    new_table_1
);
create or replace temp table final_table1 as(
  select
    *,
    first_value(Disposition) over (
      partition by opp_sfid,Disposition_temp
      order by
        RowNum
    ) as Disposition__c
  from
    grouped_table1
);

create or replace temp table grouped_table2 as (
  select
    RowNum,opp_sfid,Stage_name,coalesce(opp_hist_sfid,'NULL') as opp_hist_sfid,
    count(Stage_name) over (
      order by
        RowNum
    ) as Stagename_temp
  from
    new_table_1
);
create or replace temp table final_table2 as (
  select
    *,
    first_value(Stage_name) over (
      partition by opp_sfid, Stagename_temp
      order by
        RowNum
    ) as StageName
  from
    grouped_table2
);
create or replace temp table base_table as(
  select
    opp_sfid,coalesce(opp_hist_sfid,'NULL') as opp_hist_sfid,created_date,created_by_id,institution_c
  from
    new_table_1 t
  where
    RowNum = (
      select
        max(RowNum)
      from
        new_table_1
      where
        (
          t.created_date = new_table_1.created_date
          and t.opp_sfid = new_table_1.opp_sfid
        )
    )
);
create or replace temp table final_table3 as(
select b.opp_sfid, b.opp_hist_sfid, b.created_date,b.created_by_id, d.Disposition__c, s.StageName,b.institution_c
from base_table b
left join final_table1 d on b.opp_hist_sfid=d.opp_hist_sfid and b.opp_sfid=d.opp_sfid
left join final_table2 s on s.opp_hist_sfid=d.opp_hist_sfid and s.opp_sfid=d.opp_sfid
);
create or replace temp table final_table4 as (
  select
    id,
    string(null) as opp_hist_sfid,
    created_date,
    string(null) as created_by_id,
    stage_name,
    disposition_c,
    institution_c
  from
    `raw_b2c_sfdc.opportunity`
  where
    id not in (
      select
        opp_sfid
      from
        final_table3
    )
    and cast(created_date as date) > '2017-01-01'
	and is_deleted=false
  and institution_c = 'a0ko0000002BSH4AAO'
);
create or replace temp table `temp_opp_status_history` AS
with
final_table5 as (
  (
    select
      opp_sfid,
      opp_hist_sfid,
      created_date,
      created_by_id,
      StageName,
      Disposition__c,
      institution_c
    from
      final_table3
  )
  /*
  union all
  (
    select
      opp_sfid,
      opp_hist_sfid,
      created_date,
      created_by_id,
      Stage_name,
      Disposition,
      institution_c
    from
      old_temp_table
  ) */
  union all
    (
      select
        id as opp_sfid,
        opp_hist_sfid,
        created_date,
        created_by_id,
        stage_name as StageName,
        disposition_c as Disposition__c,
        institution_c
      from
        final_table4
    )
),
temp_src as (
select distinct
    case when l.institution_c = 'a0ko0000002BSH4AAO' THEN 'WLDN' ELSE 'Unknown' END AS institution,
    case when l.institution_c = 'a0ko0000002BSH4AAO' THEN 5 ELSE -1 END AS institution_id,
    'WLDN_SF' as source_system_name,
    l.opp_sfid,
    case when l.opp_hist_sfid= 'NULL' then Null else l.opp_hist_sfid end as opp_hist_sfid,
    l.created_date,
    utility.udf_convert_UTC_to_EST(l.created_date) as created_date_est,
    l.created_by_id,
    coalesce(l.StageName,r.stage_name) as stage_name,
    coalesce(l.Disposition__c,r.disposition_c) as disposition,
    concat(coalesce(l.StageName,r.stage_name), '-', coalesce(l.Disposition__c,r.disposition_c)) as stage_disposition,

  from
    final_table5 l
    left join `raw_b2c_sfdc.opportunity` r on l.opp_sfid=r.id
  where l.institution_c = 'a0ko0000002BSH4AAO'
),
src as (
  select *,
    extract(DATE from created_date_est) as status_date,
    (extract(HOUR from created_date_est)*3600) + (extract(MINUTE from created_date_est)*60) + extract(SECOND from created_date_est) as status_time
  from
    temp_src

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
END;