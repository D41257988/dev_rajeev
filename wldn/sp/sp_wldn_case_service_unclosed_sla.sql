CREATE OR REPLACE PROCEDURE `trans_crm_mart.sp_wldn_case_service_unclosed_sla`()
begin

declare v_startdate date default date_sub(current_date(), INTERVAL 200 day);
declare v_enddate date default current_date();
declare v_weekbegindate date default current_date();
declare v_audit_id string default upper(replace(GENERATE_UUID(), "-", ""));

declare target_tablename string default 't_wldn_case_sla';
declare load_source string default 'trans_crm_mart.sp_wldn_case_service_unclosed_sla';
declare last_refresh_time timestamp;
declare tgt_table_count int64;
declare job_start_dt timestamp default current_timestamp();
declare job_end_dt timestamp default current_timestamp();
declare job_completed_ind string default NULL;
declare job_type string default 'CASE-SERVICE-UNCLOSED-SLA';
declare load_method string default 'scheduled query';

set v_weekbegindate = (
                  select
                    max(cal_dt) as start_date
                  from
                    `mdm.dim_date`
                  where
                    cal_dt < current_date() and cal_wk_day_nm = 'SATURDAY'
                  );

set v_enddate = v_weekbegindate;

-- set v_startdate = '2023-03-25';
-- set v_enddate = '2023-04-01';

select v_startdate, v_enddate;

begin transaction;

delete from `rpt_crm_mart.t_wldn_case_sla` where week_begin = cast(replace(cast(v_weekbegindate as string), '-', '') as int64) and case_type_category = 'SC-UC';

insert into `rpt_crm_mart.t_wldn_case_sla`
(institution_id, institution, week_begin, case_id, type, subtype, type_action, record_type, original_record_type, case_number, origin, case_closed_reason, case_status, parent_id, start_status, end_status, owner_id, owner_name, owner_manager, department, actual_sla_start, actual_sla_end, actual_response_duration, slahrs, default_sla_hrs, rpt_sla_start_date, rpt_sla_end_date, rpt_response_duration, planned_sla_end_date, in_sla, gsfs_action, gsfs_action_outcome, banner_id, sla_out_owner, last_modified_by_user, last_modified_by_dept, last_modified_by_manager, actual_sla_end_nor_flag, case_type, case_type_category, etl_created_date, etl_updated_date, etl_resource_name, etl_ins_audit_key, etl_upd_audit_key)

with unique_case as (

  select distinct
      C.id,
      C.type,
      C.subtype_c,
      C.type_action_c as type_action,
      C.banner_id_c as banner_id,
      C.record_type_id,
      C.original_record_type_c as original_record_type,
      C.case_number,
      C.origin,
      -- C.reason,
      C.case_closed_reason_c as case_closed_reason,
      C.status,
      C.parent_id,
      C.owner_id as owner_id_case,
      -- C.created_date as case_created_date
      cast(`utility.udf_convert_UTC_to_EST`(C.created_date) as timestamp) as case_created_date,
      C.gsfs_action_c as gsfs_action,
      C.gsfs_action_outcome_c as gsfs_action_outcome
  from
    `raw_b2c_sfdc.case` C
    -- `v_wldn_queue.v_case_service` C

  inner join `raw_b2c_sfdc.case_history`CH on C.id = CH.case_id
  where
    -- date(CH.created_date) >= v_startdate and date(CH.created_date) < v_enddate
    cast(`utility.udf_convert_UTC_to_EST`(CH.created_date) as date) >= v_startdate and cast(`utility.udf_convert_UTC_to_EST`(CH.created_date) as date) < v_enddate
    and C.record_type_id in ('012o00000012bKoAAI', '012o00000012ZrjAAE', '0121N0000019B0JQAU', '0121N000000uGPJQA2', '0121N000001AQG8QAO', '0121N000000qslUQAQ', '0121N000000qslTQAQ')
    and C.status != 'Closed'
    and C.institution_brand_c = 'a0ko0000002BSH4AAO'
    and C.is_deleted=false

),


-- Take case history for field ('created', 'Status') and order the history status


case_history_raw as (

  select
    uc.id as case_id,
    uc.type,
    uc.subtype_c,
    uc.type_action,
    uc.banner_id,
    uc.gsfs_action,
    uc.gsfs_action_outcome,
    uc.record_type_id,
    uc.original_record_type,
    uc.case_number,
    uc.origin,
    uc.case_closed_reason,
    uc.status,
    uc.parent_id,
    uc.owner_id_case,
    uc.case_created_date,
    ch.data_type,
    ch.field,
    ch.new_value,
    ch.old_value,
    ch.created_by_id,
    -- ch.created_date,
    cast(`utility.udf_convert_UTC_to_EST`(ch.created_date) as timestamp) as created_date,
    -- case when field = 'created' then 'New' --when field = 'Owner' then 'Ownership Change'
    --     when (field='Status' and new_value in ('Re-Open','Re-opened by Enrollment','Re-open - Banner')) then 'Re-Open'
    --     when field='Status' then new_value
    --     else null end as state,
    case  when field = 'created' then 'New' when field = 'Owner' then 'Ownership Change'
          when (field='Status' and new_value in ('Re-Open','Re-opened by Enrollment','Re-open - Banner')) then 'Re-Open'
          when field='Status' then new_value
          else null end as new_value_updated,

    case  when (field='Status' and old_value in ('Re-Open','Re-opened by Enrollment','Re-open - Banner')) then 'Re-Open'
          when field = 'Owner' then 'Ownership Change'
          when field='Status' then old_value
          else null end as old_value_updated,

    case  when field = 'created' then case_created_date
          else cast(`utility.udf_convert_UTC_to_EST`(ch.created_date) as timestamp) end as start_date,

    case when (new_value = 'Closed' and old_value != 'New') then 'Z' when (old_value = 'New') then 'B' when field = 'created' then 'A' else 'C' end as temp_sort,
    case when field = 'created' then 1 else 0 end is_created
  from
    unique_case uc left join `raw_b2c_sfdc.case_history` ch on uc.id = ch.case_id
  where
    -- (field in ('created','Status') or (data_type = 'EntityId' and field = 'Owner'))
    -- field IN ('created','Status')
    (field IN ('created') or (new_value is not null and old_value is not null and field in ('Status')))
    -- and date(CH.created_date) < v_enddate
  order by
    created_date

),


case_history_row_num as (

  select
    *, row_number() over (partition by case_id order by created_date, temp_sort) as row_num
  from
    case_history_raw
  order by
    start_date

),


cte_last_user_temp as (
  select
    *,
    max(row_num) over(partition by case_id) as row_max_last, from case_history_row_num
  where
    cast(`utility.udf_convert_UTC_to_EST`(created_date) as date) < v_enddate

),


cte_last_obj as (

  select
    case_id,
    max(created_by_id) over(partition by case_id order by created_date desc ) as created_by_id,
    row_number() over (partition by case_id order by created_date desc) as row_num
  from
    `raw_b2c_sfdc.case_history` ch inner join unique_case uc on ch.case_id = uc.id
  where
    cast(`utility.udf_convert_UTC_to_EST`(ch.created_date) as date) >= v_startdate
    and cast(`utility.udf_convert_UTC_to_EST`(ch.created_date) as date) < v_enddate

),


cte_last_user as (

  select
    distinct case_id,
    cte.created_by_id,
    concat(u.first_name,' ', u.last_name) as last_modified_by_user,
    ifnull(u.department_c, u.department) as last_modified_by_dept, m.name as last_modified_by_manager
  from
    cte_last_obj cte
  left join
    `raw_b2c_sfdc.user` u on cte.created_by_id = u.id
  left join
    `raw_b2c_sfdc.user` m on upper(u.manager_id) = upper(m.id)
  where
    row_num = 1

),


cte_sla_out_owner as (

  select
    case_id,
    max(ch.new_value) over(partition by case_id order by created_date desc ) as case_sla_out_owner,
    row_number() over (partition by case_id order by created_date desc) as row_num
  from
    `raw_b2c_sfdc.case_history` ch
  inner join
    unique_case uc on ch.case_id = uc.id
  where
    cast(`utility.udf_convert_UTC_to_EST`(ch.created_date) as date) < v_startdate
    and field = 'Owner'  and data_type = 'Text'
  qualify
    row_num = 1

),


cte_case_closed_reason as (

  select
    case_id,
    max(ch.new_value) over(partition by case_id order by ch.created_date desc ) as case_closed_reason_ch,
    row_number() over (partition by case_id order by created_date desc) as row_num
  from
    `raw_b2c_sfdc.case_history` ch inner join unique_case uc on ch.case_id = uc.id
  where
    --cast(`utility.udf_convert_UTC_to_EST`(ch.created_date) as date) >= v_startdate and
    cast(`utility.udf_convert_UTC_to_EST`(ch.created_date) as date) < v_enddate
    and field = 'Case_Closed_Reason__c'
  qualify
    row_num = 1

),


case_history as (

  select
    case_id,
    type,
    subtype_c,
    type_action,
    banner_id,
    gsfs_action,
    gsfs_action_outcome,
    record_type_id,
    original_record_type,
    case_number,
    origin,
    case_closed_reason,
    status,
    parent_id,
    owner_id_case,
    case_created_date,
    data_type,
    field,
    new_value,
    old_value,
    created_date,
    -- state,
    new_value_updated,
    old_value_updated,
    start_date,
    case  when field = 'Owner' then new_value
          when (lead(field)over (partition by case_id order by row_num) = 'Owner') then lead(old_value)over (partition by case_id order by row_num)
          else null end as owner_id,
    temp_sort,
    is_created,
    row_num
  from
    case_history_row_num
  order by
    row_num

),


case_first_state_temp as (

  select
    case_id,
    min(row_num) as first_state
  from
    case_history
  where
    field = 'Status'
  group by
    case_id

),


case_first_state as (

  select
    chrn.case_id as case_id,
    chrn.old_value as first_state
  from
    case_history chrn
  inner join
    case_first_state_temp cfst
  on chrn.case_id = cfst.case_id and chrn.row_num = cfst.first_state

),


case_iscreated as (

  select
    case_id,
    max(is_created) as is_created
  from
    case_history
  group by
    case_id

),


case_history_iscreated as (

  select
    cht.case_id,
    cht.type,
    cht.subtype_c,
    cht.type_action,
    cht.banner_id,
    cht.gsfs_action,
    cht.gsfs_action_outcome,
    cht.record_type_id,
    cht.original_record_type,
    cht.case_number,
    cht.origin,
    cht.case_closed_reason,
    cht.status,
    cht.parent_id,
    cht.owner_id_case,
    cht.case_created_date,
    cht.data_type,
    cht.field,
    cht.new_value,
    cht.old_value,
    cht.created_date,
    cht.new_value_updated,
    cht.old_value_updated,
    --cht.state,
    case when row_num = 1 then case_created_date else start_date end as start_date,
    owner_id,
    row_num,
    max(row_num) over(partition by cht.case_id) as row_max,
    cic.is_created as is_created
  from
    case_history cht
  inner join
    case_iscreated cic on cht.case_id = cic.case_id

),


case_owner_info_temp as (

  select
    case_id, type,
    subtype_c,
    type_action,
    banner_id,
    gsfs_action,
    gsfs_action_outcome,
    record_type_id,
    original_record_type,
    case_number,
    origin,
    case_closed_reason,
    status,
    parent_id,
    owner_id_case,
    case_created_date,
    data_type,
    field,
    new_value,
    old_value,
    created_date,
    new_value_updated,
    old_value_updated,
    --state,
    case when (row_num != 1 and is_created = 0) then LAG(created_date) over (partition by case_id order by row_num) else start_date end as start_date,
    owner_id,
    row_num,
    row_max,
    is_created,
    case  when (row_num = 1 and row_max = 1) then created_date
          when is_created = 0 then created_date else lead(start_date) over (partition by case_id,type order by row_num) end as end_date,
    last_value(owner_id ignore nulls ) over (partition by case_id order by case_id, row_num) as owner_id_new,
    lead(new_value_updated) over (partition by case_id order by row_num) as new_state,
    last_value(owner_id ignore nulls ) over (partition by case_id order by case_id, row_num desc) as owner_id_rev_fill
  from
    case_history_iscreated
  order by
    row_num

),


case_owner_info as (

  select
    case_id,
    type,
    subtype_c,
    type_action,
    banner_id,
    gsfs_action,
    gsfs_action_outcome,
    record_type_id,
    original_record_type,
    case_number,
    origin,
    case_closed_reason,
    status,
    parent_id,
    owner_id_case,
    case_created_date,
    data_type,
    field,
    new_value,
    old_value,
    created_date,
    -- state,
    new_value_updated,
    old_value_updated,
    start_date,
    owner_id,
    row_num,
    row_max,
    end_date,
    -- ifnull(owner_id_new, owner_id_rev_fill) as owner_id_new,
    ifnull(ifnull(owner_id_new, owner_id_rev_fill), owner_id_case) as owner_id_new,
    new_state,
    is_created
  from
    case_owner_info_temp
  order by
    row_num

),


case_dep as (

  select
    coi.case_id,
    coi.type,
    coi.subtype_c as subtype,
    coi.type_action,
    coi.banner_id,
    coi.gsfs_action,
    coi.gsfs_action_outcome,
    coi.record_type_id,
    rt.name as record_type,
    coi.original_record_type,
    coi.case_number,
    coi.origin,
    coi.case_closed_reason,
    coi.status,
    coi.parent_id,
    coi.owner_id_case,
    case when (row_num = 1 and row_max = 1) then old_value when is_created = 0 then old_value else coi.new_value_updated end as old_state,
    case when (row_num = 1 and row_max = 1) then new_value when is_created = 0 then new_value else coi.new_state end as new_state,
    coi.start_date ,
    coi.end_date,
    coi.owner_id_new as owner_id,
    concat(u.first_name,' ', u.last_name) as owner_name, u.department, u.department_c , gp.name, coi.row_num, coi.created_date, loe_q.departmentname,
    m.name as owner_manager
  from
    case_owner_info coi
  left join
    `raw_b2c_sfdc.user` u on coi.owner_id_new = u.id
  left join
    `raw_b2c_sfdc.group` gp on coi.owner_id_new = gp.id
  left join
    -- `trans_edw_sm_dm_dbo.dim_loe_queue` loe_q on lower(gp.name) = lower(loe_q.name)
	`rpt_crm_mart.v_wldn_queue` loe_q on lower(gp.name) = lower(loe_q.name)
  left join
    `raw_b2c_sfdc.record_type` rt on coi.record_type_id = rt.id
  left join
    `raw_b2c_sfdc.user` m on upper(u.manager_id) = upper(m.id)
  -- WHERE end_date is not null -- commented so status of unclosed status can also be mapped

),


case_owner_dep as (

  select
    case_id,
    type,
    subtype,
    type_action,
    banner_id,
    gsfs_action,
    gsfs_action_outcome,
    record_type,
    original_record_type,
    case_number,
    origin,
    case_closed_reason,
    status,
    parent_id,
    owner_id_case,
    old_state,
    new_state,
    owner_id,
    ifnull(owner_name, name) as owner_name,
    owner_manager,
    -- ifnull(department_c, department) as department,
    ifnull(ifnull(department_c, department),departmentname) as department,
    start_date,
    end_date,
    row_num,
    created_date
  from
    case_dep

),


case_subcase_temp4 as (

  select
    cst.*,
    cfs.first_state
  from
    case_owner_dep cst
  left join
    case_first_state cfs ON cst.case_id = cfs.case_id
  order by
    row_num

),


case_resolution_history as (

  select
    case_id,
    -- concat(case_id,'-', subcase_nb) as subcase,
    type,
    subtype,
    type_action,
    banner_id,
    gsfs_action,
    gsfs_action_outcome,
    record_type,
    original_record_type,
    case_number,
    origin,
    case_closed_reason,
    status,
    parent_id,
    -- owner_id_case as case_owner,
    -- case when (row_num = 1 and first_state is not null) then first_state else old_state end as start_status,
    ifnull(case when (row_num = 1 and first_state is not null) then first_state else old_state end, status) as start_status,
    -- new_state as end_status,
    case when cast(end_date as date) < v_enddate then new_state else null end as end_status,
    owner_id,
    owner_name,
    owner_manager,
    department,
    start_date,
    -- end_date,
    case when (new_state is not null and cast(end_date as date) < v_enddate) then end_date else null end as end_date,
    row_num
  from
    case_subcase_temp4
  order by
    case_id, start_date

),


case_sla_nor_flag_temp as (

  select
    crh.*,
    case when start_status in ('New', 'Re-Open', 'Open', 'Transferred', 'Work Completed') then 1 else 0 end as sla_start,--nor_flag,
    -- case when start_status not in ('New', 'Re-Open', 'Open', 'Transferred') then 1 else 0 end as sla_end,
    case when (lag (department) over (partition by case_id order by row_num)) != department then 1 else 0 end as dept_change,
    dsla.slahrs as slahrs,
    24 as default_sla_hrs,
    --dsla.weekendhrsflag as is_dept_weekend_working,
    case when dsla.weekendhrsflag is null then 'NA' else dsla.weekendhrsflag end as is_dept_weekend_working
  from
    case_resolution_history crh
  left join
    -- `trans_edw_sm_dm_dbo.dim_loe_department` dsla on crh.department = dsla.departmentname and dsla.institutiondimkey = 3
    `rpt_academics.v_sla_department` dsla on crh.department = dsla.departmentname and dsla.institution_id = 5

),


case_sla_nor_flag as (

  select
    * except(start_status, start_date),
    case when (lag(start_status) over (partition by case_id order by row_num)= 'New' and start_status in ('Re-Open'))
    then LAG(start_status) over (partition by case_id order by row_num)
    else start_status end as start_status,
    case when (lag(start_status) over (partition by case_id order by row_num)= 'New' and start_status in ('Re-Open'))
    then LAG(start_date) over (partition by case_id order by row_num)
    else start_date end as start_date

  from
    case_sla_nor_flag_temp
  order by
    start_date

),


-- ####################### Logic to get next working day after holidays - START #########################


temp_check_holiday as (

  select
    *,
    -- case when lead(cal_dt) over (order by cal_dt) = date_add(cal_dt, interval 1 day) then 2*24 else 24 end hrs_to_add,
    date_add(cal_dt, interval (case when lead(cal_dt) over (order by cal_dt) = date_add(cal_dt, interval 1 day) then 2 else 1 end) DAY) as temp_nxt_day,
  from
    `mdm.v_dim_date_holiday`
  -- where
  --   cal_dt >= '2022-01-01'
  order by
    cal_dt

),


temp_check_holiday_weekend as (

  select
    cal_dt,
    HOLIDAY_NAME,
    temp_nxt_day,
    case  when extract(dayofweek from temp_nxt_day) = 1 then date_add(date(temp_nxt_day), interval 1 day)
          when extract(dayofweek from temp_nxt_day) = 7 then date_add(date(temp_nxt_day), interval 2 day)
          else temp_nxt_day end as temp_weekend_nxt_day
  from
    temp_check_holiday

),


get_nxt_working_day_after_holiday_temp as (

  select
    cal_dt as holiday_date,
    HOLIDAY_NAME as holiday_name,
    --temp_weekend_nxt_day,
    case  when (select count(1) from `mdm.v_dim_date_holiday` where cal_dt = temp_weekend_nxt_day) >= 1 then date_add(date(temp_weekend_nxt_day), interval 1 day)
          else temp_weekend_nxt_day end as nxt_day_weekend_holiday,
    -- temp_nxt_day,
    case  when (select count(1) from `mdm.v_dim_date_holiday` where cal_dt = temp_nxt_day) >= 1 then date_add(date(temp_nxt_day), interval 1 day)
          else temp_nxt_day end as nxt_day_holiday
  from
    temp_check_holiday_weekend

),


get_nxt_working_day_after_holiday as (

  select
    holiday_date,
    holiday_name,
    nxt_day_weekend_holiday,
    date_diff(nxt_day_weekend_holiday, holiday_date, day) as day_add_weekend_holiday,
    nxt_day_holiday,
    date_diff(nxt_day_holiday, holiday_date, day) as day_add_holiday
  from
    get_nxt_working_day_after_holiday_temp

),


-- ####################### Logic to get next working day after holidays - end ###########################


case_sla_rpt_dates as (

  select
    csla.case_id,
    csla.type,
    csla.subtype,
    csla.type_action,
    csla.banner_id,
    csla.gsfs_action,
    csla.gsfs_action_outcome,
    csla.record_type,
    csla.original_record_type,
    csla.case_number,
    csla.origin,
    csla.case_closed_reason,
    csla.status,
    csla.parent_id,
    csla.start_status,
    csla.end_status,
    csla.owner_id,
    csla.owner_name,
    csla.owner_manager,
    csla.department,
    csla.start_date as actual_sla_start,
    csla.end_date as actual_sla_end,
    csla.dept_change,
    -- date_diff(end_date, start_date, HOUR) actual_response_duration,
    -- round(date_diff(end_date, start_date, second)/3600, 4) actual_response_duration,
    case when end_date is not null then round(date_diff(end_date, start_date, second)/3600, 4)
    else round(date_diff(cast(concat(v_enddate, ' ', case when v_enddate = current_date() then current_time("America/New_York") else '00:00:00.000000' end, ' UTC') as timestamp), start_date, second)/3600, 4) end as actual_response_duration,
    csla.slahrs,
    csla.default_sla_hrs,
    is_dept_weekend_working,
    case  when (is_dept_weekend_working = 'Y' and nsd.holiday_date is not null ) then cast(concat(nsd.nxt_day_holiday, ' 00:00:00 UTC') as timestamp)
          when (is_dept_weekend_working != 'Y' and nsd.holiday_date is not null ) then cast(concat(nsd.nxt_day_weekend_holiday, ' 00:00:00 UTC') as timestamp)
          when extract(dayofweek from start_date) = 1 then cast(concat(date_add(date(start_date), interval 1 day), ' 00:00:00 UTC') as timestamp)
          when extract(dayofweek from start_date) = 7 then cast(concat(date_add(date(start_date), interval 2 day), ' 00:00:00 UTC') as timestamp)
          else start_date end rpt_sla_start_date,
    csla.end_date as rpt_sla_end_date,
  from
    case_sla_nor_flag csla
  -- left join `trans_edw_sm_dm_dbo.dim_loe_department`  dsla
  -- on csla.department = dsla.departmentname
  left join
    get_nxt_working_day_after_holiday nsd on date(csla.start_date) = nsd.holiday_date
  left join
    get_nxt_working_day_after_holiday ned on date(csla.end_date) = ned.holiday_date
  where
    sla_start = 1

),


case_sla_rpt_response_duration as
(

  select
    *,
    case when (is_dept_weekend_working = 'Y' and rpt_sla_end_date is not null) then (round(date_diff(rpt_sla_end_date, rpt_sla_start_date, SECOND)/3600, 4) -
    (select count(1)*24 from `mdm.v_dim_date_holiday` where cal_dt >= date(rpt_sla_start_date) and cal_dt <= date(rpt_sla_end_date)))

    when (is_dept_weekend_working != 'Y' and rpt_sla_end_date is not null) then (round(date_diff(rpt_sla_end_date, rpt_sla_start_date, SECOND)/3600, 4) -
    (select count(1)*24 from `mdm.dim_date`
      where cal_dt >= date(rpt_sla_start_date) and cal_dt <= date(rpt_sla_end_date) and cal_wk_day_nm IN ('SATURDAY','SUNDAY')) -
    (select count(1)*24 from `mdm.v_dim_date_holiday` where cal_dt >= date(rpt_sla_start_date) and cal_dt <= date(rpt_sla_end_date)))

    when (is_dept_weekend_working = 'Y' and rpt_sla_end_date is null) then (round(date_diff(cast(concat(v_enddate, ' ', case when v_enddate = current_date() then current_time("America/New_York") else '00:00:00.000000' end, ' UTC') as timestamp), rpt_sla_start_date, SECOND)/3600, 4) -
    (select count(1)*24 from `mdm.v_dim_date_holiday` where cal_dt >= date(rpt_sla_start_date) and cal_dt < date(cast(concat(v_enddate, ' ', case when v_enddate = current_date() then current_time("America/New_York") else '00:00:00.000000' end, ' UTC') as timestamp))))

    when (is_dept_weekend_working != 'Y' and rpt_sla_end_date is null) then (round(date_diff(cast(concat(v_enddate, ' ', case when v_enddate = current_date() then current_time("America/New_York") else '00:00:00.000000' end, ' UTC') as timestamp), rpt_sla_start_date, SECOND)/3600, 4) -
    (select count(1)*24 from `mdm.dim_date` where cal_dt >= date(rpt_sla_start_date) and cal_dt < date(cast(concat(v_enddate, ' ', case when v_enddate = current_date() then current_time("America/New_York") else '00:00:00.000000' end, ' UTC') as timestamp))
    and cal_wk_day_nm IN ('SATURDAY','SUNDAY')) -
    (select count(1)*24 from `mdm.v_dim_date_holiday` where cal_dt >= date(rpt_sla_start_date) and cal_dt < date(cast(concat(v_enddate, ' ', case when v_enddate = current_date() then current_time("America/New_York") else '00:00:00.000000' end, ' UTC') as timestamp))))


    end as rpt_response_duration,

    date_add(rpt_sla_start_date, interval coalesce(slahrs, default_sla_hrs) hour) as temp_planned_sla_end_date

  from
    case_sla_rpt_dates

),


case_sla_rpt_planned_end_temp as (

  select
    *,
    /* commented on 21-03-2023 for rectifying planned day issue
    (select count(1) * 24 from `mdm.dim_date`
    where cal_dt >= date(rpt_sla_start_date) and cal_dt <= date(rpt_sla_end_date) and cal_wk_day_nm IN ('SATURDAY','SUNDAY') ) as temp_planned_weekends,
    (select count(1) * 24 from `mdm.v_dim_date_holiday` where cal_dt >= date(rpt_sla_start_date) and cal_dt <= date(rpt_sla_end_date)) as temp_planned_holidays
    */

    (select count(1) * 24 from `mdm.dim_date`
    where cal_dt >= date(rpt_sla_start_date) and cal_dt <= date((date_add(rpt_sla_start_date, interval (coalesce(slahrs, default_sla_hrs)) hour)))
    and cal_wk_day_nm IN ('SATURDAY','SUNDAY') ) as temp_planned_weekends,
    (select count(1) * 24 from `mdm.v_dim_date_holiday` where cal_dt >= date(rpt_sla_start_date) and cal_dt <= date((date_add(rpt_sla_start_date, interval (coalesce(slahrs, default_sla_hrs)) hour)))) as temp_planned_holidays


  from
    case_sla_rpt_response_duration

),


case_sla_rpt_planned_end_wknd_holiday as (

  select
    *,
    case when is_dept_weekend_working = 'Y' then date_add(rpt_sla_start_date, interval (coalesce(slahrs, default_sla_hrs)+temp_planned_holidays) hour)
    else date_add(rpt_sla_start_date, interval (coalesce(slahrs, default_sla_hrs)+temp_planned_weekends+temp_planned_holidays) hour)
    end as temp_planned_sla_end_date_2,
  from
    case_sla_rpt_planned_end_temp

),


temp_case_status_sla as (

  select
    -- replace(cast(v_startdate as string), '-', '') as week_begin,
    case_id,
    type,
    subtype,
    type_action,
    banner_id,
    gsfs_action,
    gsfs_action_outcome,
    record_type,
    original_record_type,
    case_number,
    origin,
    case_closed_reason,
    status as case_status,
    parent_id,
    start_status,
    end_status,
    owner_id,
    owner_name,
    owner_manager,
    department,
    actual_sla_start,
    actual_sla_end,
    actual_response_duration,
    slahrs,
    default_sla_hrs,
    rpt_sla_start_date,
    rpt_sla_end_date,
    rpt_response_duration,
    case  when (extract(dayofweek from temp_planned_sla_end_date_2) = 1 and is_dept_weekend_working != 'Y')
          then cast(date_add(temp_planned_sla_end_date_2, interval 1 day) as timestamp)
          when (extract(dayofweek from temp_planned_sla_end_date_2) = 7 and is_dept_weekend_working != 'Y')
          then cast(date_add(temp_planned_sla_end_date_2, interval 2 day) as timestamp)
          else temp_planned_sla_end_date_2 end as planned_sla_end_date,

    case when rpt_response_duration <= coalesce(slahrs, default_sla_hrs) then 'Y' else 'N' end as in_sla,

  from
    case_sla_rpt_planned_end_wknd_holiday
  where
    (end_status not in ('New', 'Re-Open', 'Open', 'Transferred', 'Work Completed') or end_status is null)
    -- where end_holiday is not null
    and ((date(actual_sla_start) >= v_startdate and date(actual_sla_start) < v_enddate) or (date(actual_sla_end) >= v_startdate and date(actual_sla_end) < v_enddate))
  order by
    actual_sla_start

)


-- ####################### Logic to handle cases with status 'Closed' in case table but no entry of status 'Closed' in case_history table. - STARTS ########################



-- ####################### Logic to handle cases with status 'Closed' in case table but no entry of status 'Closed' in case_history table. - end ###########################


select
  5 as institution_id,
  'WLDN' as institution,
  cast(replace(cast(v_weekbegindate as string), '-', '') as int64) as week_begin,
  t1.case_id,
  type,
  subtype,
  type_action,
  record_type,
  original_record_type,
  cast(case_number as string) as case_number,
  origin,
  -- case_closed_reason as reason,
  ccr.case_closed_reason_ch as case_closed_reason,
  case_status,
  parent_id,
  start_status,
  end_status,
  owner_id,
  owner_name,
  owner_manager,
  department,
  actual_sla_start,
  actual_sla_end,
  actual_response_duration,
  slahrs,
  default_sla_hrs,
  rpt_sla_start_date,
  rpt_sla_end_date,
  rpt_response_duration,
  -- planned_sla_end_date,
  case when in_sla = 'N' then planned_sla_end_date else null end as planned_sla_end_date,
  in_sla,
  gsfs_action,
  gsfs_action_outcome,
  banner_id,
  case when in_sla = 'N' then ifnull(csoo.case_sla_out_owner, owner_name) else null end as sla_out_owner,
  -- case when in_sla = 'N' then csoo.case_sla_out_owner else null end as sla_out_owner,
  case when last_modified_by_user = 'None' then null else last_modified_by_user end as last_modified_by_user,
  case when last_modified_by_dept = 'None' then null else last_modified_by_dept end as last_modified_by_dept,
  case when last_modified_by_manager = 'None' then null else last_modified_by_manager end as last_modified_by_manager,
  case when end_status is null then 'Y' else 'N' end as actual_sla_end_nor_flag,
  'service' as case_type,
  'SC-UC' as case_type_category,
  current_timestamp() as etl_created_date,
  cast(null as timestamp) as etl_updated_date,
  'trans_crm_mart.sp_wldn_case_service_unclosed_sla' as etl_resource_name,
  v_audit_id as etl_ins_audit_key,
  cast(null as string) etl_upd_audit_key

from
  temp_case_status_sla t1
left join
  cte_last_user clu on t1.case_id = clu.case_id
left join
  cte_case_closed_reason ccr on t1.case_id = ccr.case_id
left join
  cte_sla_out_owner csoo on t1.case_id = csoo.case_id
where
  end_status is null;


set job_end_dt = current_timestamp();
set job_completed_ind = 'Y';

insert into `audit_cdw_log.audit_load_details` (audit_load_key, job_name, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source)
values (v_audit_id, target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);


commit transaction;

exception when error then

  -- select @@error.message;
  rollback transaction;

  insert into `audit_cdw_log.audit_load_details` (audit_load_key, job_name, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source)
  values (upper(replace(generate_uuid(), "-", "")), 't_wldn_case_sla', current_timestamp(), cast(NULL as TIMESTAMP), 'N', 'CASE-SERVICE-UNCLOSED-SLA', 'scheduled query', 'trans_crm_mart.sp_wldn_case_service_unclosed_sla');

  insert into
  audit_cdw_log.error_log (error_load_key, process_name, table_name, error_attribute, error_details, etl_create_date, etl_resource_name)
  values
  (upper(replace(generate_uuid(), "-", "")),'CASE-SERVICE-UNCLOSED-SLA', 't_wldn_case_sla', @@error.statement_text, @@error.message, current_timestamp() ,'trans_crm_mart.sp_wldn_case_service_unclosed_sla') ;

  -- RAISE USING message = @@error.message;


end
