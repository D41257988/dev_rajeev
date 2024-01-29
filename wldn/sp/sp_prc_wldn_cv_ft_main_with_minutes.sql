begin

declare institution string default 'WLDN';
declare institution_id int64 default 5;
declare dml_mode string default 'delete-insert';
declare target_dataset string default 'trans_academics';
declare target_tablename string default 'prc_wldn_cv_ft_main_with_minutes';
declare source_tablename string default 'prc_wldn_cv_ft_main_with_minutes_temp';
declare load_source string default 'trans_academics.sp_prc_wldn_cv_ft_main_with_minutes';
--declare result string default "";
declare additional_attributes ARRAY<struct<keyword string, value string>>;
declare last_refresh_time timestamp;
declare tgt_table_count int64;

/* common across */
declare job_start_dt timestamp default current_timestamp();
declare job_end_dt timestamp default current_timestamp();
declare job_completed_ind string default null;
declare job_type string default 'canvas';
declare load_method string default 'scheduled query';
declare out_sql string;


begin

SET additional_attributes= [("audit_load_key", v_audit_key),
          ("load_method",load_method),
          ("load_source",load_source),
          ("job_type", job_type)];
/* end common across */


create or replace temp table `lea` as
select *
	from (
		select *,
				row_number() over(partition by banner_id_c order by last_modified_date desc, created_date desc) as row_num
		from `rpt_crm_mart.v_wldn_loe_enrollment_advisors`
		) where row_num=1;


create or replace temporary table `prc_wldn_cv_ft_main_with_minutes_temp` as

with src as (
select * from (
  select distinct
    5 as institution_id,
    main.institution,
    main.user_id,
    main.course_user_pk1,
    cast(main.crs_main_pk1 as int64) as crs_main_pk1,
    main.person_uid,
    main.credential_id,
    main.first_name,
    main.last_name,
    main.email,
    main.student_phone_number,
    main.state_province,
    main.campus_desc as course_campus_desc,
    main.part_of_term,
    main.program,
    main.program_desc,
    main.course_code,
    main.course_identification,
    cast(main.course_credits as bignumeric) course_credits,
    main.last_attend_date,
    main.student_type,
    main.instructor_name,
    main.instructor_email,
    main.course_reference_number,
    main.subject,
    main.course_number,
    main.academic_period,
    main.offering_number,
   main.student_campus,
    main.section_start_date,
    main.section_end_date,
    main.last_login_dt,
    main.posted_dt,
    lea.name as ea_name,----------------------------------------------------------------------
    lea.manager_name as ea_mgr_group,-----------------------------------------------------------------------
    main.recruiter,
    main.recruiter_desc,
    main.aa_person_uid,
    main.student_start_date,
	main.sc_registration_status as registration_status,		-- added on 8/13 as part of ft registration activity inclusion
	main.registration_status_date,	-- added on 8/13 as part of ft registration activity inclusion
	main.last_date_change,			-- added on 8/13 as part of ft registration activity inclusion
	main.section_add_date,			-- added on 8/13 as part of ft registration activity inclusion
    main.activity_submission_count,
    main.activity_submission_count_prior_to_start,
    main.gradable_item_received_count,
    main.gradable_item_received_count_with_classcafe,
    main.login_minutes,
	main.lms_indicator,
	--row_number() over(partition by main.credential_id order by lea.last_modified_date, lea.created_date desc) as row_num,-------------------------------------------
    cast(coalesce((
	select sum(coalesce(floor(minutesspent), 0)) from `trans_academics.prc_wldn_cv_ft_agg_activity_inst_min` activity
	where main.user_id = activity.user_id and cast(main.crs_main_pk1 as string) = cast(activity.course_id as string) and lastclick >= main.section_start_date
	), null) as bignumeric) activity_minutes,
	cast(coalesce((select sum(coalesce(floor(minutesspent), 0)) from `trans_academics.prc_wldn_cv_ft_agg_activity_inst_min` activity
    where main.user_id = activity.user_id and cast(main.crs_main_pk1 as string) = cast(activity.course_id as string) and lastclick < main.section_start_date), null) as bignumeric) activity_minutes_prior_to_start,
	'WLDN_CV_BNR' as source_system_name
  from `trans_academics.prc_wldn_cv_ft_main` main
  left join lea
  on main.recruiter = lea.banner_id_c
--  where banner_id_c='BCA'
  ) --where row_num=1  ----------------------------------------------------------

 )

  select src.*,
        job_start_dt as etl_created_date,
        job_start_dt as etl_updated_date,
        load_source as etl_resource_name,
        v_audit_key as etl_ins_audit_key,
        v_audit_key as etl_upd_audit_key,
        farm_fingerprint(format('%T', concat(user_id,credential_id,academic_period,crs_main_pk1))) AS etl_pk_hash,
        farm_fingerprint(format('%T', src )) as etl_chg_hash,
        FROM src;



-- merge process
  call utility.sp_process_elt (institution, dml_mode , target_dataset, target_tablename, null, source_tablename, additional_attributes, out_sql );


  set job_end_dt = current_timestamp();
  set job_completed_ind = 'Y';

-- export success audit log record
  call `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);


  set v_result_sp_prc_wldn_cv_ft_main_with_minutes = 'SUCCESS';

EXCEPTION WHEN error THEN

  SET job_end_dt = cast (NULL as TIMESTAMP);
  SET job_completed_ind = 'N';

CALL `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);


-- insert into error_log table
  insert into `audit_cdw_log.error_log` (error_load_key, process_name, table_name, error_details, etl_create_date, etl_resource_name, etl_ins_audit_key) values (v_audit_key,'CANVAS_FT_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;


  set v_result_sp_prc_wldn_cv_ft_main_with_minutes = @@error.message;

  RAISE USING message = @@error.message;



end;

end