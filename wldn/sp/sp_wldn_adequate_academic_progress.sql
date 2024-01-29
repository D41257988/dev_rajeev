begin

    declare institution string default 'WLDN';
    declare institution_id int64 default 5;
    declare dml_mode string default 'delete-insert';
    declare target_dataset string default 'rpt_academics';
    declare target_tablename string default 't_wldn_adequate_academic_progress';
    declare source_tablename string default 't_aap_report_temp';
    declare load_source string default 'trans_academics.sp_wldn_adequate_academic_progress';
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

---------------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE tbl_academicadvisor  AS
(
	with vw_map_loe_contact_aa as(
        SELECT
        c.id as contactsf_id,
        bp.banner_id_c as waldenbannerid ,
        c.walden_academic_advisor_c as waldenacademicadvisorsfid,
      	-- c.walden_academic_advisor_onyx_id_c, -- raj - column deleted from sandbox
        u.id as ServiceUsersfid

        from raw_b2c_sfdc.contact c
        join raw_b2c_sfdc.brand_profile_c as bp
        on bp.contact_c = c.id and c.is_deleted=false and lower(c.institution_code_c)='walden'
        left join rpt_crm_mart.v_wldn_service_user u
        on u.id = c.walden_academic_advisor_c
        where c.walden_academic_advisor_c is not null

  )
  select mca.waldenbannerid,mca.waldenacademicadvisorsfid,su.id as ServiceUsersfid ,su.name as academic_advisor_name
    from vw_map_loe_contact_aa  mca
    inner join rpt_crm_mart.v_wldn_service_user  su
                on mca.waldenacademicadvisorsfid = su.id
                where mca.waldenbannerid is not null
                and mca.waldenacademicadvisorsfid is not null


);

create or replace temp table lms_data as(
    /*--------------------blackboard----------------*/
    select
       bu.pk1 as user_pk1 ,
	   cm.pk1 as crsmain_pk1,
	   cu.pk1 as course_user_pk1,
		xl.bnr_crn,
		xl.academic_period,
		xl.institution,
		xl.institution_id,
		xl.credential_id,
		'Black Board'  as lms_indicator

	from  rpt_academics.t_student_course sc
    left join trans_academics.prc_ft_xref_bbcm_bnrsc xl on
          xl.bnr_crn = sc.course_reference_number
			and xl.academic_period = sc.academic_period
			and xl.institution = sc.institution

    left join stg_l1_bb_bblearn.users  bu
			on bu.pk1 = xl.users_pk1 and split(bu.batch_uid, '.')[OFFSET(0)] =  case when xl.institution='WLDN' then 'USW1' else xl.institution end
			and bu.row_status != 2
			and bu._fivetran_deleted=false
			and xl.institution_id =5


		left join stg_l1_bb_bblearn.course_main cm
			on cm.pk1 = xl.crsmain_pk1	and right(cm.course_id,6) = xl.academic_period
			and left(cm.course_id,4)= case when xl.institution='WLDN' then 'USW1' else xl.institution end
			and cm.row_status != 2
			and cm._fivetran_deleted=false

		left join stg_l1_bb_bblearn.course_users  cu
			on cu.users_pk1 = bu.pk1 --and cu.bb_version = bu.bb_version
			and cu.crsmain_pk1 = cm.pk1	and cu.row_status != 2
			and cu._fivetran_deleted=false

  union all
  /* canvas data*/
	select
	bu.canvas_id as user_pk1,
	cm.canvas_id as crsmain_pk1,
	cu.canvas_id as course_user_pk1,
    xl.bnr_crn,
	xl.academic_period,
	xl.institution,
	xl.institution_id,
	xl.credential_id,
	'Canvas' as lms_indicator

	from  rpt_academics.t_student_course sc
    left join trans_academics.prc_ft_xref_bbcm_bnrsc xl on
          xl.bnr_crn = sc.course_reference_number
			and xl.academic_period = sc.academic_period
			and xl.institution = sc.institution

    left join trans_lms.prc_wldn_user  bu
			on bu.canvas_id = xl.users_pk1
			and bu.is_active=true

		left join  trans_lms.prc_wldn_course cm
			on cm.canvas_id = xl.crsmain_pk1
			and cm.is_active=true

		left join trans_lms.prc_wldn_course_enrollment  cu
			on cu.canvas_id = bu.canvas_id  and cu.canvas_id = cm.canvas_id
			and cu.is_active=true
  );

CREATE OR REPLACE TEMP TABLE prc_aap_student_details  AS
(
	with v_internet_address_max_activity_date as(
	select  a.* from rpt_academics.v_internet_address a
												join   (SELECT MAX(activity_date) as mx_activity_date,entity_uid,internet_address_preferred_ind,internet_address_status,institution FROM `rpt_academics.v_internet_address`
												where internet_address_preferred_ind = 'Y'  and internet_address_status = 'A'
												group by entity_uid,internet_address_preferred_ind,internet_address_status,institution) b
												on a.entity_uid = b.entity_uid
												 and a.activity_date = b.mx_activity_date
												 and a.internet_address_preferred_ind =b.internet_address_preferred_ind
												 and a.internet_address_status =b.internet_address_status
												 and a.institution = b.institution
	),
	t_address_max_add_no as(
		select  a.* from rpt_academics.t_address a
												join   (SELECT MAX(address_number) as mx_add_no,entity_uid,preferred_address_ind,institution FROM `rpt_academics.t_address`
												where preferred_address_ind = 'Y'
												group by entity_uid,preferred_address_ind,institution
												) b
												on a.entity_uid = b.entity_uid
												 and a.address_number = b.mx_add_no
												 and  a.preferred_address_ind = b.preferred_address_ind
												 and a.institution = b.institution
	),
	v_telephone_max_psn as(
		select  a.* from rpt_academics.v_telephone a
												join   (SELECT MAX(phone_seq_number) as mx_phone_seq_number,entity_uid,phone_type,institution FROM `rpt_academics.v_telephone`
												where phone_type = 'HOME'  group by entity_uid,phone_type,institution) b
												on a.entity_uid = b.entity_uid
												and a.phone_seq_number = b.mx_phone_seq_number
												and  a.phone_type = b.phone_type
												and a.institution = b.institution
	),
	v_admissions_application_max_app_no as(
			select  a.* from rpt_academics.v_admissions_application a
			 join (
				SELECT MAX(application_number) as mx_application_number,aa.person_uid,aa.institution
				 FROM `rpt_academics.v_admissions_application` aa
				 join rpt_academics.t_general_student gs on  aa.person_uid = gs.person_uid
				 and aa.application_date < gs.start_date
				 and aa.institution_id=gs.institution_id
				 where latest_decision in ('RS','RI','RC')
				 group by person_uid,institution) b
			on a.person_uid = b.person_uid
			and a.institution = b.institution
			and a.application_number = b.mx_application_number
	)




  select  distinct sc.institution as campus_desc,

	concat(sp.credential_id ,sc.course_reference_number) as studentcrse,
	sp.credential_id as student_id,
	sp.last_name as student_last_name,
	sp.first_name as student_first_name,
	sc.start_date,
	ia.internet_address as student_email,
	sa.state_province as state,
	st.phone_number_combined as student_phone_number,
	sc.academic_period as term,
	sc.sub_academic_period as part_of_term,
	gs.program,
	concat(sc.subject , ' - ' , sc.course_number  , ' - ' , sc.course_section_number )as course_code,
	sc.course_reference_number,
	co.part_of_term_start_date as course_start_date,
	co.part_of_term_end_date as course_end_date,
	sc.last_attend_date as last_login_date,
	case when gs.start_date < lt.start_date then 'CONT'
	when gs.student_population = 'U' then 'CONT'
	else 'NEW'
	end as student_type,
	case when last_attend_date < co.part_of_term_start_date or last_attend_date is null
		 then 'Y'
		 else 'N'
	end as fta_flag,
	case when cc.course_info1 = 'FTA_EXEMPT'
		 then 'Y'
		 else 'N'
	end as fta_exempt_ind,
	case when cc.course_info2 = 'NON_REVENUE'
		 then 'Y'
		 else 'N'
	end as non_revenue_ind,
	case when cc.course_info3 is not null--= 'reasearch_forum'
		 then 'Y'
		 else 'N'
	end as reasearch_forum_ind,
	concat(sc.subject , ' - ' , sc.course_number) as course_identification,
	null as no_sub_flag,
	l.user_pk1,
	l.crsmain_pk1,
	l.course_user_pk1,
	l.credential_id as xref_id_number,
	l.lms_indicator,
	case when sc.campus is null then sc.institution
	--torrens
	when (sc.campus = 'O' or sc.campus = 'M') and sc.institution = 'AUT1' then 'AUT1'
	when sc.campus = 'TUA' then 'AUT1'
	--gilion
	when sc.campus = 'M' and sc.institution  = 'CHG1' then 'CHG1'
	when sc.campus = 'G' then 'CHG1'
	--univeristy of liverpool
	when sc.campus = 'M' and sc.institution  = 'UKL1' then 'UKL1'
	when sc.campus = 'LO' then 'UKL1'
	--kendall
	when sc.campus = 'KC' then 'USK1' -- consortium home
	when (sc.campus = 'M' or sc.campus = 'UCL' )and sc.institution  = 'USK1' then 'USK1'
	when sc.campus = 'ucl' and sc.institution  = 'USK1' then 'USK1'
	--nsad
	when (sc.campus = 'M' or sc.campus = 'WB') and sc.institution  = 'USN1' then 'USN1'
	--nhu
	when sc.campus = 'NHU' then 'USN2' -- consortium home
	when (sc.campus = 'CCC' or sc.campus = 'SMC' or sc.campus = 'EPA' or sc.campus = 'M') and sc.institution ='USN2' then 'USN2'
	--walden
	when sc.campus = 'WAL' then 'WLDN'--'USW1' -- consortium home
	--sfuad
	when (sc.campus = 'CSF' or sc.campus = 'SF') then 'USS1'

	else sc.campus
	end as course_campus,

	sc.campus_desc course_campus_desc,
	gs.campus as student_campus,
	gs.campus_desc as student_campus_desc,
	concat(coalesce(it.instructor_last_name,''), ' ' , coalesce(it.instructor_first_name,'') )as instructor_name,
	aa.recruiter_desc as ea_rep_name,

	mca.academic_advisor_name

	from rpt_academics.t_student_course sc

		inner join rpt_academics.v_course_catalog cc
		on sc.academic_period = cc.academic_period
		and sc.course_number = cc.course_number
		and sc.subject = cc.subject
		and sc.institution = cc.institution --cc.multi_source --need to check with praveena


		inner join rpt_academics.t_person sp
			on sc.person_uid = sp.person_uid
			and sc.institution = sp.institution


		inner join rpt_academics.v_term lt
			on sc.academic_period = lt.academic_period
			and lt.institution  = sc.institution


		inner join rpt_academics.t_course_offering co
			on sc.academic_period = co.academic_period
			and sc.course_reference_number = co.course_reference_number
			and sc.subject = co.subject
			and sc.institution = co.institution
			and sc.course_number = co.course_number
      and CAST(CURRENT_DATE() as string format 'YYYYMMDD') in (CAST(co.part_of_term_start_date+6 as string format 'YYYYMMDD'),CAST(co.part_of_term_start_date+7 as string format 'YYYYMMDD'),CAST(co.part_of_term_start_date+8 as string format 'YYYYMMDD'),CAST(co.part_of_term_start_date+9 as string format 'YYYYMMDD'),CAST(co.part_of_term_start_date+10 as string format 'YYYYMMDD'),CAST(co.part_of_term_start_date+11 as string format 'YYYYMMDD'),CAST(co.part_of_term_start_date+12 as string format 'YYYYMMDD'),CAST(co.part_of_term_start_date+13 as string format 'YYYYMMDD'),CAST(co.part_of_term_start_date+14 as string format 'YYYYMMDD'),CAST(co.part_of_term_start_date+15 as string format 'YYYYMMDD'),CAST(co.part_of_term_start_date+16 as string format 'YYYYMMDD'))


		inner join rpt_academics.t_general_student gs
			on sp.person_uid = gs.person_uid
			and gs.primary_program_ind = 'Y'
			and gs.academic_period = sc.academic_period
			and gs.institution  = sc.institution


		inner join rpt_academics.t_student_course_reg_audit ra
			on sc.person_uid = ra.person_uid
			and sc.academic_period = ra.academic_period
			and sc.course_reference_number = ra.course_reference_number
			and sc.institution = ra.institution

		left join v_internet_address_max_activity_date ia --rpt_academics.v_internet_address ia
			on sp.person_uid = ia.entity_uid
			and ia.institution  = sc.institution


		left join t_address_max_add_no sa
			on sp.person_uid = sa.entity_uid
			AND SA.institution = sc.institution


		 left join v_telephone_max_psn st
			on sp.person_uid = st.entity_uid
			and st.institution  = sp.institution


		left join rpt_academics.v_instructional_assignment it
			on sc.course_reference_number = it.course_reference_number
			and sc.academic_period = it.academic_period
			and sc.institution = it.institution


		left join v_admissions_application_max_app_no aa
			on sc.person_uid = aa.person_uid
			and sc.institution = aa.institution

	    join lms_data  l

			on l.credential_id = sp.credential_id
			-- and l.bnr_crn = sc.course_reference_number
			-- and l.academic_period = sc.academic_period
			-- and l.institution = sc.institution



		left join tbl_academicadvisor mca
			on mca.waldenbannerid=sp.credential_id

	where --sc.institution in (select property from ctrl_property where efta_ind = 'y') --= @v_property
	sc.registration_status  in ('RE','RW','AU','RL')
	and ra.registration_status  in ('RE','RW','AU','RL')
	and sc.sub_academic_period not in ('22','24','42') -- added the condition to eliminate part of terms for uvm as per kris willing on 09082011
	--and sc.course_identification like 'comm1001%'
	and ltrim(rtrim(sc.course_identification)) in ('HMNT1001')

	and sc.institution_id=5
		and cc.institution_id=5
		and lt.institution_id=5
		and co.institution_id=5
		and sp.institution_id=5
		and gs.institution_id=5
		and ia.institution_id=5
		and ra.institution_id=5
		and sa.institution_id=5
		and st.institution_id=5
		and it.institution_id=5
		and aa.institution_id=5
		--and l.institution_id=5

	-- --and sc.person_uid = 41918
	--and sc.academic_period = '201550'


);


	CREATE OR REPLACE TEMP TABLE  participation_discussion
	as
	(
		SELECT DISTINCT SD.STUDENT_ID,SD.CAMPUS_DESC,SD.TERM,SD.PART_OF_TERM,SD.COURSE_REFERENCE_NUMBER,
			CASE  WHEN (REPLACE(SP.SUBJECT,' ','') like '%WEEK1%' OR REPLACE(AREA_NAME,' ','') like '%WEEK1%') THEN 'Week_1_Discussion'
				WHEN (REPLACE(SP.SUBJECT,' ','') like '%WEEK2%' OR REPLACE(AREA_NAME,' ','') like '%WEEK2%') THEN 'Week_2_Discussion'
			END as TITLE,
			CASE  WHEN (REPLACE(SP.SUBJECT,' ','') like '%WEEK1%'  OR REPLACE(AREA_NAME,' ','') like '%WEEK1%') THEN 'Y'
				WHEN (REPLACE(SP.SUBJECT,' ','') like '%WEEK2%' OR REPLACE(AREA_NAME,' ','') like '%WEEK2%' ) THEN 'Y'
			END as Student_Participation_Ind,
			concat(CASE  WHEN (REPLACE(SP.SUBJECT,' ','') like '%WEEK1%' OR REPLACE(AREA_NAME,' ','') like '%WEEK1%') THEN 'Week_1_Discussion'
				WHEN (REPLACE(SP.SUBJECT,' ','') like '%WEEK2%' OR REPLACE(AREA_NAME,' ','') like '%WEEK2%') THEN 'Week_2_Discussion'
			END , '_Student_Participation_Indicator') as PARTICIPATION_IND_VALUES


	from prc_aap_student_details sd
			inner join rpt_student_cdm.prc_lms_submissions_forum_posts sp
			on sd.crsmain_pk1 = sp.crsmain_pk1 and sd.user_pk1 = sp.users_pk1 --and sp.bb_version = 'BB9WA'
				 and (replace(sp.subject,' ','') like '%WEEK1%'  or replace(sp.subject,' ','') like '%WEEK2%'
				 	or replace(area_name,' ','') like '%WEEK1%'  or replace(area_name,' ','') like '%WEEK2%' )
		);

	  CREATE OR REPLACE TEMP TABLE participation_journals as
	  (
      SELECT DISTINCT SD.STUDENT_ID,SD.CAMPUS_DESC,SD.TERM,SD.PART_OF_TERM,SD.COURSE_REFERENCE_NUMBER,
        CASE  WHEN (REPLACE(GM.DISPLAY_TITLE,' ','') like '%part1%') THEN 'Week_1_Assignment_Part_1'
          WHEN (REPLACE(GM.DISPLAY_TITLE,' ','') like '%part2%') THEN 'Week_1_Assignment_Part_2'
        END as TITLE,
        CASE  WHEN (REPLACE(GM.DISPLAY_TITLE,' ','') like '%part1%') THEN  'Y'
          WHEN (REPLACE(GM.DISPLAY_TITLE,' ','') like '%part2%') THEN 'Y'
        END as Student_Participation_Ind,
        concat(CASE  WHEN (REPLACE(GM.DISPLAY_TITLE,' ','') like '%part1%') THEN 'Week_1_Assignment_Part_1'
          WHEN (REPLACE(GM.DISPLAY_TITLE,' ','') like '%part2%') THEN 'Week_1_Assignment_Part_2'
        END , '_Student_Participation_Indicator') as PARTICIPATION_IND_VALUES
    	from prc_aap_student_details sd

			inner join rpt_student_cdm.prc_lms_submissions_blogs_and_journals sp
			on sd.crsmain_pk1 = sp.crsmain_pk1 and sd.user_pk1 = sp.users_pk1 --and sp.bb_version = 'BB9WA'
				and (replace(sp.subject,' ','') like '%WEEK1%'  or replace(sp.subject,' ','') like '%WEEK2%'
					OR replace(area_name,' ','') like '%WEEK1%'  or replace(area_name,' ','') like '%WEEK2%' )

			inner join stg_l1_bb_bblearn.gradebook_main gm
				on sp.gm_pk1 = gm.pk1 and sp.crsmain_pk1 = gm.crsmain_pk1 --and sp.bb_version = gm.bb_version
				and (replace(gm.display_title,' ','') like '%part1%' or replace(gm.display_title,' ','') like '%part2%')


	  );
CREATE OR REPLACE TEMP TABLE src_tmp as(
		SELECT DISTINCT SD.STUDENT_ID,SD.CAMPUS_DESC,SD.TERM,SD.PART_OF_TERM,SD.COURSE_REFERENCE_NUMBER,
			CASE REPLACE(GM.DISPLAY_TITLE,' ','')
				WHEN 'Week1Discussion' THEN 'Week_1_Discussion'
				WHEN 'Week1AssignmentPart1' THEN 'Week_1_Assignment_Part_1'
				WHEN 'Week1AssignmentPart2' THEN 'Week_1_Assignment_Part_2'
				WHEN 'Week2Discussion' THEN 'Week_2_Discussion'
				--WHEN 'Week2Quiz' THEN 'Week 2 Quiz'
				WHEN 'Week2Assignment' THEN 'Week_2_Assignment'
			END AS TITLE,
			--GM.DISPLAY_TITLE as TITLE,
			GG.AVERAGE_SCORE,GM.POSSIBLE,
			concat(CASE REPLACE(GM.DISPLAY_TITLE,' ','')
				WHEN 'Week1Discussion' THEN 'Week_1_Discussion'
				--WHEN 'Week1Assignment' THEN 'Week 1 Assignment'
				WHEN 'Week1AssignmentPart1' THEN 'Week_1_Assignment_Part_1'
				WHEN 'Week1AssignmentPart2' THEN 'Week_1_Assignment Part_2'
				WHEN 'Week2Discussion' THEN 'Week_2_Discussion'
				--WHEN 'Week2Quiz' THEN 'Week 2 Quiz'
				WHEN 'Week2Assignment' THEN 'Week_2_Assignment'
			END , '_Possible Points') as POSSIBLE_POINTS,
			CASE WHEN REPLACE(GM.DISPLAY_TITLE,' ','') in ('Week1AssignmentPart1') AND (GL.MODIFIER_PK1 IS NOT NULL OR GG.AVERAGE_SCORE > 0) THEN 'Y'
				WHEN REPLACE(GM.DISPLAY_TITLE,' ','') in ('Week1AssignmentPart2') AND (GL.MODIFIER_PK1 IS NOT NULL OR GG.AVERAGE_SCORE > 0) THEN 'Y'
				WHEN REPLACE(GM.DISPLAY_TITLE,' ','') in ('Week2Assignment') AND (GL.MODIFIER_PK1 IS NOT NULL OR GG.AVERAGE_SCORE > 0) THEN 'Y'
				--WHEN REPLACE(GM.DISPLAY_TITLE,' ','') in ('Week1Discussion') AND (REPLACE(SP.SUBJECT,' ','') like '%WEEK1%') THEN 'Y'
				--WHEN REPLACE(GM.DISPLAY_TITLE,' ','') in ('Week2Discussion') AND (REPLACE(SP.SUBJECT,' ','') like '%WEEK2%') THEN 'Y'
				ELSE 'N'
			END as STUDENT_PARTICIPATION_IND,
			concat(CASE REPLACE(GM.DISPLAY_TITLE,' ','')
				WHEN 'Week1Discussion' THEN 'Week_1_Discussion'
				--WHEN 'Week1Assignment' THEN 'Week 1 Assignment'
				WHEN 'Week1AssignmentPart1' THEN 'Week_1_Assignment_Part_1'
				WHEN 'Week1AssignmentPart2' THEN 'Week_1_Assignment_Part_2'
				WHEN 'Week2Discussion' THEN 'Week_2_Discussion'
				--WHEN 'Week2Quiz' THEN 'Week 2 Quiz'
				WHEN 'Week2Assignment' THEN 'Week_2_Assignment'
			END , '_Student_Participation_Indicator') as PARTICIPATION_IND_VALUES
			--GM.DISPLAY_TITLE+' Possible Points' as POSSIBLE_POINTS

     from prc_aap_student_details sd
        inner join stg_l1_bb_bblearn.gradebook_main gm on gm.crsmain_pk1 = sd.crsmain_pk1 --and gm.bb_version = 'BB9WA'
    and gm.scorable_ind = 'Y' and gm.possible > 0 and gm.deleted_ind = 'N'
        left join stg_l1_bb_bblearn.gradebook_grade gg on gg.course_users_pk1 = sd.course_user_pk1 and gg.gradebook_main_pk1 = gm.pk1 --and gg.bb_version = gm.bb_version
        left join stg_l1_bb_bblearn.gradebook_log gl on gl.gradebook_main_pk1 = gm.pk1 --and gl.bb_version = gm.bb_version
    and gl.user_pk1 = sd.user_pk1 and gl.modifier_role = 's'

   	WHERE REPLACE(GM.DISPLAY_TITLE,' ','') in ('Week1Discussion','Week1AssignmentPart1','Week1AssignmentPart2','Week2Discussion'
			,'Week2Assignment')
			--and SD.COURSE_IDENTIFICATION like 'HMNT%'
			--and SD.STUDENT_ID = 'A00075739'
	);

CREATE OR REPLACE TEMP TABLE prc_aap_bb_gb_details_sd AS
(

	select s.student_id,s.campus_desc,s.term,s.part_of_term,s.course_reference_number,s.title,s.average_score,s.possible,s.possible_points,
		coalesce(sp.student_participation_ind,sj.student_participation_ind,s.student_participation_ind) student_participation_ind,s.participation_ind_values
	from src_tmp s
    left join participation_discussion sp on s.student_id = sp.student_id and s.campus_desc = sp.campus_desc
        and s.term = sp.term  and s.part_of_term = sp.part_of_term and s.course_reference_number = sp.course_reference_number
        and s.title = sp.title and s.participation_ind_values = sp.participation_ind_values

    left join participation_journals sj on s.student_id = sj.student_id and s.campus_desc = sj.campus_desc
        and s.term = sj.term  and s.part_of_term = sj.part_of_term and s.course_reference_number = sj.course_reference_number
        and s.title = sj.title and s.participation_ind_values = sj.participation_ind_values

  union all

  /* canvas data--*/
  Select
    split(user_sis_id, '.')[OFFSET(1)] as student_id,
    xref.institution as campus_desc,
    xref.academic_period as term,
    xref.sub_academic_period as part_of_term,
    xref.course_reference_number,
    a.title,
    agd.score as average_score,
    agd.points_possible as possible,
    concat(CASE REPLACE(a.title,' ','')
				WHEN 'Week1Discussion' THEN 'Week_1_Discussion'
				--WHEN 'Week1Assignment' THEN 'Week 1 Assignment'
				WHEN 'Week1AssignmentPart1' THEN 'Week_1_Assignment_Part_1'
				WHEN 'Week1AssignmentPart2' THEN 'Week_1_Assignment Part_2'
				WHEN 'Week2Discussion' THEN 'Week_2_Discussion'
				--WHEN 'Week2Quiz' THEN 'Week 2 Quiz'
				WHEN 'Week2Assignment' THEN 'Week_2_Assignment'
			END , '_Possible Points') as  possible_points,


CASE WHEN REPLACE(a.title,' ','') in ('Week1AssignmentPart1') AND (SCORE > 0) THEN 'Y'
				WHEN REPLACE(a.title,' ','') in ('Week1AssignmentPart2') AND (SCORE > 0) THEN 'Y'
				WHEN REPLACE(a.title,' ','') in ('Week2Assignment') AND (SCORE > 0) THEN 'Y'
				ELSE 'N'
			END as student_participation_ind

,concat(CASE REPLACE(a.title,' ','')
				WHEN 'Week1Discussion' THEN 'Week_1_Discussion'
				--WHEN 'Week1Assignment' THEN 'Week 1 Assignment'
				WHEN 'Week1AssignmentPart1' THEN 'Week_1_Assignment_Part_1'
				WHEN 'Week1AssignmentPart2' THEN 'Week_1_Assignment_Part_2'
				WHEN 'Week2Discussion' THEN 'Week_2_Discussion'
				--WHEN 'Week2Quiz' THEN 'Week 2 Quiz'
				WHEN 'Week2Assignment' THEN 'Week_2_Assignment'
			END , '_Student_Participation_Indicator') as participation_ind_values

/*case when agd.grading_type = 'not_graded' or agd.grading_type is null then 'N' else 'Y' end as scorable_ind

,xref.academic_period,
xref.sub_academic_period,
xref.course_reference_number,
agd.points_possible as possible,
u.user_sis_id,
a.title,
xref.institution,
agd.score */

    from `trans_lms.prc_wldn_assignment_grade_details` agd
	join `trans_lms.prc_wldn_assignment` a on agd.assignment_id = a.canvas_id and agd.course_id = a.course_id
    join `trans_lms.prc_wldn_user` u on agd.user_id = u.canvas_id
    join `trans_lms.prc_wldn_course_enrollment` ce on ce.user_id = agd.user_id and agd.course_id = ce.course_id
    join `trans_lms.prc_wldn_course` c on ce.course_id = c.canvas_id
    join `trans_academics.v_prc_bb_course_xlist_details` xref on xref.bb_course_id = c.sis_source_id
    where lower(type) like 'student%' and u.user_sis_id like '%.%'

);

CREATE OR REPLACE TEMP TABLE  pivot_title as(
    select student_id,campus_desc,term,part_of_term,course_reference_number,week_1_discussion,week_2_discussion,week_2_assignment,
		-- uncomment later
    week_1_assignment,
		week_1_assignment_part_1,week_1_assignment_part_2

  from prc_aap_bb_gb_details_sd
	pivot
		(sum(average_score) for title in ('week_1_discussion','week_2_discussion','week_2_assignment','week_1_assignment','week_1_assignment_part_1','week_1_assignment_part_2')
		) as pt1

);
CREATE OR REPLACE TEMP TABLE pivot_possible as(
    select student_id,campus_desc,term,part_of_term,course_reference_number
        week_1_discussion_possible_points,
        week_2_discussion_possible_points,
		week_2_assignment_possible_points,
		week_1_assignment_part_1_possible_points,
		week_1_assignment_part_2_possible_points
    from prc_aap_bb_gb_details_sd
		pivot
		(sum(possible) for possible_points in ('week_1_discussion_possible_points','week_2_discussion_possible_points',
		'week_2_assignment_possible_points','week_1_assignment_part_1_possible_points','week_1_assignment_part_2_possible_points')
		) as pt2

);
CREATE OR REPLACE TEMP TABLE pivot_student_participation_ind as(
    select student_id,campus_desc,term,part_of_term,course_reference_number,
		week_1_discussion_student_participation_indicator,
		week_2_discussion_student_participation_indicator,
		week_1_assignment_part_1_student_participation_indicator,
		week_1_assignment_part_2_student_participation_indicator,
		week_2_assignment_student_participation_indicator
-- 		from prc_aap_bb_gb_details_sd
    from prc_aap_bb_gb_details_sd
		pivot
		(max(student_participation_ind) for participation_ind_values in ('week_1_discussion_student_participation_indicator',
		'week_2_discussion_student_participation_indicator','week_1_assignment_part_1_student_participation_indicator',
		'week_1_assignment_part_2_student_participation_indicator','week_2_assignment_student_participation_indicator')
		) as pt3


);

CREATE OR REPLACE TEMP TABLE temp_Get_AAP_BB_GradableItems as
(
  select p1.*,-- uncomment later
	    p2.week_1_discussion_possible_points,
	    p2.week_2_discussion_possible_points,
		p2.week_2_assignment_possible_points,p2.week_1_assignment_part_1_possible_points,p2.week_1_assignment_part_2_possible_points ,p3.week_1_discussion_student_participation_indicator,p3.week_2_discussion_student_participation_indicator,
		p3.week_1_assignment_part_1_student_participation_indicator,p3.week_1_assignment_part_2_student_participation_indicator,
		p3.week_2_assignment_student_participation_indicator
    from pivot_title p1
  left join pivot_possible p2 on p1.student_id=p2.student_id
  left join pivot_student_participation_ind p3 on  p1.student_id=p3.student_id
 );

CREATE OR REPLACE TEMP TABLE prc_aap_bb_gradableitems as(
--select * from temp_Get_AAP_BB_GradableItems

select
		student_id,campus_desc,term,part_of_term,course_reference_number,
		-- uncomment later
		max(week_1_discussion_possible_points) week1_disc_possible,
		max(week_1_discussion) week1_disc_received,
		max(week_1_discussion_student_participation_indicator) week1_discussion_student_participation_ind,
		max(week_2_discussion_possible_points) week2_disc_possible,max(week_2_discussion) week2_disc_received,
		max(week_2_discussion_student_participation_indicator) week2_discussion_student_participation_ind,
		--max(week_2_quiz_possible_points) week2_quiz_possible,max(week_2_quiz) week2_quiz_received,
		max(week_1_assignment_part_1_possible_points) week1_assignment_part1_possible,
		max(week_1_assignment_part_1) week1_assignment_part1_received,
		max(week_1_assignment_part_2_possible_points) week1_assignment_part2_possible,
		max(week_1_assignment_part_2) week1_assignment_part2_received,
		max(week_1_assignment_part_1_student_participation_indicator) week1_assignment_part1_student_participation_ind,
		max(week_1_assignment_part_2_student_participation_indicator) week1_assignment_part2_student_participation_ind,
		max(week_2_assignment_possible_points) week2_assignment_possible,
		max(week_2_assignment) week2_assignment_received,
		max(week_2_assignment_student_participation_indicator) week2_assignment_student_participation_ind

  From temp_Get_AAP_BB_GradableItems s
  group by student_id,campus_desc,term,part_of_term,course_reference_number


);

CREATE OR REPLACE TEMP TABLE src AS
(
 select distinct sd.campus_desc,
  sd.studentcrse,
  sd.student_id,
  sd.student_last_name,
  sd.student_first_name,
  sd.start_date,
  sd.student_email,
  sd.state,
  sd.student_phone_number,
  sd.term,
  sd.part_of_term,
  sd.program,
  sd.course_code,
  sd.course_reference_number,
  sd.course_start_date,
  sd.course_end_date,
  sd.last_login_date,
  sd.student_type,
  sd.fta_flag,
  sd.fta_exempt_ind,
  sd.non_revenue_ind,
  sd.reasearch_forum_ind,
  sd.course_identification,
  sd.no_sub_flag,
  coalesce(bg.week1_disc_received,0) as disc1_pts_received,
  coalesce(cast(bg.week1_disc_possible as int64),60) as disc1_pts_possible,
  bg.week1_discussion_student_participation_ind,

  coalesce(bg.week1_assignment_part1_received,0) as assignment_part1_received,
  coalesce(bg.week1_assignment_part1_possible,60) as assignment_part1_possible,
  bg.week1_assignment_part1_student_participation_ind,
  coalesce(bg.week1_assignment_part2_received,0) as assignment_part2_received,
  coalesce(bg.week1_assignment_part2_possible,60) as assignment_part2_possible,
  bg.week1_assignment_part2_student_participation_ind,

  coalesce(bg.week2_disc_received,0) as disc2_pts_received,
  coalesce(bg.week2_disc_possible,60) as disc2_pts_possible,
  bg.week2_discussion_student_participation_ind,
  coalesce(bg.week2_assignment_received,0) as assignment2_pts_received,
  coalesce(bg.week2_assignment_possible,60) as assignment2_pts_possible,
  bg.week2_assignment_student_participation_ind,
  coalesce(bg.week1_disc_received,0) + coalesce(bg.week1_assignment_part1_received,0) + coalesce(bg.week1_assignment_part2_received,0)
  + coalesce(bg.week2_disc_received,0) + coalesce(bg.week2_assignment_received,0) as total_pts_received,
  coalesce(cast(bg.week1_disc_possible as int64),60) + coalesce(bg.week1_assignment_part1_possible,60) + coalesce(bg.week1_assignment_part2_possible,60)
  + coalesce(bg.week2_disc_possible,60) + coalesce(bg.week2_assignment_possible,60) as total_pts_possible,
  case
  when (coalesce(bg.week1_disc_received,0) + coalesce(bg.week1_assignment_part1_received,0) + coalesce(bg.week1_assignment_part2_received,0) + coalesce(bg.week2_disc_received,0) + coalesce(bg.week2_assignment_received,0))>=270 then 'A'
  when (coalesce(bg.week1_disc_received,0) + coalesce(bg.week1_assignment_part1_received,0) + coalesce(bg.week1_assignment_part2_received,0) + coalesce(bg.week2_disc_received,0) + coalesce(bg.week2_assignment_received,0)) >=240 then 'B'
  when (coalesce(bg.week1_disc_received,0) + coalesce(bg.week1_assignment_part1_received,0) + coalesce(bg.week1_assignment_part2_received,0) + coalesce(bg.week2_disc_received,0) + coalesce(bg.week2_assignment_received,0)) >=210 then 'C'
  when (coalesce(bg.week1_disc_received,0) + coalesce(bg.week1_assignment_part1_received,0) + coalesce(bg.week1_assignment_part2_received,0) + coalesce(bg.week2_disc_received,0) + coalesce(bg.week2_assignment_received,0)) >=180 then 'D'
  when sd.last_login_date < sd.course_start_date or sd.last_login_date is null then ''
  else 'Below D'
  end as grade_possible,
  sd.instructor_name,
  sd.ea_rep_name,
  sd.academic_advisor_name,
  sd.lms_indicator

  from prc_aap_student_details sd


  left join  prc_aap_bb_gradableitems   bg on sd.student_id = bg.student_id
  --left join #prc_aap_bb_gradableitems  bg on sd.student_id = bg.student_id
    and sd.term = bg.term
    and sd.part_of_term = bg.part_of_term
    and sd.campus_desc = bg.campus_desc
    and sd.course_reference_number = bg.course_reference_number

where sd.campus_desc = sd.course_campus
);

/*note- delete n insert using merge process */

CREATE OR REPLACE TEMP TABLE t_aap_report_temp --CLUSTER BY etl_pk_hash, pk_chg_hash
  AS (
       select src.*,
        job_start_dt as etl_created_date,
        job_start_dt as etl_updated_date,
        load_source as etl_resource_name,
        v_audit_key as etl_ins_audit_key,
        v_audit_key as etl_upd_audit_key,
        farm_fingerprint(format('%T',  (src))) as etl_pk_hash,
        farm_fingerprint(format('%T', src )) as etl_chg_hash,
		5 as institution_id,
		"WLDN" as institution,
        "WLDN_BNR" as source_system_name
        from src
);
       call utility.sp_process_elt (institution, dml_mode , target_dataset, target_tablename, null, source_tablename, additional_attributes, out_sql );

        set job_end_dt = current_timestamp();
        set job_completed_ind = 'Y';

        /* export success audit log record */
        call `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);

        set result = 'SUCCESS';

        EXCEPTION WHEN error THEN

        set job_end_dt = cast (NULL as TIMESTAMP);
        set job_completed_ind = 'N';

        call `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key, target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);

        /* insert into error_log table */
        insert into
        `audit_cdw_log.error_log` (error_load_key, process_name, table_name, error_details, etl_create_date, etl_resource_name, etl_ins_audit_key)
        values
         (v_audit_key,'EDW_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;


SET result =  @@error.message;

RAISE USING message = @@error.message;


    end;

end