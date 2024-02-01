CREATE OR REPLACE PROCEDURE tds_analytics_storage.sp_Walden_Total_Population()
BEGIN
Create or replace temp table Base_Population as(
    SELECT DISTINCT
      per.person_uid
      ,per.credential_id
      ,per.last_name
      ,per.first_name
      ,per.citizenship_desc
      ,per.birth_date
      ,per.gender
      ,per.ipeds_ethnicity
      ,per.entity_origin
      ,intl.international_flag
      ,eml.internet_address student_email
      ,eml2.internet_address personal_email
      ,tel.phone_number_combined home_phone
      ,tel2.phone_number_combined cell_phone
      ,addy.state_province
      ,addy.nation_iso_code
      ,addy.postal_code
      ,SSA.contactsfid
      ,SSA.aa_name
      ,SSA.aa_manager
    FROM `rpt_semantic.person` per
    LEFT JOIN `rpt_semantic.internet_address_current` eml
      on per.person_uid = eml.entity_uid
      and eml.internet_address_type = 'UNIV'
      and per.institution_id = eml.institution_id
    LEFT JOIN `rpt_semantic.internet_address_current` eml2
      on per.person_uid = eml2.entity_uid
      and eml2.internet_address_type = 'PERS'
      and per.institution_id = eml2.institution_id
    LEFT JOIN `rpt_semantic.telephone_current` tel
      on per.person_uid = tel.person_uid
      and tel.phone_type = 'HOME'
      and per.institution_id = tel.institution_id
    LEFT JOIN `rpt_semantic.telephone_current` tel2
      on per.person_uid = tel2.person_uid
      and tel2.phone_type = 'CELL'
      and per.institution_id = tel2.institution_id
    LEFT JOIN `rpt_semantic.address_current` addy
      on per.person_uid = addy.entity_uid
      and preferred_address_ind = 'Y'
      and per.institution_id = addy.institution_id
    LEFT JOIN `rpt_semantic.wldn_map_ea_aa` SSA
      on per.credential_id = SSA.id
    LEFT JOIN `raw_b2c_sfdc.contact` con
      on SSA.contactsfid = con.id and con.is_deleted =  false
    LEFT JOIN `rpt_semantic.address_international` intl
      on per.person_uid = intl.person_uid
      and per.institution_id = intl.institution_id
    where per.institution_id = 5
      and lower(per.last_name) not like 'zz%'
      and per.credential_id <> 'A00000001'
      and per.credential_id like 'A%'
      and entity_origin <> 'ULTIPRO'
);

--Per Max Lill (SSA), include courses with an F or W grade,
----exclude EDUC4025, WWOW/WSRO, and CAEX
--To determine if a course should be included, must know if
----that course end date would count towards returning within one year
--student level last course end date
--Retrieves the max course end and min course start dates per term
create or replace temp table Term_Level_Course_Dates as(
  Select DISTINCT person_uid
    ,academic_period
    ,max(end_date) over (partition by person_uid) last_crs_end
    ,max(end_date) over (partition by person_uid, academic_period) max_End
    ,min(start_date) over (partition by person_uid, academic_period) min_start
    ,count(distinct academic_period) over (partition by person_uid) count_period_person
    from `rpt_semantic.student_course`
  where (reg_status in ('RW','RE','A2','A3') OR reg_status like 'W%')
  and reg_status <> 'W1'
  and institution_id = 5
  and subject not in('WWOW','WSRO')
  and course_identification <> 'EDUC4025'
  and course_identification not like 'CAEX%'
  and lower(name) not like 'zz%'
);

---------------------------------------------------------------------------------------------
--We needed to bring in the min start/max end above at the term level here to associate step out date and step in dates with the academic period of WD
--PART 1
Create or replace temp table Academic_Study1 as (SELECT
    std.person_uid
    ,std.academic_period
    ,std.academic_period_desc
    ,rank() over (partition by std.person_uid, std.program order by std.academic_period) Academic_Period_Rank
    ,trm_course.max_end
    ,trm_course.min_start
    ,trm_course.count_period_person
    ,last_value(trm_course.max_end ignore nulls) over (partition by std.person_uid order by std.academic_period asc ROWS BETWEEN UNBOUNDED PRECEDING AND current row) Crs_End_Exclude_Nulls
    ,first_value(trm_course.min_start ignore nulls) over (partition by std.person_uid order by std.academic_period asc ROWS BETWEEN current row AND UNBOUNDED FOLLOWING) Crs_Start_Exclude_Nulls
    ,std.catalog_academic_period
    ,std.academic_period_admitted
    ,std.degree
    ,std.degree_desc
    ,std.department
    ,std.department_desc
    ,std.first_concentration
    ,std.first_concentration_desc
    ,std.major
    ,std.major_desc
    ,std.product_nbr
    ,std.program_classification_desc
    ,std.program
    ,std.program_desc
    ,std.college
    ,std.college_desc
    ,std.start_date
    ,std.start_date_admitted
    ,std.student_level
    ,std.student_level_desc
    ,std.student_classification_Desc
    ,std.student_status_desc
    ,std.student_population_desc
    ,std.primary_program_ind
    ,std.expected_graduation_date
    ,std.enrolled_ind
    ,std.current_time_status_desc
    FROM `rpt_semantic.academic_study` std
    LEFT JOIN Term_Level_Course_Dates trm_course
      on std.person_uid = trm_course.person_uid
      and std.academic_period = trm_course.academic_period
    where institution_id = 5
      and cast(std.academic_period as int) < 900000
      and lower(name) not like 'zz%'
      And credential_id <> 'A00000001'
);

--PART 2
create or replace temp table Academic_study as (Select distinct *
    ,case when count_period_person = 1 THEN Crs_End_Exclude_Nulls
      ELSE lead(Crs_End_Exclude_Nulls,1) over (partition by person_uid order by academic_period desc, Crs_End_Exclude_Nulls desc) END Step_Out_Date
    ,lag(Crs_Start_Exclude_Nulls,1) over (partition by person_uid order by academic_period desc, Crs_Start_Exclude_Nulls desc) Step_In_Date
  FROM Academic_Study1
);
---------------------------------------------------------------------------------------------
--Identifies students who have graduated
create or replace temp table Academic_Outcome as (SELECT
  person_uid
  ,academic_period_graduation
  ,program
  ,outcome_graduation_date
  FROM `rpt_semantic.academic_outcome`
    where graduated_ind = 'Y'
      and institution_id = 5
);

--Identifies students who have applied for graduation or are in their last course/term
create or replace temp table P1 as (SELECT
  person_uid
  ,grad_req_completed_term
  ,program
  ,'Y' P1_Flag
  FROM `rpt_semantic.academic_outcome`
    where status in('P1','P2','P3','P4')
      and institution_id = 5
);

--Student hold information
create or replace temp table Holds as (Select
  person_uid
  ,max(graduation_hold_ind) graduation_hold_ind
  ,max(registration_hold_ind) registration_hold_ind
  ,max(transcript_hold_ind) transcript_hold_ind
  ,max(active_hold_ind) active_hold_ind
  ,max(case when hold = 'AP' then 'Y' END) AP_Hold
  ,max(case when hold = 'PM' then 'Y' END) PM_Hold
  ,max(case when hold = 'DN' then 'Y' END) DN_Hold
  ,max(case when hold = 'LH' then 'Y' END) LH_Hold
  ,max(case when hold = 'HH' then 'Y' END) HH_Hold
  ,max(case when hold in ('BH','C2','C1','FS') then 'Y' END) Financial_Hold
  ,max(case when hold = 'CH' THEN 'Y' END) Contingency_Hold
  ,max(case when hold = 'PA' THEN 'Y' END) NonActivity_Hold
  ,max(case when hold = 'RH' THEN 'Y' END) Registrar_Hold
 FROM `rpt_semantic.hold`
 where cast(hold_to_date as date) > current_date
 and institution_id = 5
 GROUP BY person_uid
  --AP = Academic Progress
  --PM = Provisional Messaging
  --DN = Admissions Denial Hold
  --LH = Library Hold
  --HH = Hiatus Hold (LOA)
);

--Course level data
create or replace temp table Course as (select
  person_uid
  ,academic_period
  ,sub_academic_period
  ,sub_academic_period_desc
  ,course_identification
  ,course_reference_number
  ,course_level
  ,course_title_long
  ,course_title_short
  ,credits_earned
  ,credits_for_gpa
  ,end_date course_end
  ,start_date course_start
  ,final_grade
  ,last_attend_Date
  ,quality_points
  ,reg_status
  ,registration_status_desc
  ,registration_status_date
  ,college course_college
  ,college_desc course_college_desc
  ,department course_department
  ,department_desc course_department_desc
  from `rpt_semantic.student_course`
  where transfer_course_ind = 'N'
  and institution_id = 5
  and lower(name) not like 'zz%'
);

/*
--Per Max Lill (SSA), include courses with an F or W grade,
----exclude EDUC4025, WWOW/WSRO, and CAEX
--To determine if a course should be included, must know if
----that course end date would count towards returning within one year
--Student level last course end date
create or replace temp table Last_Crs as(Select person_uid,
  max(end_date) over (partition by person_uid) last_crs_end
  FROM  `rpt_semantic.student_course`
  where (reg_status in ('RW','RE','A2','A3') OR reg_status like 'W%')
  and reg_status <> 'W1'
  and institution_id = 5
  and subject not in('WWOW','WSRO')
  and course_identification <> 'EDUC4025'
  and course_identification not like 'CAEX%'
  and lower(name) not like 'zz%'
);
*/

--term level credits/gpa
create or replace temp table Credits_GPA as(SELECT distinct
  id
  ,program
  ,term
  ,cum_gpa
  ,gpa
  ,cum_credits_toc
  ,cum_credits_passed
  FROM `rpt_academics.v_wldn_gpa_prg_term`
  where institution_id = 5
);

--student level credits
create or replace temp table Institution_Credits as (SELECT maxS.szrdgpa_pidm
    ,cr.szrdgpa_over_hours_earned
    FROM(Select distinct szrdgpa_pidm
      ,max(cast(szrdgpa_seqno as integer)) max_seq
      FROM `raw_wldn_bnr.szrdgpa`
      GROUP BY szrdgpa_pidm) maxS
    LEFT JOIN `raw_wldn_bnr.szrdgpa` cr
      on maxS.szrdgpa_pidm = cr.szrdgpa_pidm
      and cast(maxS.max_seq as integer) = cast(cr.szrdgpa_seqno as integer)
);

--Last SF Opp Create Date
create or replace temp table max_SFopp as(SELECT DISTINCT
  Contact_id
  ,max(opp.created_date) over (partition by contact_id) max_create_date
  ,con.banner_id_c
  ,con.degree_plan_available_c
  FROM `raw_b2c_sfdc.opportunity` opp
  LEFT JOIN `raw_b2c_sfdc.contact` con
  on opp.contact_id = con.id
  where opp.institution_c = 'a0ko0000002BSH4AAO'
  and opp.is_deleted = false
);

create or replace temp table CTE as(Select distinct
    base.person_uid
      ,base.credential_id
      ,base.last_name
      ,base.first_name
      ,base.citizenship_desc
      ,base.birth_date
      ,base.gender
      ,base.ipeds_ethnicity
      ,base.entity_origin
      ,base.international_flag
      ,base.student_email
      ,base.personal_email
      ,base.home_phone
      ,base.cell_phone
      ,base.state_province
      ,base.nation_iso_code
      ,base.postal_code
      ,base.contactsfid
      ,base.aa_name
      ,base.aa_manager
      ,case when right(std.academic_period,2) in('10','30','50','70') THEN 'Quarter'
        When right(std.academic_period,2) in ('20','40','60') THEN 'Semester'
        END AP_Term_Type
      ,std.academic_period
      ,std.academic_period_desc
      ,std.Academic_Period_Rank
      ,std.min_start
      ,std.max_end
      ,std.catalog_academic_period
      ,std.academic_period_admitted
      ,std.degree
      ,std.degree_desc
      ,std.department
      ,std.department_desc
      ,std.first_concentration
      ,std.first_concentration_desc
      ,inst.szrdgpa_over_hours_earned Institution_Credits
      ,std.major
      ,std.major_desc
      ,std.product_nbr
      ,std.program_classification_desc
      ,std.program
      ,std.program_desc
      ,std.college
      ,std.college_desc
      ,std.start_date
      ,std.start_date_admitted
      ,std.student_level
      ,std.student_level_desc
      ,std.student_classification_Desc
      ,std.student_status_desc
      ,std.student_population_desc
      ,std.primary_program_ind
      ,std.expected_graduation_date
      ,std.enrolled_ind
      ,std.current_time_status_desc
      ,opp.max_create_date max_SFopp_create_date
      ,opp.degree_plan_available_c
      ,Last_Crs.last_crs_end
      ,outcome.outcome_graduation_date
      ,case when max(academic_period_graduation) over (partition by std.person_uid) < std.academic_period THEN 'Y' END Alumni_Flag
      --Filter to exclude the scenario where a student is registered and dropped in the term immediately following their graduation from the same program
      ,case when outcome_graduation_date is null
        and lead(outcome_graduation_date) over (partition by std.person_uid order by std.academic_period desc) is not null
        and product_nbr = lead(product_nbr) over (partition by std.person_uid order by std.academic_period desc) THEN 'Exclude' END FILTER
      ,P1.P1_Flag
      ,WD.withdrawal_code
      ,WD.withdrawal_desc
      ,WD.effective_date WD_Effective_Date
      ,WD_Reason_CD.shrttrm_wrsn_code WD_Reason_Cd
      ,WD_Reason_Desc.value_description WD_Reason_Desc
      ,case when WD.withdrawal_code is not null then std.Step_Out_Date END Step_Out_Date
      ,case when WD.withdrawal_code is not null then std.Step_In_Date END Step_In_Date
      ,case when WD.withdrawal_code is not null and std.Step_In_Date is not null THEN date_diff(std.Step_In_Date, std.Step_Out_Date, day) END Stepped_Out_Days
      ,case when WD.withdrawal_code is not null and std.Step_In_Date is null THEN date_diff(current_date(), std.Step_Out_Date, day) END Step_Out_Duration
      ,Credits_GPA.cum_gpa
      ,Credits_GPA.gpa
      ,Credits_GPA.cum_credits_toc
      ,Credits_GPA.cum_credits_passed
      ,hld.graduation_hold_ind
      ,hld.registration_hold_ind
      ,hld.transcript_hold_ind
      ,hld.active_hold_ind
      ,hld.AP_Hold
      ,hld.PM_Hold
      ,hld.DN_Hold
      ,hld.LH_Hold
      ,hld.HH_Hold
      ,hld.Financial_Hold
      ,hld.Contingency_Hold
      ,hld.NonActivity_Hold
      ,hld.Registrar_Hold
      ,crs.sub_academic_period
      ,crs.sub_academic_period_desc
      ,crs.course_identification
      ,crs.course_reference_number
      ,crs.course_level
      ,crs.course_title_long
      ,crs.course_title_short
      ,crs.credits_earned
      ,crs.credits_for_gpa
      ,crs.course_end
      ,crs.course_start
      ,crs.final_grade
      ,crs.last_attend_Date
      ,crs.quality_points
      ,crs.reg_status
      ,crs.registration_status_desc
      ,crs.registration_status_date
      ,crs.course_college
      ,crs.course_college_desc
      ,crs.course_department
      ,crs.course_department_desc
   FROM Academic_study std
   LEFT JOIN Base_Population base on
    std.person_uid = base.person_uid
  LEFT JOIN Academic_Outcome outcome
    on std.person_uid = outcome.person_uid
    and std.program = outcome.program
    and std.academic_period = outcome.academic_period_graduation
  LEFT JOIN P1
    on std.person_uid = P1.person_uid
    and std.program = p1.program
    and std.academic_period = P1.grad_req_completed_term
  LEFT JOIN `rpt_semantic.withdrawal` WD
    on std.person_uid = wd.person_uid
    and std.academic_period = wd.academic_period
    and WD.institution_id = 5
  LEFT JOIN `raw_wldn_bnr.shrttrm` WD_Reason_CD
    on std.person_uid = WD_Reason_CD.shrttrm_pidm
    and std.academic_period = WD_Reason_CD.shrttrm_term_code
  LEFT JOIN `rpt_semantic.withdraw_reason` WD_Reason_Desc
    on WD_Reason_CD.shrttrm_wrsn_code = WD_Reason_Desc.value
    and WD_Reason_Desc.institution_id = 5
  LEFT JOIN Holds hld
    on std.person_uid = hld.person_uid
  LEFT JOIN Course crs
    on std.person_uid = crs.person_uid
    and std.academic_period = crs.academic_period
  LEFT JOIN (
    Select DISTINCT person_uid,
    last_crs_end
    FROM Term_Level_Course_Dates) Last_Crs
    on std.person_uid = Last_Crs.person_uid
  LEFT JOIN Credits_GPA
    on base.credential_id = Credits_GPA.id
    and std.academic_period = Credits_GPA.term
    and std.program = Credits_GPA.program
  LEFT JOIN `raw_wldn_manualfiles.ser_prgm_budget` budget
    on std.program = budget.program_group
    and budget.instituition = 'WLDN'
  LEFT JOIN max_SFopp opp
    on base.credential_id = opp.banner_id_c
  LEFT JOIN Institution_Credits inst
    on std.person_uid = inst.szrdgpa_pidm
  where base.credential_id is not null
    and budget_code <> 'UVM'
);

--Excludes the aforementioned reg/drop/grad population
CREATE OR REPLACE TABLE tds_analytics_storage.Walden_Total_Population as(Select DISTINCT *
  FROM CTE where FILTER is null
);

END
