BEGIN
    declare institution string default 'WLDN';
    declare institution_id int64 default 5;
    declare dml_mode string default 'delete-insert';
    declare target_dataset string default 'rpt_crm_mart';
    declare target_tablename string default 't_wldn_total_student_report';
    declare source_tablename string default 'src_temp';
    declare load_source string default 'trans_crm_mart.sp_wldn_total_student_report';
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


    begin

      SET additional_attributes= [("audit_load_key", v_audit_key),
              ("load_method",load_method),
              ("load_source",load_source),
              ("job_type", job_type)];
      /* end common across */


create or replace temp table degreed_total as(

SELECT distinct vas.credential_id,
       vas.person_uid,
       vas.STUDENT_STATUS_DESC AS STUDENT_STATUS,
       rec.program_cd as PROGRAM,--rec.PROGRAM_NAME PROGRAM, not in rec praveena
       'Course-Based Degreed' AS STUDENT_TYPE,
       vas.STUDENT_POPULATION_DESC,
       lp.PHONE_NUMBER AS HOME_PHONE,
       vas.START_DATE AS PROGRAM_START_DATE,
       vas.FIRST_CONCENTRATION_DESC AS CONCENTRATION,
       vas.DEPARTMENT_DESC AS SCHOOL,
       vas.COLLEGE_DESC AS COLLEGE,
       CASE
           WHEN vas.STUDENT_LEVEL IN ( 'CS', 'SQ', 'SS', 'GQ', 'GS' ) THEN
               'Masters/Cert'
           WHEN vas.STUDENT_LEVEL IN ( 'DQ', 'DS' ) THEN
               'Doctoral'
           WHEN vas.STUDENT_LEVEL IN ( 'UQ', 'US' ) THEN
               'Undergraduate'
           ELSE
               'ERROR'
       END AS DEGREE_LEVEL,
       case when (substr(vas.academic_period, 5,2) in ('10','30','50','70'))
	        then 'Quarter'
            when (substring(vas.academic_period, 5,2) in('20','22','40','60','80'))
            then 'Semester'
            ELSE
               'Error'
       END AS TERM_TYPE


FROM `rpt_academics.t_academic_study` vas
    --LEFT JOIN `daas-cdw-prod.raw_wldn_manualfiles.bi_reconcile_list` rec--BI_Analytics_DM.dbo.Walden_Reconciled_List

     LEFT JOIN `rpt_academics.t_wldn_reconcile_list` rec
        ON vas.credential_id = rec.applicant_id
            AND safe_CAST(LEFT(CAST(vas.START_DATE as string),10) as date) = safe_CAST(LEFT(CAST(rec.student_start_date as string),10) as date)
            and vas.institution_id=5
    LEFT JOIN `rpt_academics.t_academic_outcome` vao
        ON vas.credential_id = vao.credential_id
           AND vas.PROGRAM = vao.PROGRAM
           and vao.institution_id=5
           AND
           (
               (
                   vao.STATUS_DESC = 'Awarded'
                   AND vao.GRADUATED_IND = 'Y'
               )
               OR vao.STATUS_DESC IN ( 'Grad App Rcd or Final Course', 'Thesis or Diss Complete',
                                       'Unconfirmed CAPP Requirements'
                                     )
           )
    LEFT JOIN `rpt_academics.v_telephone_current` lp-- SMart.dbo.lk_phone lp
        ON --vas.credential_id = lp.credential_id
            --and lp.phone_type='business'
            vas.person_uid = lp.person_uid
           AND lp.PHONE_TYPE = 'HOME'
           and lp.institution_id=5
WHERE vao.credential_id IS NULL
      AND vas.PRIMARY_PROGRAM_IND = 'Y'
      AND
      (
          (
              vas.START_DATE > '2010-01-01'
              AND vas.STUDENT_STATUS = 'IS'
              AND rec.applicant_id IS NOT NULL
          )
          OR vas.STUDENT_STATUS = 'AS'
      )
      AND vas.ACADEMIC_PERIOD =
      (
          SELECT MAX(v.ACADEMIC_PERIOD)
          FROM `rpt_academics.t_academic_study` v
              LEFT JOIN `rpt_academics.v_term` lt
                  ON v.ACADEMIC_PERIOD = lt.academic_period
                  and v.institution_id=5
                  and lt.institution_id=5
          WHERE v.credential_id = vas.credential_id
                AND v.PRIMARY_PROGRAM_IND = 'Y'
                AND lt.start_date < CURRENT_date()
      )
      AND LEFT(vas.credential_id, 1) = 'A'
      and   (vas.degree not in ('NDEG','00000') or vas.degree is NULL)

                  and vas.academic_period >= '198710'


);

create or replace temp table wd2_d as(

-- with wd_d as(
--     SELECT dt.credential_id,
--         dpsw.person_uid ,
--         MAX(dpsw.Effective_Date) AS WD_Date
--     FROM degreed_total dt
--         LEFT JOIN `rpt_academics.v_withdrawal` dpsw
--             ON dt.person_uid = dpsw.person_uid
--             and dpsw.institution_id = 5
--             WHERE dt.STUDENT_STATUS = 'Inactive'
--     GROUP BY dt.credential_id,dpsw.person_uid

-- )

SELECT wd.credential_id,
       wd.WD_Date,
       countif(dpsw.Withdrawal_Code = 'AW' ) AS AW_WDs,
       countif(dpsw.Withdrawal_Code = 'SW' ) AS SW_WDs,
       countif(dpwr.value_description = 'Academic Performance' ) AS AW_Academic_Performance,
       countif(dpwr.value_description = 'FTA' ) AS AW_FTA,
       countif(dpwr.value_description = 'Failure to Register' ) AS AW_Failure_to_Register,
       countif(dpwr.value_description = 'Financial Suspension' ) AS AW_Financial_Suspension,
       countif(dpwr.value_description = 'Failure to Return from LOA' ) AS AW_Failure_to_Return_from_LOA,
       countif(dpwr.value_description = 'Academic Conduct' ) AS AW_Academic_Conduct,
       countif(dpwr.value_description = 'Write Off' ) AS AW_Write_Off,
       countif(dpwr.value_description = 'Deceased' ) AS AW_Deceased,
       countif(dpwr.value_description = 'Inactive Student' ) AS AW_Inactive_Student,
       countif(dpwr.value_description = 'Never Enrolled' ) AS AW_Never_Enrolled,
       countif(dpwr.value_description = 'Failure Complt Doc Wrtng Req' ) AS AW_Failure_Complt_Doc_Wrtng_Req,
       countif(dpwr.value_description = 'Financial Aid' ) AS AW_Financial_Aid

    FROM (
        SELECT dt.credential_id,
            dpsw.person_uid ,
            MAX(dpsw.Effective_Date) AS WD_Date
        FROM degreed_total dt
            LEFT JOIN `rpt_academics.v_withdrawal` dpsw
                ON dt.person_uid = dpsw.person_uid
                and dpsw.institution_id = 5
                WHERE dt.STUDENT_STATUS = 'Inactive'
            GROUP BY dt.credential_id,dpsw.person_uid
      ) wd
        LEFT JOIN `rpt_academics.v_withdrawal` dpsw
            ON wd.person_uid = dpsw.person_uid
            and dpsw.Institution_id = 5
            AND wd.WD_Date = dpsw.Effective_Date
        LEFT JOIN `rpt_academics.v_withdraw_reason` dpwr         ON dpsw.Withdrawal_Code = dpwr.value and dpwr.institution_id=5
    GROUP BY wd.credential_id,wd.WD_Date

);

create or replace temp table  DEGREED_all as
(
    with  mr_reg_status_d as(
        SELECT dt.credential_id,
        countif(vsc.REGISTRATION_STATUS IN ( 'RE', 'RW', 'AU' ) ) AS REGISTRATIONS,
        countif( vsc.REGISTRATION_STATUS IN ( 'DN', 'DD', 'DU', 'DW', 'W1' ) ) AS DROPS,
        countif( vsc.REGISTRATION_STATUS IN ( 'DC', 'DR', 'W2', 'W3', 'WD', 'WI', 'WM', 'WN', 'WO', 'WT' )) AS WDS,
        MAX(vsc.REGISTRATION_STATUS_DATE) AS Most_Recent_Reg_Status_Date

    FROM degreed_total dt
        LEFT JOIN `rpt_academics.t_student_course` vsc
            ON dt.credential_id = vsc.credential_id
            and vsc.institution_id=5
    WHERE vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'
    GROUP BY dt.credential_id
    ),
    -- , mr_reg_status2_d as(
    -- SELECT mrs.credential_id,
    --     mrs.Most_Recent_Reg_Status_Date,
    --     CASE
    --         WHEN mrs.REGISTRATIONS > 0 THEN
    --             'RE'
    --         WHEN mrs.WDS > 0 THEN
    --             'WD'
    --         WHEN mrs.DROPS > 0 THEN
    --             'DD'
    --     END AS REGISTRATION_STATUS
    -- FROM mr_reg_status_d mrs
    --     LEFT JOIN rpt_academics.t_student_course vsc
    --         ON mrs.credential_id = vsc.credential_id
    --         AND mrs.Most_Recent_Reg_Status_Date = vsc.REGISTRATION_STATUS_DATE
    --         and vsc.institution_id=5
    -- WHERE vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'
    -- ),
    -- mr_reg_end_d as(
    --     SELECT dt.credential_id,
    --     MAX(vsc.END_DATE) AS Most_Recent_Reg_Course_End_Date

    -- FROM degreed_total dt
    --     LEFT JOIN rpt_academics.t_student_course vsc
    --         ON dt.credential_id = vsc.credential_id
    --         and vsc.institution_id=5
    -- WHERE vsc.REGISTRATION_STATUS IN ( 'RE', 'RW' )
    --     AND vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'
    -- GROUP BY dt.credential_id
    -- ),

    holds_d as(
        SELECT dt.credential_id,
            countif(lh.HOLD = 'HH' ) AS LOA_COUNT,
            MAX(lh.HOLD_TO_DATE) AS MR_LOA_END,
            countif(lh.HOLD IN ( 'BH', 'C2', 'C1', 'FS' ) ) AS FINANCIAL_HOLD_COUNT,
            countif(lh.HOLD = 'CH' ) AS CONTINGENCY_HOLD_COUNT,
            countif(lh.HOLD = 'PA' ) AS NON_ACTIVITY_HOLD_COUNT,
            countif(lh.HOLD = 'RH' ) AS REGISTRAR_HOLD_COUNT,
            countif(lh.REGISTRATION_HOLD_IND = 'Y' ) AS REGISTRATION_HOLDS,
            countif(lh.TRANSCRIPT_HOLD_IND = 'Y' ) AS TRANSCRIPT_HOLDS,
            countif(lh.GRADUATION_HOLD_IND = 'Y' ) AS GRADUATION_HOLDS
        FROM degreed_total dt
        LEFT JOIN rpt_academics.v_hold lh--SMart.dbo.lk_hold lh
            ON dt.credential_id = lh.credential_id
            and lh.institution_id=5
           AND current_datetime()
           BETWEEN cast(lh.HOLD_FROM_DATE as datetime) AND cast(lh.HOLD_TO_DATE as datetime)
        LEFT JOIN rpt_academics.v_hold loa
            ON dt.credential_id = loa.credential_id
            and loa.institution_id=5
            AND current_datetime()
            BETWEEN cast(loa.HOLD_FROM_DATE as datetime) AND cast(loa.HOLD_TO_DATE as datetime)
            AND loa.HOLD = 'HH'


GROUP BY dt.credential_id

    ),
    -- alumni_d as(
    --     SELECT DISTINCT
    --     dt.credential_id
    -- FROM degreed_total dt
    --     LEFT JOIN `rpt_academics.t_academic_outcome` vao
    --         ON dt.credential_id = vao.credential_id
    --         and vao.institution_id=5
    -- WHERE dt.PROGRAM != vao.PROGRAM --uncomment later
    --     AND
    --     (
    --         (
    --             vao.STATUS_DESC = 'Awarded'
    --             AND vao.GRADUATED_IND = 'Y'
    --         )
    --         OR vao.STATUS_DESC IN ( 'Grad App Rcd or Final Course', 'Thesis or Diss Complete',
    --                                 'Unconfirmed CAPP Requirements'
    --                                 )
    --     )
    -- ),

    -- gpa_d as(
    --         SELECT dt.credential_id,
    --         dt.PROGRAM,
    --         MAX(gpa.term) AS MR_GPA_Term
    --     FROM degreed_total dt
    --         LEFT JOIN `rpt_academics.v_wldn_gpa_prg_term` gpa
    --             ON dt.credential_id = gpa.ID
    --             AND dt.PROGRAM = gpa.PROGRAM
    --     GROUP BY dt.credential_id,
    --             dt.PROGRAM
    -- ),

    gpa2_d as(
        SELECT dt.credential_id,
        cum.cum_gpa,
        CASE
            WHEN cum.cum_gpa >= 3.5 THEN
                '3.50 +'
            WHEN cum.cum_gpa
                    BETWEEN 3.00 AND 3.49 THEN
                '3.00_3.49'
            WHEN cum.cum_gpa
                    BETWEEN 2.70 AND 2.99 THEN
                '2.70_2.99'
            WHEN cum.cum_gpa
                    BETWEEN 2.50 AND 2.69 THEN
                '2.50_2.69'
            WHEN cum.cum_gpa
                    BETWEEN 1.90 AND 2.49 THEN
                '1.90_2.49'
            WHEN cum.cum_gpa < 1.90 THEN
                '1.90_'
        END AS cum_gpa_range,
        cum.cum_credits_passed,
        cum.cum_credits_toc
    FROM degreed_total dt
        LEFT JOIN (
            SELECT dt.credential_id,
                dt.PROGRAM,
                MAX(gpa.term) AS MR_GPA_Term
            FROM degreed_total dt
            LEFT JOIN `rpt_academics.v_wldn_gpa_prg_term` gpa
                ON dt.credential_id = gpa.ID
                AND dt.PROGRAM = gpa.PROGRAM
            GROUP BY dt.credential_id,
                dt.PROGRAM
        ) g
            ON dt.credential_id = g.credential_id
        LEFT JOIN `rpt_academics.v_wldn_gpa_prg_term` cum
            ON g.credential_id = cum.ID
            AND g.PROGRAM = cum.PROGRAM
            AND g.MR_GPA_Term = cum.term
    )
    ,
    financial_d as(
        SELECT dt.credential_id,
        fin.NEW_MILESTONE FIN_AID_STATUS,
        fin.TERM
    FROM degreed_total dt
       -- LEFT JOIN BI_Analytics_DM.dbo.fa_data fin praveena: table not found in gcp
        LEFT JOIN `trans_academics.prc_rrt_fa_data` fin
            ON dt.credential_id = fin.ALTERNATE_ID
           -- AND fin.STATUS NOT IN ( '50', '51' )
        --Que to praveena: status column not found  in prc_rrt_fa_data, prc_rrt_fa_data not in preprod or prod. Praveena's comment: will get back

    WHERE CURRENT_DATETIME()<= fin.TERM_END_DATE
    ),
    -- opportunity_d as(
    --         SELECT dt.credential_id,
    --         MAX(op.Start_Date_c) AS MR_Opp_Start_Date
    --     FROM degreed_total dt
    --         LEFT JOIN `raw_b2c_sfdc.opportunity` op
    --             ON dt.credential_id = op.banner_id_c
    --             AND op.primary_flag_c = true
    --     GROUP BY dt.credential_id
    -- ),

    field_d as(
        SELECT dt.credential_id,
            SUM(vsc.CREDITS_PASSED) AS FIELD_CREDITS
        FROM degreed_total dt
            LEFT JOIN `rpt_academics.t_student_course` vsc
                ON dt.credential_id = vsc.credential_id
                and vsc.institution_id=5
                AND vsc.START_DATE >= cast(dt.PROGRAM_START_DATE as date)
                AND vsc.COURSE_IDENTIFICATION IN ( 'EDUC3052', 'EDUC3053', 'EDUC3054', 'EDUC3055', 'EDUC3056', 'EDUC4010',
                                                    'EDUC4020', 'EDUC4030', 'EDUC6648', 'EDUC6801E', 'EDUC6802E',
                                                    'EDUC6803E', 'EDUC6804E', 'EDUC6805E', 'EDUC6806E', 'EDUC7801',
                                                    'EDUC7802', 'EDUC7803', 'EDUC7804', 'EDUC7805', 'EDUC7806', 'SOCW6500',
                                                    'SOCW6510', 'SOCW6520', 'SOCW6530', 'SWLB651', 'SWLB0652A', 'SWLB652',
                                                    'SCLB651', 'SCLB652', 'SCLB0652A', 'GRMP6100', 'GRTA6100', 'GRTP6100',
                                                    'GRHP6100', 'GRHA6100', 'GRWP6100', 'GRFA6100', 'CPLB601L', 'CPLB602L',
                                                    'CPLB802L', 'CPLB803L', 'FPLB631L', 'FPLB632L', 'SPLB671L', 'SPLB672L',
                                                    'COUN6401S', 'COUN6500S', 'COUN6501S', 'COUN6671', 'COUN6682A',
                                                    'COUN6682B', 'COUN8890', 'COUN8895', 'COUN8896', 'CPSY6700', 'CPSY6810',
                                                    'CPSY6910', 'PSYC8281C', 'PSYC8283C', 'PSYC8284C', 'PSYC8285C',
                                                    'PSYC8292L', 'PSYC8293L', 'PSYC8294L', 'PSYC8295L', 'PSYC8920T',
                                                    'PSYC8920R', 'NRSE6600', 'NUNP6531A', 'NUNP6531C', 'NUNP6531F',
                                                    'NUNP6531N', 'NUNP6540A', 'NUNP6540C', 'NUNP6540F', 'NUNP6540N',
                                                    'NUNP6541A', 'NUNP6541C', 'NUNP6541F', 'NUNP6541N', 'NUNP6550A',
                                                    'NUNP6550C', 'NUNP6550F', 'NUNP6550N', 'NUNP6551A', 'NUNP6551C',
                                                    'NUNP6551F', 'NUNP6551N', 'NUNP6560C', 'NUNP6560F', 'NUNP6560N',
                                                    'NUNP6565A', 'NUNP6565C', 'NUNP6565F', 'NUNP6565N', 'NUNP6640A',
                                                    'NUNP6640C', 'NUNP6640F', 'NUNP6640N', 'NUNP6650A', 'NUNP6650C',
                                                    'NUNP6650F', 'NUNP6650N', 'NUNP6660A', 'NUNP6660C', 'NUNP6660F',
                                                    'NUNP6660N', 'NUNP6670C', 'NUNP6670F', 'NUNP6670N', 'NURS4210',
                                                    'NURS4220', 'NURS6341', 'NURS6351', 'NURS6431', 'NURS6531C', 'NURS6531N',
                                                    'NURS6540N', 'NURS6541N', 'NURS6550C', 'NURS6550N', 'NURS6551A',
                                                    'NURS6551C', 'NURS6551D', 'NURS6551N', 'NURS6565N', 'NURS6640N',
                                                    'NURS6650C', 'NURS6650N', 'NURS6670C', 'NURS6670N', 'NURS6720',
                                                    'NURS6730', 'NURS8400', 'NURS8410', 'NURS8500', 'NURS8510', 'NURS8600',
                                                    'PRAC6566', 'PUBH6638', 'PUBH6639', 'PUBH6640', 'PRAC6531', 'PRAC6531A',
                                                    'PRAC6531C', 'PRAC6531F', 'PRAC6531K', 'PRAC6531S', 'PRAC6531W',
                                                    'PRAC6540', 'PRAC6540A', 'PRAC6540C', 'PRAC6540F', 'PRAC6540K',
                                                    'PRAC6540S', 'PRAC6540W', 'PRAC6541', 'PRAC6541A', 'PRAC6541C',
                                                    'PRAC6541F', 'PRAC6541K', 'PRAC6541S', 'PRAC6541W', 'PRAC6550',
                                                    'PRAC6550A', 'PRAC6550C', 'PRAC6550F', 'PRAC6550K', 'PRAC6550S',
                                                    'PRAC6550W', 'PRAC6552', 'PRAC6552A', 'PRAC6552C', 'PRAC6552F',
                                                    'PRAC6552K', 'PRAC6552S', 'PRAC6552W', 'PRAC6560', 'PRAC6560A',
                                                    'PRAC6560C', 'PRAC6560F', 'PRAC6560K', 'PRAC6560S', 'PRAC6560W',
                                                    'PRAC6565', 'PRAC6565A', 'PRAC6565C', 'PRAC6565F', 'PRAC6565K',
                                                    'PRAC6565S', 'PRAC6565W', 'PRAC6566A', 'PRAC6566C', 'PRAC6566F',
                                                    'PRAC6566K', 'PRAC6566S', 'PRAC6566W', 'PRAC6568', 'PRAC6568A',
                                                    'PRAC6568C', 'PRAC6568K', 'PRAC6568S', 'PRAC6568W', 'PRAC6640',
                                                    'PRAC6640A', 'PRAC6640C', 'PRAC6640F', 'PRAC6640K', 'PRAC6640S',
                                                    'PRAC6640W', 'PRAC6650', 'PRAC6650A', 'PRAC6650C', 'PRAC6650F',
                                                    'PRAC6650K', 'PRAC6650S', 'PRAC6650W', 'PRAC6660', 'PRAC6660A',
                                                    'PRAC6660C', 'PRAC6660F', 'PRAC6660K', 'PRAC6660S', 'PRAC6660W',
                                                    'PRAC6670', 'PRAC6670A', 'PRAC6670C', 'PRAC6670F', 'PRAC6670K',
                                                    'PRAC6670S', 'PRAC6670W', 'COUN6401D', 'COUN6500D', 'COUN6501D',
                                                    'COUN6771', 'COUN6771D', 'COUN6782A', 'COUN6782B', 'COUN6782D',
                                                    'COUN6782E', 'COUN8320', 'COUN8682A', 'COUN8682B', 'CPSY8284C',
                                                    'CPSY8285C', 'CPSY8290', 'CPSY8291', 'CPSY8292', 'CPSY8293', 'CPSY8294',
                                                    'CPSY8295', 'FPSY6915', 'NRSE6600A', 'NRSE6600C', 'NRSE6600M',
                                                    'NRSE6600W', 'NURS6341A', 'NURS6341C', 'NURS6341M', 'NURS6341S',
                                                    'NURS6341W', 'NURS6351A', 'NURS6351C', 'NURS6351M', 'NURS6351W',
                                                    'NURS6431A', 'NURS6431C', 'NURS6431M', 'NURS6431W', 'NURS6600',
                                                    'NURS6600A', 'NURS6600C', 'NURS6600M', 'NURS6600S', 'NURS6600W',
                                                    'NURS6720A', 'NURS6720C', 'NURS6720S', 'NURS6730A', 'NURS6730C',
                                                    'NURS6730S', 'NURS8400A', 'NURS8400M', 'NURS8400W', 'NURS8410A',
                                                    'NURS8410M', 'NURS8410S', 'NURS8410W', 'NURS8500A', 'NURS8500C',
                                                    'NURS8500M', 'NURS8500W', 'NURS8510A', 'NURS8510C', 'NURS8510M',
                                                    'NURS8510W', 'NURS8600A', 'NURS8600C', 'NURS8600M', 'NURS8600S',
                                                    'NURS8600W', 'PUBH6635', 'PUBH6638G', 'PUBH6639G', 'PUBH8635',
                                                    'PUBH8990', 'SOCW4100', 'SOCW4110'
                                                    )
        GROUP BY dt.credential_id

    ),

    -- future_reg_d as(

    --     SELECT dt.credential_id,
    --         COUNT(DISTINCT (vsc.COURSE_IDENTIFICATION)) FUTURE_REG_COUNT
    --     FROM  degreed_total dt
    --         LEFT JOIN `rpt_academics.t_student_course` vsc
    --             ON dt.credential_id = vsc.credential_id
    --             and vsc.institution_id=5
    --     WHERE vsc.REGISTRATION_STATUS IN ( 'RE', 'RW' )
    --         AND vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'
    --         AND vsc.START_DATE > current_date()
    --     GROUP BY dt.credential_id


    -- ),

    -- current_reg_d as(
    --     SELECT dt.credential_id,
    --         COUNT(DISTINCT (vsc.COURSE_IDENTIFICATION)) CURRENT_REG_COUNT

    --     FROM degreed_total dt
    --         LEFT JOIN `rpt_academics.t_student_course` vsc
    --             ON dt.credential_id = vsc.credential_id
    --             and vsc.institution_id=5
    --     WHERE vsc.reg_status IN ( 'RE', 'RW' )
    --         AND vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'
    --         AND current_date()
    --         BETWEEN vsc.START_DATE AND vsc.END_DATE --uncomment later
    --     GROUP BY dt.credential_id
    -- ),

    historical_fin_aid_d as(
        SELECT dt.credential_id

        FROM degreed_total dt
            INNER JOIN `trans_academics.v_lk_powerfaids` pf --SMart.dbo.lk_PowerFaids pf
                ON dt.credential_id = pf.alternate_id
    )
    -- max_etl_date_age as(
    --      SELECT MAX(etl_created_date) as max_etl_created_date from
    --            rpt_student_finance.t_wldn_aging
    -- )

    SELECT DISTINCT
       dt.credential_id as Student_ID,
       c.Id Student_Salesforce_Contact_ID,
       dt.STUDENT_STATUS Student_Status,
       dt.STUDENT_TYPE Student_Modality,
       dt.DEGREE_LEVEL Degree_Level,
       dt.PROGRAM Program,
       dt.STUDENT_POPULATION_DESC Student_Population,
       dt.HOME_PHONE Home_Phone,
       cast(dt.PROGRAM_START_DATE as date) Program_Start_Date,
       dt.CONCENTRATION Concentration,
       dt.SCHOOL School,
       dt.COLLEGE College,
       dt.TERM_TYPE Term_Type,
       wd2.WD_Date AS Withdrawal_Date,
       CASE
           WHEN wd2.AW_WDs > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Administrative_Withdrawal_AW,
       CASE
           WHEN wd2.AW_Academic_Performance > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Academic_Performance,
       CASE
           WHEN wd2.AW_FTA > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_FTA,
       CASE
           WHEN wd2.AW_Failure_to_Register > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Failure_to_Register,
       CASE
           WHEN wd2.AW_Financial_Suspension > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Financial_Suspension,
       CASE
           WHEN wd2.AW_Failure_to_Return_from_LOA > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Failure_to_Return_from_LOA,
       CASE
           WHEN wd2.AW_Academic_Conduct > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Academic_Conduct,
       CASE
           WHEN wd2.AW_Write_Off > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Write_Off,
       CASE
           WHEN wd2.AW_Deceased > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Deceased,
       CASE
           WHEN wd2.AW_Inactive_Student > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Inactive_Student,
       CASE
           WHEN wd2.AW_Never_Enrolled > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Never_Enrolled,
       CASE
           WHEN wd2.AW_Failure_Complt_Doc_Wrtng_Req > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Failure_Complt_Doc_Wrtng_Req,
       CASE
           WHEN wd2.AW_Financial_Aid > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Financial_Aid,
       CASE
           WHEN wd2.SW_WDs > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Student_Withdrawal,
       mrs2.Most_Recent_Reg_Status_Date Most_Recent_Registration_Status_Change_Date,
       mrs2.REGISTRATION_STATUS Most_Recent_Registration_Status,
       mre.Most_Recent_Reg_Course_End_Date Most_Recent_Registered_Course_End_Date,
       CASE
           WHEN h.CONTINGENCY_HOLD_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Contingency_Hold,
       CASE
           WHEN h.FINANCIAL_HOLD_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Financial_Hold,
       CASE
           WHEN h.GRADUATION_HOLDS > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Graduation_Hold,
       CASE
           WHEN h.LOA_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS On_LOA,
       h.MR_LOA_END AS Most_Recent_LOA_End_Date,
       CASE
           WHEN h.NON_ACTIVITY_HOLD_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Non_Activity_Hold,
       CASE
           WHEN h.REGISTRAR_HOLD_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Registrar_Hold,
       CASE
           WHEN h.REGISTRATION_HOLDS > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Registration_Hold,
       CASE
           WHEN h.TRANSCRIPT_HOLDS > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Transcript_Hold,
       CASE
           WHEN a.credential_id IS NOT NULL THEN
               'Yes'
           ELSE
               'No'
       END AS Alumni,
       g2.cum_credits_passed Cumulative_Passed_Credits,
       g2.cum_credits_toc Cumulative_TOC,
       g2.cum_gpa Cumulative_GPA,
       g2.cum_gpa_range Cumulative_GPA_Range,
       age.CURRENT_AMOUNT_DUE Current_Balance,
       f.FIN_AID_STATUS Current_Financial_Aid_Status,
       f.TERM AS Current_Financial_Aid_Term,
       o.MR_Opp_Start_Date Most_Recent_Opportunity_Start_Date,
       lp.FIRST_NAME,
       lp.LAST_NAME,
       lea.AA_Name,
       lea.AA_Manager,
       lea.AA_Location,
       lea.EA_Name_Orig,
       u.Name as EA_Manager_Orig,
       lea.EA_Location_Orig,
       le_p.internet_address as Personal_Email,
       le_w.internet_address as Walden_Email,
      --  case when le_p.internet_address_type = 'PERS' then internet_address end as Personal_Email,
      --  case when le_p.internet_address_type = 'UNIV' then internet_address end as Walden_Email,
       upper(addr.STREET_LINE1) as STREET_LINE1,
       addr.STREET_LINE2,
       addr.STREET_LINE3,
       upper(addr.CITY) CITY,
       addr.STATE_PROVINCE,
       addr.POSTAL_CODE,
       addr.NATION_DESC,
       CASE
	    WHEN mailing_country ='UNITED STATES' THEN 'US CONTINENT'
	    WHEN mailing_country ='APO' THEN 'US MILITARY BASES'
	    WHEN mailing_country IN ( 'AMERICAN SAMOA', 'NORTHERN MARIANA ISLANDS', 'U.S. VIRGIN ISLANDS', 'GUAM', 'PUERTO RICO' ) THEN 'US TERRITORIES'
	    WHEN mailing_country IS NULL THEN NULL
	  ELSE
	  'NON US'
	END as residency,
       fi.FIELD_CREDITS AS Field_Course_Credits_Passed,
       CASE
           WHEN frd.FUTURE_REG_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Future_Registrations,
       CAST(crd.CURRENT_REG_COUNT AS string) AS Current_Registrations,
       CASE
           WHEN hfa.credential_id IS NOT NULL THEN
               'Yes'
           ELSE
               'No'
       END AS Ever_Used_Financial_Aid_at_Walden,
       CASE WHEN wd2.WD_Date BETWEEN date_add(current_date(),interval -3 month) AND current_date() THEN
               'Yes'
           ELSE
               'No'
       END AS Withdrawal_Past_3_Months,
       current_datetime() AS Report_Update,
FROM degreed_total dt

    LEFT JOIN wd2_d wd2
        ON dt.credential_id = wd2.credential_id

    LEFT JOIN (
            SELECT mrs.credential_id,
                mrs.Most_Recent_Reg_Status_Date,
                CASE
                    WHEN mrs.REGISTRATIONS > 0 THEN
                        'RE'
                    WHEN mrs.WDS > 0 THEN
                        'WD'
                    WHEN mrs.DROPS > 0 THEN
                        'DD'
                END AS REGISTRATION_STATUS
            FROM mr_reg_status_d mrs
            LEFT JOIN rpt_academics.t_student_course vsc
                    ON mrs.credential_id = vsc.credential_id
                    AND mrs.Most_Recent_Reg_Status_Date = vsc.REGISTRATION_STATUS_DATE
                    and vsc.institution_id=5
            WHERE vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'

    ) mrs2
        ON dt.credential_id = mrs2.credential_id

    LEFT JOIN (
            SELECT dt.credential_id,
                MAX(vsc.END_DATE) AS Most_Recent_Reg_Course_End_Date
            FROM degreed_total dt
            LEFT JOIN rpt_academics.t_student_course vsc
                ON dt.credential_id = vsc.credential_id
                and vsc.institution_id=5
            WHERE vsc.REGISTRATION_STATUS IN ( 'RE', 'RW' )
                AND vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'
            GROUP BY dt.credential_id

    ) mre
        ON dt.credential_id = mre.credential_id

    LEFT JOIN holds_d h
        ON dt.credential_id = h.credential_id

    LEFT JOIN (
            SELECT DISTINCT
                dt.credential_id
            FROM degreed_total dt
            LEFT JOIN `rpt_academics.t_academic_outcome` vao
                ON dt.credential_id = vao.credential_id
                and vao.institution_id=5
             WHERE dt.PROGRAM != vao.PROGRAM --uncomment later
            AND
            (
                (
                    vao.STATUS_DESC = 'Awarded'
                    AND vao.GRADUATED_IND = 'Y'
                )
                OR vao.STATUS_DESC IN ( 'Grad App Rcd or Final Course', 'Thesis or Diss Complete',
                                        'Unconfirmed CAPP Requirements')
            )
    ) a
        ON dt.credential_id = a.credential_id

    LEFT JOIN gpa2_d g2
        ON dt.credential_id = g2.credential_id

    LEFT JOIN financial_d f
        ON dt.credential_id = f.credential_id

    LEFT JOIN (
            SELECT dt.credential_id,
                MAX(op.Start_Date_c) AS MR_Opp_Start_Date
            FROM degreed_total dt
                LEFT JOIN `raw_b2c_sfdc.opportunity`  op
                    ON dt.credential_id = op.banner_id_c
                    AND op.primary_flag_c = true

            GROUP BY dt.credential_id
    ) o
        ON dt.credential_id = o.credential_id

    LEFT JOIN `rpt_academics.t_person` lp --SMart.dbo.lk_person lp
        ON dt.credential_id = lp.credential_id
        and lp.institution_id=5

    LEFT JOIN `rpt_crm_mart.v_wldn_map_ea_aa` lea
        ON dt.credential_id = lea.ID
        and lea.institution_id=5
    left join  `raw_b2c_sfdc.user` u
        on u.id=lea.EA_Manager_Orig
    LEFT JOIN `rpt_academics.v_internet_address_current` le_p
        ON dt.PERSON_UID = le_p.ENTITY_UID
           AND le_p.internet_address_type = 'PERS'
           and le_p.institution_id=5

    LEFT JOIN `rpt_academics.v_internet_address_current` le_w
        ON dt.PERSON_UID = le_w.ENTITY_UID
           AND le_w.internet_address_type = 'UNIV'
           and le_w.institution_id=5

    LEFT JOIN `rpt_academics.t_address_current` addr
        ON dt.credential_id = addr.credential_id
        and addr.institution_id=5
        and addr.address_type = 'PR'


    LEFT JOIN field_d fi
        ON dt.credential_id = fi.credential_id

    LEFT JOIN (
            SELECT dt.credential_id,
                COUNT(DISTINCT (vsc.COURSE_IDENTIFICATION)) FUTURE_REG_COUNT
            FROM  degreed_total dt
                LEFT JOIN `rpt_academics.t_student_course` vsc
                    ON dt.credential_id = vsc.credential_id
                    and vsc.institution_id=5
            WHERE vsc.REGISTRATION_STATUS IN ( 'RE', 'RW' )
                AND vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'
                AND vsc.START_DATE > current_date()
            GROUP BY dt.credential_id
    ) frd
        ON dt.credential_id = frd.credential_id

    LEFT JOIN (
            SELECT dt.credential_id,
                COUNT(DISTINCT (vsc.COURSE_IDENTIFICATION)) CURRENT_REG_COUNT

            FROM degreed_total dt
                LEFT JOIN `rpt_academics.t_student_course` vsc
                    ON dt.credential_id = vsc.credential_id
                    and vsc.institution_id=5
            WHERE vsc.reg_status IN ( 'RE', 'RW' )
                AND vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'
                AND current_date()
                BETWEEN vsc.START_DATE AND vsc.END_DATE --uncomment later
            GROUP BY dt.credential_id
    ) crd
        ON dt.credential_id = crd.credential_id

    LEFT JOIN (select * from `raw_b2c_sfdc.contact` where is_deleted=false and lower(institution_code_c)='walden') c  ON dt.credential_id = c.Banner_ID_c


    LEFT JOIN (rpt_student_finance.t_wldn_aging age
                join (select max(etl_created_date) as etl_created_date from rpt_student_finance.t_wldn_aging where institution_id=5) max_date
                on age.etl_created_date=max_date.etl_created_date)
            ON dt.credential_id = age.STUDENT_ID
            and age.institution_id=5

    LEFT JOIN historical_fin_aid_d hfa
        ON dt.credential_id = hfa.credential_id

    ORDER BY dt.credential_id
);


------ NON degree-------
create or replace temp table ndegreed_total as (
SELECT DISTINCT
       vsc.credential_id,
       vsc.person_uid,
        COUNTIF(vsc.REGISTRATION_STATUS IN ('RE', 'RW')) AS STUDENT_STATUS,
       'Non-Degree' AS PROGRAM,
       'Individual Course Taker' AS STUDENT_TYPE,
       'Not Available' AS STUDENT_POPULATION_DESC,
        lp.PHONE_NUMBER AS HOME_PHONE,
       vsc.START_DATE AS PROGRAM_START_DATE,
       'Not Available' AS CONCENTRATION,
       'Not Available' AS SCHOOL,
       'Not Available' AS COLLEGE,
       'Non-Degree' AS DEGREE_LEVEL,
       'Non-Degree' AS TERM_TYPE
FROM `rpt_academics.t_student_course` vsc
    LEFT JOIN `rpt_academics.t_academic_study` vas
        ON vsc.credential_id = vas.credential_id
           AND vsc.academic_period = vas.academic_period
           and vsc.institution_id=5
           and vas.institution_id=5
     LEFT JOIN `rpt_academics.v_telephone_current` lp -- phone_current
        ON --vas.credential_id = lp.credential_id
            vas.person_uid = lp.person_uid
          AND lp.PHONE_TYPE = 'HOME'
          and lp.institution_id=5
    WHERE vas.credential_id IS NULL
      AND vsc.REGISTRATION_STATUS IN ( 'AU', 'RE', 'RW', 'A2', 'A3', 'DC', 'DR', 'W2', 'W3', 'WD', 'WI', 'WM', 'WN',
                                       'WO', 'WT'
                                     )
      AND vsc.END_DATE > '2010-01-01'
      AND vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'
      AND vsc.END_DATE =
      (
          SELECT MAX(END_DATE)
          FROM `rpt_academics.t_student_course`
          WHERE credential_id = vsc.credential_id
                AND REGISTRATION_STATUS IN ( 'AU', 'RE', 'RW', 'A2', 'A3', 'DC', 'DR', 'W2', 'W3', 'WD', 'WI', 'WM',
                                             'WN', 'WO', 'WT'
                                           )
                AND END_DATE > '2010-01-01'
                AND COURSE_IDENTIFICATION NOT LIKE '%SRO%'
                and institution_id=5
      )
      AND LEFT(vsc.credential_id, 1) = 'A'
      GROUP BY vsc.credential_id,
         lp.PHONE_NUMBER,
         vsc.START_DATE,
         vsc.COURSE_LEVEL,
         vsc.person_uid);

--select * from ndegreed_total

---------------------------------------- Inactives WD -------------------------------------
create or replace temp table non_degree as(

-- with wd_n as(
-- SELECT ndt.credential_id,dpsw.person_uid,
--        MAX(dpsw.Effective_Date) AS WD_Date

-- FROM ndegreed_total ndt --temp_table created above
--     LEFT JOIN `rpt_academics.v_withdrawal` dpsw
--         ON ndt.person_uid = dpsw.person_uid  --credential_id not present in v_withdrawl (person_uid)
--            --AND dpsw.Is_Deleted_Flag = 'N'
--            AND dpsw.institution_id = 5
-- WHERE ndt.STUDENT_STATUS IS NULL
--       --OR ndt.STUDENT_STATUS = '0'
-- GROUP BY ndt.credential_id,dpsw.person_uid
-- ),

with wd2_n as(
SELECT wn.credential_id,
       wn.WD_Date,
       COUNTIF(dpsw.withdrawal_code = 'AW') AS AW_WDs,
       COUNTIF(dpsw.withdrawal_code = 'SW') AS SW_WDs,
       COUNTIF(dpwr.value_description = 'Academic Performance') AS AW_Academic_Performance,
       COUNTIF(dpwr.value_description = 'FTA') AS AW_FTA,
       COUNTIF(dpwr.value_description = 'Failure to Register') AS AW_Failure_to_Register,
       COUNTIF(dpwr.value_description = 'Financial Suspension') AS AW_Financial_Suspension,
       COUNTIF(dpwr.value_description = 'Failure to Return from LOA') AS AW_Failure_to_Return_from_LOA,
       COUNTIF(dpwr.value_description = 'Academic Conduct') AS AW_Academic_Conduct,
       COUNTIF(dpwr.value_description = 'Write Off') AS AW_Write_Off,
       COUNTIF(dpwr.value_description = 'Deceased') AS AW_Deceased,
       COUNTIF(dpwr.value_description = 'Inactive Student') AS AW_Inactive_Student,
       COUNTIF(dpwr.value_description = 'Never Enrolled') AS AW_Never_Enrolled,
       COUNTIF(dpwr.value_description = 'Failure Complt Doc Wrtng Req') AS AW_Failure_Complt_Doc_Wrtng_Req,
       COUNTIF(dpwr.value_description = 'Financial Aid') AS AW_Financial_Aid
FROM --wd_n
    (
        SELECT ndt.credential_id,dpsw.person_uid,
            MAX(dpsw.Effective_Date) AS WD_Date

        FROM ndegreed_total ndt --temp_table created above
            LEFT JOIN `rpt_academics.v_withdrawal` dpsw
                ON ndt.person_uid = dpsw.person_uid  --credential_id not present in v_withdrawl (person_uid)
                --AND dpsw.Is_Deleted_Flag = 'N'
                AND dpsw.institution_id = 5
        WHERE ndt.STUDENT_STATUS IS NULL
            --OR ndt.STUDENT_STATUS = '0'
        GROUP BY ndt.credential_id,dpsw.person_uid
    ) wn
    LEFT JOIN rpt_academics.v_withdrawal dpsw
        ON wn.person_uid = dpsw.person_uid
          --  AND dpsw.Is_Deleted_Flag = 'N'
           AND dpsw.institution_id = 5
           AND wn.WD_Date = dpsw.effective_date
    LEFT JOIN rpt_academics.v_withdraw_reason dpwr
        ON dpsw.withdrawal_code = dpwr.value
        and dpwr.institution_id=5
GROUP BY wn.credential_id,
         wn.WD_Date
    )


-------------------------------- MR Registration Status (ALL) ------------------------------

,mr_reg_status_n as (
SELECT ndt.credential_id,
      COUNTIF(vsc.REGISTRATION_STATUS IN ('RE', 'RW', 'AU')) AS REGISTRATIONS,
       COUNTIF(vsc.REGISTRATION_STATUS IN ('DN', 'DD', 'DU', 'DW', 'W1')) AS DROPS,
       COUNTIF(vsc.REGISTRATION_STATUS IN ('DC', 'DR', 'W2', 'W3', 'WD', 'WI', 'WM', 'WN', 'WO', 'WT')) AS WDS,
       MAX(vsc.REGISTRATION_STATUS_DATE) AS Most_Recent_Reg_Status_Date
FROM ndegreed_total ndt
    LEFT JOIN rpt_academics.t_student_course vsc
        ON ndt.credential_id = vsc.credential_id
        and vsc.institution_id=5
WHERE vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'
GROUP BY ndt.credential_id)

,mr_reg_status2_n AS (
SELECT mrs.credential_id,
       mrs.Most_Recent_Reg_Status_Date,
       CASE
           WHEN mrs.REGISTRATIONS > 0 THEN
               'RE'
           WHEN mrs.WDS > 0 THEN
               'WD'
           WHEN mrs.DROPS > 0 THEN
               'DD'
       END AS REGISTRATION_STATUS
FROM mr_reg_status_n mrs
    LEFT JOIN rpt_academics.t_student_course vsc
        ON mrs.credential_id = vsc.credential_id
           AND mrs.Most_Recent_Reg_Status_Date = vsc.REGISTRATION_STATUS_DATE
           and vsc.institution_id=5
WHERE vsc.course_identification NOT LIKE '%SRO%')



, mr_reg_end_n as(
SELECT ndt.credential_id,
       MAX(vsc.END_DATE) AS Most_Recent_Reg_Course_End_Date
FROM ndegreed_total ndt
    LEFT JOIN `rpt_academics.t_student_course` vsc
        ON ndt.credential_id = vsc.credential_id
        and vsc.institution_id=5
WHERE vsc.REGISTRATION_STATUS IN ( 'RE', 'RW' )
      AND vsc.course_identification NOT LIKE '%SRO%'
GROUP BY ndt.credential_id)

--------------------------------------------------------- Holds (ALL) ------------------------------------------------
, holds_n as (
SELECT ndt.credential_id,
       countif(lh.HOLD = 'HH') AS LOA_COUNT,
       MAX(loa.HOLD_TO_DATE) AS MR_LOA_END,
       countif(lh.HOLD IN ( 'BH', 'C2', 'C1', 'FS' )) AS FINANCIAL_HOLD_COUNT,
       countif(lh.HOLD = 'CH') AS CONTINGENCY_HOLD_COUNT,
       countif(lh.HOLD = 'PA' ) AS NON_ACTIVITY_HOLD_COUNT,
       countif(lh.HOLD = 'RH' ) AS REGISTRAR_HOLD_COUNT,
       countif(lh.REGISTRATION_HOLD_IND = 'Y' ) AS REGISTRATION_HOLDS,
       countif(lh.TRANSCRIPT_HOLD_IND = 'Y' ) AS TRANSCRIPT_HOLDS,
       countif(lh.GRADUATION_HOLD_IND = 'Y' ) AS GRADUATION_HOLDS

FROM ndegreed_total ndt
    LEFT JOIN rpt_academics.v_hold lh
        ON ndt.credential_id = lh.credential_id
           AND CURRENT_TIMESTAMP
           BETWEEN lh.hold_from_date AND lh.hold_to_date
           and lh.institution_id=5
    LEFT JOIN rpt_academics.v_hold loa
        ON ndt.credential_id = loa.credential_id
           AND CURRENT_TIMESTAMP
           BETWEEN loa.HOLD_FROM_DATE AND loa.HOLD_TO_DATE
           AND loa.HOLD = 'HH'
           and loa.institution_id=5
GROUP BY ndt.credential_id)


-------------------------------------------------------- Alumni flag (ALL) ------------------------------------------------------
,alumni_n as (
SELECT DISTINCT
       ndt.credential_id

FROM ndegreed_total ndt
    LEFT JOIN rpt_academics.t_academic_outcome vao
        ON ndt.credential_id = vao.credential_id
        and vao.institution_id=5
WHERE ndt.PROGRAM != vao.PROGRAM
      AND
      (
          (
              vao.STATUS_DESC = 'Awarded'
              AND vao.GRADUATED_IND = 'Y'
          )
          OR vao.STATUS_DESC IN ( 'Grad App Rcd or Final Course', 'Thesis or Diss Complete',
                                  'Unconfirmed CAPP Requirements'
                                )
      )
)
--select * from alumni_n

--------------------------------------------------- Financial Status (ALL) ----------------------------------------------------
,financial_n as (
SELECT ndt.credential_id,
       --fin.MILESTONE FIN_AID_STATUS,
       fin.NEW_MILESTONE FIN_AID_STATUS,
       fin.TERM

FROM ndegreed_total ndt
    -- LEFT JOIN BI_Analytics_DM.dbo.Financial_Aid_Reporting fin
    LEFT JOIN trans_academics.prc_rrt_fa_data fin
        ON ndt.credential_id = fin.ALTERNATE_ID
           --AND fin.STATUS NOT IN ( '50', '51' ) cant find mapping, table has no data as well
           )
--select * from financial_n

---------------------------------------------------- Opportunity Status (ALL) --------------------------------------------------
, opportunity_n as (
SELECT ndt.credential_id,
       MAX(op.Start_Date_c) AS MR_Opp_Start_Date
FROM ndegreed_total ndt
    LEFT JOIN `raw_b2c_sfdc.opportunity` op
    ON ndt.credential_id = op.banner_id_c
        AND op.primary_flag_c = true
        --    and op._fivetran_deleted=false
GROUP BY ndt.credential_id)




----------------------------------------------------- Future Course Registrations -----------------------------------------------
,future_reg_n as(
SELECT ndt.credential_id,
       COUNT(DISTINCT (vsc.COURSE_IDENTIFICATION)) FUTURE_REG_COUNT
FROM ndegreed_total ndt
    LEFT JOIN `rpt_academics.t_student_course` vsc
        ON ndt.credential_id = vsc.credential_id
        and vsc.institution_id=5
WHERE vsc.REGISTRATION_STATUS IN ( 'RE', 'RW' )
      AND vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'
      AND vsc.START_DATE > current_date()
GROUP BY ndt.credential_id)
--select * from future_reg_n

----------------------------------------------------- Current Course Registrations -----------------------------------------------
,current_reg_n as (
SELECT ndt.credential_id,
       COUNT(DISTINCT (vsc.COURSE_IDENTIFICATION)) CURRENT_REG_COUNT
FROM ndegreed_total ndt
    LEFT JOIN `rpt_academics.t_student_course` vsc
        ON ndt.credential_id = vsc.credential_id
        and vsc.institution_id=5
WHERE vsc.REGISTRATION_STATUS IN ( 'RE', 'RW' )
      AND vsc.COURSE_IDENTIFICATION NOT LIKE '%SRO%'
      AND current_date()
      BETWEEN vsc.START_DATE AND vsc.END_DATE
GROUP BY ndt.credential_id)

,historical_fin_aid_n as (
SELECT ndt.credential_id
FROM ndegreed_total ndt
    INNER JOIN trans_academics.v_lk_powerfaids pf
        ON ndt.credential_id = pf.alternate_id

        )

SELECT DISTINCT
       ndt.credential_id Student_ID,
       c.Id Student_Salesforce_Contact_ID,
       CASE
           WHEN ndt.STUDENT_STATUS > 0 THEN
               'Active'
           ELSE
               'Inactive'
       END AS Student_Status,
       ndt.STUDENT_TYPE Student_Modality,
       ndt.DEGREE_LEVEL Degree_Level,
       ndt.PROGRAM Program,
       ndt.STUDENT_POPULATION_DESC Student_Population,
       ndt.HOME_PHONE Home_Phone,
       ndt.PROGRAM_START_DATE Program_Start_Date,
       ndt.CONCENTRATION Concentration,
       ndt.SCHOOL School,
       ndt.COLLEGE College,
       ndt.TERM_TYPE Term_Type,
       CASE
           WHEN mrs2.REGISTRATION_STATUS = 'RE' THEN
               cast(mre.Most_Recent_Reg_Course_End_Date as date)
           ELSE
               cast(mrs2.Most_Recent_Reg_Status_Date as date)
       END AS Withdrawal_Date,
       CASE
           WHEN wn2.AW_WDs > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Administrative_Withdrawal_AW,
       CASE
           WHEN wn2.AW_Academic_Performance > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Academic_Performance,
       CASE
           WHEN wn2.AW_FTA > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_FTA,
       CASE
           WHEN wn2.AW_Failure_to_Register > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Failure_to_Register,
       CASE
           WHEN wn2.AW_Financial_Suspension > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Financial_Suspension,
       CASE
           WHEN wn2.AW_Failure_to_Return_from_LOA > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Failure_to_Return_from_LOA,
       CASE
           WHEN wn2.AW_Academic_Conduct > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Academic_Conduct,
       CASE
           WHEN wn2.AW_Write_Off > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Write_Off,
       CASE
           WHEN wn2.AW_Deceased > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Deceased,
       CASE
           WHEN wn2.AW_Inactive_Student > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Inactive_Student,
       CASE
           WHEN wn2.AW_Never_Enrolled > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Never_Enrolled,
       CASE
           WHEN wn2.AW_Failure_Complt_Doc_Wrtng_Req > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Failure_Complt_Doc_Wrtng_Req,
       CASE
           WHEN wn2.AW_Financial_Aid > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS AW_Financial_Aid,
       CASE
           WHEN wn2.SW_WDs > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Student_Withdrawal,
       mrs2.Most_Recent_Reg_Status_Date Most_Recent_Registration_Status_Change_Date,
       mrs2.REGISTRATION_STATUS Most_Recent_Registration_Status,
       mre.Most_Recent_Reg_Course_End_Date Most_Recent_Registered_Course_End_Date,
       CASE
           WHEN h.CONTINGENCY_HOLD_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Contingency_Hold,
       CASE
           WHEN h.FINANCIAL_HOLD_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Financial_Hold,
       CASE
           WHEN h.GRADUATION_HOLDS > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Graduation_Hold,
       CASE
           WHEN h.LOA_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS On_LOA,
       h.MR_LOA_END AS Most_Recent_LOA_End_Date,
       CASE
           WHEN h.NON_ACTIVITY_HOLD_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Non_Activity_Hold,
       CASE
           WHEN h.REGISTRAR_HOLD_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Registrar_Hold,
       CASE
           WHEN h.REGISTRATION_HOLDS > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Registration_Hold,
       CASE
           WHEN h.TRANSCRIPT_HOLDS > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Transcript_Hold,
       CASE
           WHEN a.credential_id IS NOT NULL THEN
               'Yes'
           ELSE
               'No'
       END AS Alumni,
       null AS Cumulative_Passed_Credits,
       null AS Cumulative_TOC,
       null AS Cumulative_GPA,
       cast(null as string) AS Cumulative_GPA_Range,
       age.CURRENT_AMOUNT_DUE Current_Balance,
       f.FIN_AID_STATUS Current_Financial_Aid_Status,
       f.TERM Current_Financial_Aid_Term,
       o.MR_Opp_Start_Date Most_Recent_Opportunity_Start_Date,
       lp.FIRST_NAME,
       lp.LAST_NAME,
       lea.AA_Name,
       lea.AA_Manager,
       lea.AA_Location,
       lea.EA_Name_Orig,
       u.Name as EA_Manager_Orig,
       lea.EA_Location_Orig,
      --  case when le_p.internet_address_type = 'PERS' then internet_address end as Personal_Email,
      --  case when le_p.internet_address_type = 'UNIV' then internet_address end as Walden_Email,
       le_p.internet_address as Personal_Email,
       le_w.internet_address as Walden_Email,
       upper(addr.STREET_LINE1) as STREET_LINE1,
       addr.STREET_LINE2,
       addr.STREET_LINE3,
       upper(addr.CITY) as CITY,
       addr.STATE_PROVINCE,
       addr.POSTAL_CODE,
       addr.NATION_DESC,
       CASE
	    WHEN mailing_country ='UNITED STATES' THEN 'US CONTINENT'
	    WHEN mailing_country ='APO' THEN 'US MILITARY BASES'
	    WHEN mailing_country IN ( 'AMERICAN SAMOA', 'NORTHERN MARIANA ISLANDS', 'U.S. VIRGIN ISLANDS', 'GUAM', 'PUERTO RICO' ) THEN 'US TERRITORIES'
	    WHEN mailing_country IS NULL THEN NULL
	  ELSE
	  'NON US'
	END as residency,

       NULL AS Field_Course_Credits_Passed,
       CASE
           WHEN frn.FUTURE_REG_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Future_Registrations,
       CAST(crn.CURRENT_REG_COUNT AS string) AS Current_Registrations,
       CASE
           WHEN hfa.credential_id IS NOT NULL THEN
               'Yes'
           ELSE
               'No'
       END AS Ever_Used_Financial_Aid_at_Walden,
       CASE
           WHEN mrs2.REGISTRATION_STATUS = 'RE' THEN
               CASE
                   WHEN cast(mre.Most_Recent_Reg_Course_End_Date as date)
                        BETWEEN date_add(current_date(),interval -3 month) AND current_date() THEN
                       'Yes'
                   ELSE
                       'No'
               END
           ELSE
               CASE
                   WHEN cast(mrs2.Most_Recent_Reg_Status_Date as date)
                        BETWEEN date_add(current_date(),interval -3 month) AND current_date() THEN
                       'Yes'
                   ELSE
                       'No'
               END
       END AS Withdrawal_Past_3_Months,
       current_datetime() AS Report_Update
FROM ndegreed_total ndt
    LEFT JOIN wd2_n wn2
        ON ndt.credential_id = wn2.credential_id
    LEFT JOIN mr_reg_status2_n mrs2
        ON ndt.credential_id = mrs2.credential_id
    LEFT JOIN mr_reg_end_n mre
        ON ndt.credential_id = mre.credential_id
    LEFT JOIN holds_n h
        ON ndt.credential_id = h.credential_id
    LEFT JOIN alumni_n a
        ON ndt.credential_id = a.credential_id
    LEFT JOIN financial_n f
        ON ndt.credential_id = f.credential_id
    LEFT JOIN opportunity_n o
        ON ndt.credential_id = o.credential_id
    LEFT JOIN future_reg_n frn
        ON ndt.credential_id = frn.credential_id
    LEFT JOIN current_reg_n crn
        ON ndt.credential_id = crn.credential_id
     LEFT JOIN `rpt_academics.t_person` lp --SMart.dbo.lk_person lp
        ON ndt.credential_id = lp.credential_id
        and lp.institution_id=5
    LEFT JOIN `rpt_crm_mart.v_wldn_map_ea_aa` lea
        ON ndt.credential_id = lea.ID
        and lea.institution_id=5
    left join  `raw_b2c_sfdc.user` u
        on u.id=lea.EA_Manager_Orig
    LEFT JOIN `rpt_academics.v_internet_address_current` le_p
        ON ndt.PERSON_UID = le_p.ENTITY_UID
           AND le_p.internet_address_type = 'PERS'
           and le_p.institution_id=5

    LEFT JOIN `rpt_academics.v_internet_address_current` le_w
        ON ndt.PERSON_UID = le_w.ENTITY_UID
           AND le_w.internet_address_type = 'UNIV'
           and le_w.institution_id=5



    LEFT JOIN `rpt_academics.t_address_current` addr
        ON ndt.credential_id = addr.credential_id
        and addr.institution_id=5
        and addr.address_type = 'PR'

     LEFT JOIN (select * from `raw_b2c_sfdc.contact` where is_deleted=false and lower(institution_code_c)='walden') c
       ON ndt.credential_id = c.Banner_ID_c

    LEFT JOIN (rpt_student_finance.t_wldn_aging age
                join (select max(etl_created_date) as etl_created_date from rpt_student_finance.t_wldn_aging where institution_id=5 ) max_date
                on age.etl_created_date=max_date.etl_created_date)
            ON ndt.credential_id = age.STUDENT_ID
            and age.institution_id =5


    LEFT JOIN historical_fin_aid_n hfa
        ON ndt.credential_id = hfa.credential_id



);

create or replace temp table TEMPO as (
with tempo_total as(
  SELECT tmp.CBL_Id_c AS ID,
        tmp.Student_Status_c AS STUDENT_STATUS,
        tmp.Program_c,
        prod.Name AS PROGRAM,
        'Tempo' AS STUDENT_TYPE,
        'Not Available' AS STUDENT_POPULATION_DESC,
        con.Home_Phone AS HOME_PHONE,
        tmp.First_Start_Date_c AS PROGRAM_START_DATE,
        prod.Specialization_c AS CONCENTRATION,
        'Not Available' AS SCHOOL,
        'Not Available' AS COLLEGE,
        tmp.Degree_Level_c AS DEGREE_LEVEL,
        'Tempo' AS TERM_TYPE,
        tmp.Student_Enrollment_Status_c AS REGISTRATION_STATUS,
        CASE
            WHEN RIGHT(tmp.Student_Enrollment_Status_c, 3) = 'A1)' THEN
                'Yes'
            ELSE
                'No'
        END AS AD_WD,
        CASE
            WHEN RIGHT(tmp.Student_Enrollment_Status_c, 3) = 'W1)' THEN
                'Yes'
            ELSE
                'No'
        END AS SW_WD
  FROM `stg_l2_salesforce.student_program_c` tmp
      LEFT JOIN `stg_l2_salesforce.product_2` prod
          ON tmp.Program_c = prod.Id
        --   and tmp._fivetran_deleted= false
        --   and prod._fivetran_deleted= false

      LEFT JOIN `stg_l2_salesforce.contact` con
          ON tmp.CBL_Id_c = con.CBL_Student_ID_c
        --   and con._fivetran_deleted= false
  WHERE tmp.Degree_Status_c != 'Degree Received'

        AND tmp.First_Start_Date_c =
        (
            SELECT MAX(spc.First_Start_Date_c)
            FROM `stg_l2_salesforce.student_program_c` spc
            WHERE spc.CBL_Id_c = tmp.CBL_Id_c
                  AND spc.last_activity_date =
                  (
                      SELECT MAX(last_activity_date)
                      FROM stg_l2_salesforce.student_program_c
                      WHERE CBL_Id_c = spc.CBL_Id_c
                            AND First_Start_Date_c = spc.First_Start_Date_c
                  )
                --   and spc._fivetran_deleted= false
        )
        AND tmp.created_date =
        (
            SELECT MAX(created_date)
            FROM `stg_l2_salesforce.student_program_c`
            WHERE CBL_Id_c = tmp.CBL_Id_c
            -- and _fivetran_deleted= false
        )
        AND LEFT(tmp.CBL_Id_c, 1) = 'C'
)
,wd_t as(
      SELECT tt.ID,
          CASE
              WHEN MAX(Subscription_DP_WD_Effective_Date_c) IS NOT NULL THEN
                  MAX(Subscription_DP_WD_Effective_Date_c)
              WHEN MAX(End_Date_c) IS NOT NULL THEN
                  MAX(End_Date_c)
              ELSE
                  NULL
          END AS WD_Date
    FROM tempo_total tt
        LEFT JOIN `stg_l2_salesforce.student_subscription_c` sub
            ON tt.ID = sub.CBL_Id_c
              AND sub.Change_Reason_c = 'Withdrawal'
            --   and sub._fivetran_deleted= false
    WHERE tt.STUDENT_STATUS = 'Inactive'
    GROUP BY tt.ID

),
holds_t as(
    SELECT tt.ID,
       countif(CURRENT_date()
                       BETWEEN tmp.Leave_of_Absence_LOA_From_c AND tmp.Leave_of_Absence_LOA_To_c ) AS LOA_COUNT,
       MAX(tmp.Leave_of_Absence_LOA_To_c) AS MR_LOA_END,
       countif(RIGHT(tmp.Student_Enrollment_Status_c, 3) = 'FH)' ) AS FINANCIAL_HOLD_COUNT,
       countif(tmp.Open_Contingency_Hold_c > 0
                       OR RIGHT(tmp.Student_Enrollment_Status_c, 3) = 'CH)' ) AS CONTINGENCY_HOLD_COUNT,
       countif(RIGHT(tmp.Student_Enrollment_Status_c, 3) = 'NP)' ) AS NON_ACTIVITY_HOLD_COUNT,
       -99 AS REGISTRAR_HOLD_COUNT,
       countif(tmp.Active_Hold_Codes_c > 0 ) AS REGISTRATION_HOLDS,
       -99 AS TRANSCRIPT_HOLDS,
       -99 AS GRADUATION_HOLDS
  FROM tempo_total tt
    LEFT JOIN `stg_l2_salesforce.student_program_c` tmp
        ON tt.ID = tmp.CBL_Id_c
        -- and tmp._fivetran_deleted=false
  GROUP BY tt.ID


),
gpa_t as(
  SELECT tt.ID,
       tmp.GPA_c AS cum_gpa,
       CASE
           WHEN tmp.GPA_c >= 3.5 THEN
               '3.50 +'
           WHEN tmp.GPA_c
                BETWEEN 3.00 AND 3.49 THEN
               '3.00_3.49'
           WHEN tmp.GPA_c
                BETWEEN 2.70 AND 2.99 THEN
               '2.70_2.99'
           WHEN tmp.GPA_c
                BETWEEN 2.50 AND 2.69 THEN
               '2.50_2.69'
           WHEN tmp.GPA_c
                BETWEEN 1.90 AND 2.49 THEN
               '1.90_2.49'
           WHEN tmp.GPA_c < 1.90 THEN
               '1.90_'
       END AS cum_gpa_range,
       tmp.All_Credits_Completed_c AS cum_credits_passed,
       tmp.Number_of_Credits_Transferred_c AS cum_credits_toc
FROM tempo_total tt
    LEFT JOIN `stg_l2_salesforce.student_program_c` tmp
        ON tt.ID = tmp.CBL_Id_c
           AND tt.Program_c = tmp.Program_c
        --    and tmp._fivetran_deleted=false

),
financial_t as (
  SELECT tt.ID,
       --fin.MILESTONE FIN_AID_STATUS,
       fin.NEW_MILESTONE FIN_AID_STATUS,
       fin.TERM
  FROM tempo_total tt
    -- LEFT JOIN BI_Analytics_DM.dbo.Financial_Aid_Reporting fin
    LEFT JOIN `trans_academics.prc_rrt_fa_data` fin
        ON tt.ID = fin.ALTERNATE_ID
         --  AND fin.STATUS NOT IN ( '50', '51' )
),
opportunity_t as(
    SELECT tt.ID,
          MAX(op2.Student_Start_Date_c) AS MR_Opp_Start_Date
    FROM tempo_total tt
        LEFT JOIN `stg_l2_salesforce.opportunity` op2
            ON tt.ID = op2.CBL_Student_Id_c
            -- and op2._fivetran_deleted=false
    GROUP BY tt.ID
  )
  SELECT DISTINCT
       tt.ID Student_ID,
       pgm.Student_c Student_Salesforce_Contact_ID,
       tt.STUDENT_STATUS Student_Status,
       tt.STUDENT_TYPE Student_Modality,
       tt.DEGREE_LEVEL Degree_Level,
       tt.PROGRAM Program,
       tt.STUDENT_POPULATION_DESC Student_Population,
       tt.HOME_PHONE Home_Phone,
       tt.PROGRAM_START_DATE Program_Start_Date,
       tt.CONCENTRATION Concentration,
       tt.SCHOOL School,
       tt.COLLEGE College,
       tt.TERM_TYPE Term_Type,
       wd.WD_Date AS Withdrawal_Date,
       tt.AD_WD AS Administrative_Withdrawal_AW,
       'N/A' AS AW_Academic_Performance,
       'N/A' AS AW_FTA,
       'N/A' AS AW_Failure_to_Register,
       'N/A' AS AW_Financial_Suspension,
       'N/A' AS AW_Failure_to_Return_from_LOA,
       'N/A' AS AW_Academic_Conduct,
       'N/A' AS AW_Write_Off,
       'N/A' AS AW_Deceased,
       'N/A' AS AW_Inactive_Student,
       'N/A' AS AW_Never_Enrolled,
       'N/A' AS AW_Failure_Complt_Doc_Wrtng_Req,
       'N/A' AS AW_Financial_Aid,
       tt.SW_WD AS Student_Withdrawal,
       cast(NULL as TIMESTAMP ) Most_Recent_Registration_Status_Change_Date,
       tt.REGISTRATION_STATUS Most_Recent_Registration_Status,
       cast(NULL as date ) Most_Recent_Registered_Course_End_Date,
       CASE
           WHEN h.CONTINGENCY_HOLD_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Contingency_Hold,
       CASE
           WHEN h.FINANCIAL_HOLD_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Financial_Hold,
       CASE
           WHEN h.GRADUATION_HOLDS > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Graduation_Hold,
       CASE
           WHEN h.LOA_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS On_LOA,
       cast(h.MR_LOA_END as timestamp) AS Most_Recent_LOA_End_Date,
       CASE
           WHEN h.NON_ACTIVITY_HOLD_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Non_Activity_Hold,
       CASE
           WHEN h.REGISTRAR_HOLD_COUNT > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Registrar_Hold,
       CASE
           WHEN h.REGISTRATION_HOLDS > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Registration_Hold,
       CASE
           WHEN h.TRANSCRIPT_HOLDS > 0 THEN
               'Yes'
           ELSE
               'No'
       END AS Transcript_Hold,
       cast (NULL as string) AS Alumni,
       g.cum_credits_passed Cumulative_Passed_Credits,
       g.cum_credits_toc Cumulative_TOC,
       g.cum_gpa Cumulative_GPA,
       g.cum_gpa_range Cumulative_GPA_Range,
       age.CURRENT_AMOUNT_DUE Current_Balance,
       f.FIN_AID_STATUS Current_Financial_Aid_Status,
       f.TERM Current_Financial_Aid_Term,
       o.MR_Opp_Start_Date Most_Recent_Opportunity_Start_Date,
       con.First_Name AS FIRST_NAME,
       con.Last_Name AS LAST_NAME,
       pgm.Coach_Name_c AS AA_Name,
       cast(NULL as string) AS AA_Manager,
       cast(NULL as string) AS AA_Location,
       cast(NULL as string) AS EA_Name_Orig,
       cast(NULL as string) AS EA_Manager_Orig,
       cast(NULL as string) AS EA_Location_Orig,
       con.Email AS Personal_Email,
       pgm.Student_s_Walden_Email_c AS Walden_Email,
       upper(con.mailing_street) AS STREET_LINE1,
       cast(NULL as string) AS STREET_LINE2,
       cast(NULL as string) AS STREET_LINE3,
       upper(con.Mailing_City) AS CITY,
       CASE
           WHEN con.Mailing_State = 'Alabama' THEN
               'AL'
           WHEN con.Mailing_State = 'Alaska' THEN
               'AK'
           WHEN con.Mailing_State = 'Arizona' THEN
               'AZ'
           WHEN con.Mailing_State = 'Arkansas' THEN
               'AR'
           WHEN con.Mailing_State = 'California' THEN
               'CA'
           WHEN con.Mailing_State = 'Colorado' THEN
               'CO'
           WHEN con.Mailing_State = 'Connecticut' THEN
               'CT'
           WHEN con.Mailing_State = 'Delaware' THEN
               'DE'
           WHEN con.Mailing_State = 'District of Columbia' THEN
               'DC'
           WHEN con.Mailing_State = 'Florida' THEN
               'FL'
           WHEN con.Mailing_State = 'Georgia' THEN
               'GA'
           WHEN con.Mailing_State = 'Hawaii' THEN
               'HI'
           WHEN con.Mailing_State = 'Idaho' THEN
               'ID'
           WHEN con.Mailing_State = 'Illinois' THEN
               'IL'
           WHEN con.Mailing_State = 'Indiana' THEN
               'IN'
           WHEN con.Mailing_State = 'Iowa' THEN
               'IA'
           WHEN con.Mailing_State = 'Kansas' THEN
               'KS'
           WHEN con.Mailing_State = 'Kentucky' THEN
               'KY'
           WHEN con.Mailing_State = 'Louisiana' THEN
               'LA'
           WHEN con.Mailing_State = 'Maine' THEN
               'ME'
           WHEN con.Mailing_State = 'Maryland' THEN
               'MD'
           WHEN con.Mailing_State = 'Massaschusetts' THEN
               'MA'
           WHEN con.Mailing_State = 'Michigan' THEN
               'MI'
           WHEN con.Mailing_State = 'Minnesota' THEN
               'MN'
           WHEN con.Mailing_State = 'Mississippi' THEN
               'MS'
           WHEN con.Mailing_State = 'Missouri' THEN
               'MO'
           WHEN con.Mailing_State = 'Montana' THEN
               'MT'
           WHEN con.Mailing_State = 'Nebraska' THEN
               'NE'
           WHEN con.Mailing_State = 'Nevada' THEN
               'NV'
           WHEN con.Mailing_State = 'New Hampshire' THEN
               'NH'
           WHEN con.Mailing_State = 'New Jersey' THEN
               'NJ'
           WHEN con.Mailing_State = 'New Mexico' THEN
               'NM'
           WHEN con.Mailing_State = 'New York' THEN
               'NY'
           WHEN con.Mailing_State = 'North Carolina' THEN
               'NC'
           WHEN con.Mailing_State = 'North Dakota' THEN
               'ND'
           WHEN con.Mailing_State = 'Ohio' THEN
               'OH'
           WHEN con.Mailing_State = 'Oklahoma' THEN
               'OK'
           WHEN con.Mailing_State = 'Oregon' THEN
               'OR'
           WHEN con.Mailing_State = 'Pennsylvania' THEN
               'PA'
           WHEN con.Mailing_State = 'Rhode Island' THEN
               'RI'
           WHEN con.Mailing_State = 'South Carolina' THEN
               'SC'
           WHEN con.Mailing_State = 'South Dakota' THEN
               'SD'
           WHEN con.Mailing_State = 'Tennessee' THEN
               'TN'
           WHEN con.Mailing_State = 'Texas' THEN
               'TX'
           WHEN con.Mailing_State = 'Utah' THEN
               'UT'
           WHEN con.Mailing_State = 'VERMONT' THEN
               'VT'
           WHEN con.Mailing_State = 'Virginia' THEN
               'VA'
           WHEN con.Mailing_State = 'Washington' THEN
               'WA'
           WHEN con.Mailing_State = 'West Virginia' THEN
               'WV'
           WHEN con.Mailing_State = 'Wisconsin' THEN
               'WI'
           WHEN con.Mailing_State = 'Wyoming' THEN
               'WY'
           WHEN con.Mailing_State = 'American Samoa' THEN
               'AS'
           WHEN con.Mailing_State = 'Guam' THEN
               'GU'
           WHEN con.Mailing_State = 'Marshall Islands' THEN
               'MH'
           WHEN con.Mailing_State = 'Micronesia' THEN
               'FM'
           WHEN con.Mailing_State = 'Northern Marianas' THEN
               'MP'
           WHEN con.Mailing_State = 'Palau' THEN
               'PW'
           WHEN con.Mailing_State = 'Puerto Rico' THEN
               'PR'
           WHEN con.Mailing_State = 'Virgin Islands' THEN
               'VI'
           ELSE
               con.Mailing_State
       END AS STATE_PROVINCE,
       con.Mailing_Postal_Code AS POSTAL_CODE,
       con.Mailing_Country AS NATION_DESC,
       CASE
	    WHEN mailing_country ='UNITED STATES' THEN 'US CONTINENT'
	    WHEN mailing_country ='APO' THEN 'US MILITARY BASES'
	    WHEN mailing_country IN ( 'AMERICAN SAMOA', 'NORTHERN MARIANA ISLANDS', 'U.S. VIRGIN ISLANDS', 'GUAM', 'PUERTO RICO' ) THEN 'US TERRITORIES'
	    WHEN mailing_country IS NULL THEN NULL
	  ELSE
	  'NON US'
	END AS residency,
       cast(NULL as BIGNUMERIC) AS Field_Course_Credits_Passed,
       'N/A' AS Future_Registrations,
       'N/A' AS Current_Registrations,
       'Unsure' AS Ever_Used_Financial_Aid_at_Walden,
       CASE
           WHEN wd.WD_Date BETWEEN date_add(current_date(),interval -3 month) AND current_date() THEN
               'Yes'
           ELSE
               'No'
       END AS Withdrawal_Past_3_Months,
       current_datetime() AS Report_Update
FROM tempo_total tt
    LEFT JOIN wd_t wd
        ON tt.ID = wd.ID
    LEFT JOIN holds_t h
        ON tt.ID = h.ID
    LEFT JOIN gpa_t g
        ON tt.ID = g.ID
    LEFT JOIN financial_t f
        ON tt.ID = f.ID
    LEFT JOIN opportunity_t o
        ON tt.ID = o.ID
    LEFT JOIN (select * from `raw_b2c_sfdc.contact` where is_deleted=false and lower(institution_code_c)='walden') con
        ON tt.ID = con.CBL_Student_ID_c

    LEFT JOIN `stg_l2_salesforce.student_program_c` pgm
        ON tt.ID = pgm.CBL_Id_c

        LEFT JOIN (rpt_student_finance.t_wldn_aging age
                join (select max(etl_created_date) as etl_created_date from rpt_student_finance.t_wldn_aging where institution_id=5) max_date
                on age.etl_created_date=max_date.etl_created_date)
            ON tt.ID = age.STUDENT_ID
            and age.institution_id=5


);

create or replace temp table src as(
select * from DEGREED_all
union all
select * from non_degree
union all
select * from TEMPO
);


CREATE OR REPLACE TEMP TABLE src_temp --CLUSTER BY etl_pk_hash, pk_chg_hash
  AS (
SELECT
       src.*, institution,institution_id,'WLDN_BNR' as source_system_name,
                    farm_fingerprint(format('%T', concat(''))) AS etl_pk_hash,
                    farm_fingerprint(format('%T', src )) as etl_chg_hash,
                    job_start_dt as etl_created_date,
                    job_start_dt as etl_updated_date,
                    load_source as etl_resource_name,
                    v_audit_key as etl_ins_audit_key,
                    v_audit_key as etl_upd_audit_key,

        from src
  );

        /* merge process */
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
         (v_audit_key,'DS_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;


SET result =  @@error.message;

RAISE USING message = @@error.message;

    end;


end