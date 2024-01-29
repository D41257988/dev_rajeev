BEGIN
/*Begin PBI Query to temp table */
--SOAR CU Banner Data Elements POC.
--The cardinality for this main SQL script will be one row per student (per active hold if any)
--Final name changes done to align query output with existing PBI file entries
CREATE OR REPLACE TEMP TABLE CURRENT_TERM AS (
         Select DISTINCT MAX(academic_period) Over (PARTITION BY term_type) Current_Term
  FROM rpt_academics.t_wldn_academic_calendar_ds
  WHERE start_date <= CURRENT_DATE);
CREATE OR REPLACE TEMP TABLE CT_TERM AS (
       Select academic_period TERM_CODE,
              academic_period_desc TERM_NAME,
              start_date TERM_START_DATE,
              'WLDN' INSTITUTION_CODE
       FROM rpt_academics.t_wldn_academic_calendar_ds
       );

CREATE OR REPLACE TEMP TABLE CT_COURSE AS (
       Select PERSON_UID,
              credential_id DSI,
              ACADEMIC_PERIOD,
              COURSE_IDENTIFICATION,
              COURSE_REFERENCE_NUMBER,
              COURSE_LEVEL,
              REGISTRATION_STATUS,
              INSTITUTION,
              INSTITUTION_COURSE_IND,
              CREDITS_EARNED,
              CREDITS_ATTEMPTED,
              IN_HISTORY_TBL_IND,
              TRANSFER_COURSE_IND,
              GRADE_TYPE,
              FINAL_GRADE,
              SCHEDULE_TYPE,
              SCHEDULE_TYPE_DESC
       FROM rpt_semantic.student_course
       WHERE INSTITUTION = 'WLDN');

CREATE OR REPLACE TEMP TABLE CN_TERMS AS
(
  Select MAX(CASE
         WHEN TERMS_FINAL.RNK = 1 THEN TERMS_FINAL.TERM_CODE
         END) Next_Term_Code,
         MAX(CASE
         WHEN TERMS_FINAL.RNK = 1 THEN TERMS_FINAL.TERM_NAME
         END) Next_Term,
         MAX(CASE
         WHEN TERMS_FINAL.RNK = 2 THEN TERMS_FINAL.TERM_CODE
         END) Current_Term_Code,
         MAX(CASE
         WHEN TERMS_FINAL.RNK = 2 THEN TERMS_FINAL.TERM_NAME
         END) Current_Term,
         MAX(CASE
         WHEN TERMS_FINAL.RNK = 2 THEN TERMS_FINAL.TERM_START_DATE
         END) Current_Term_Start,
         1 TERMS_JOIN
  FROM (
    Select TERMS_SUB2.TERM_CODE,
           TERMS_SUB2.TERM_NAME,
           TERMS_SUB2.TERM_START_DATE,
           RANK () OVER (ORDER BY TERMS_SUB2.TERM_CODE DESC) RNK
    FROM (
        Select DISTINCT TERMS_SUB.TERM_CODE,
                TERMS_SUB.TERM_NAME,
                TERMS_SUB.TERM_START_DATE
        FROM CT_TERM TERMS_SUB
        INNER JOIN (
            Select MIN(NEXT_TERM_SUB.TERM_CODE) OVER (PARTITION BY NEXT_TERM_SUB.INSTITUTION_CODE) Term_Filter,
            NEXT_TERM_SUB.INSTITUTION_CODE
            FROM CT_TERM NEXT_TERM_SUB
            WHERE NEXT_TERM_SUB.TERM_START_DATE > CURRENT_DATE) NEXT_TERM
       -- TERMS_SUB.INSTITUTION_CODE = NEXT_TERM.INSTITUTION_CODE
        ON TERMS_SUB.TERM_CODE <= NEXT_TERM.Term_Filter) TERMS_SUB2) TERMS_FINAL);

CREATE OR REPLACE TEMP TABLE CT_ADDRESS AS (
         Select DISTINCT ADDR_SUB.PERSON_UID,
         ADDR_SUB.CITY,
         ADDR_SUB.STATE_PROVINCE,
         ADDR_SUB.ADDRESS_NUMBER,
         MAX(ADDR_SUB.ADDRESS_NUMBER) OVER (PARTITION BY ADDR_SUB.PERSON_UID) Max_Address_Number
  FROM `rpt_semantic.address` ADDR_SUB
  WHERE ADDR_SUB.INSTITUTION = 'WLDN'
  AND ADDR_SUB.ADDRESS_TYPE = 'MA');

CREATE OR REPLACE TEMP TABLE CT_INT_ADDRESS AS (
         Select DISTINCT INT_ADDR_SUB.PERSON_UID,
         INT_ADDR_SUB.INTERNET_ADDRESS,
         INT_ADDR_SUB.ACTIVITY_DATE,
         MAX(INT_ADDR_SUB.ACTIVITY_DATE) OVER (PARTITION BY INT_ADDR_SUB.PERSON_UID) Max_Email_Date
  FROM `rpt_semantic.internet_address` INT_ADDR_SUB
  WHERE INT_ADDR_SUB.INSTITUTION = 'WLDN'
  AND INT_ADDR_SUB.INTERNET_ADDRESS_PREFERRED_IND = 'Y');

CREATE OR REPLACE TEMP TABLE CT_PHONE AS (
              SELECT DISTINCT PHONE_FINAL.PERSON_UID,
              COALESCE(MAX(CASE WHEN PHONE_FINAL.PHONE_TYPE = 'CELL' THEN PHONE_FINAL.phone_number END) OVER (PARTITION BY PHONE_FINAL.PERSON_UID),
              MAX(CASE WHEN PHONE_FINAL.PHONE_TYPE = 'PR' THEN PHONE_FINAL.phone_number END) OVER (PARTITION BY PHONE_FINAL.PERSON_UID)) phone_number,
              COALESCE(MAX(CASE WHEN PHONE_FINAL.PHONE_TYPE = 'CELL' THEN PHONE_FINAL.PHONE_AREA END) OVER (PARTITION BY PHONE_FINAL.PERSON_UID),
              MAX(CASE WHEN PHONE_FINAL.PHONE_TYPE = 'PR' THEN PHONE_FINAL.PHONE_AREA END) OVER (PARTITION BY PHONE_FINAL.PERSON_UID)) phone_area
       FROM(
              SELECT PHONE_SUB.PERSON_UID,
                        PHONE_SUB.PHONE_TYPE,
                        PHONE_SUB.PHONE_AREA,
                        CONCAT('(', PHONE_SUB.PHONE_AREA, ') ', SUBSTRING(PHONE_SUB.PHONE_NUMBER, 1, 3), '-', SUBSTRING(PHONE_SUB.PHONE_NUMBER, 4)) phone_number,
                        RANK() OVER (PARTITION BY PHONE_SUB.PERSON_UID, PHONE_SUB.PHONE_TYPE ORDER BY PHONE_SUB.PHONE_SEQ_NUMBER DESC) phone_rank
              FROM `rpt_semantic.telephone` PHONE_SUB
              WHERE PHONE_SUB.PHONE_TYPE IN ('CELL','PR')
           AND PHONE_SUB.PHONE_STATUS_IND IS NULL
        AND PHONE_SUB.INSTITUTION = 'WLDN') PHONE_FINAL
       WHERE
              PHONE_FINAL.phone_rank = 1);

CREATE OR REPLACE TEMP TABLE CT_CONTACT AS (
             SELECT
              c.DSI_PC,
              c.CREATEDDATE last_review_date,
              c.CALL_DISPOSITION last_review_subject,
        c.COMMENTS_C
       FROM(
      SELECT
                     a.banner_id_c DSI_PC,
                     T.Subject,
                     T.DESCRIPTION COMMENTS_C,
                     T.CALL_DISPOSITION,
                     T.call_type,
                     T.ACTIVITY_DATE,
                     T.COMPLETED_DATE_TIME,
            --T.COMMENTS_C,
                     DATETIME(T.CREATED_DATE, "US/Central") CREATEDDATE,
                     T.STATUS,
                     RANK() OVER (PARTITION BY a.banner_id_c ORDER BY T.CREATED_DATE DESC) rnk
              FROM `raw_b2c_sfdc.task` T
                     LEFT OUTER JOIN `raw_b2c_sfdc.contact` a  ON t.account_ID = a.account_id
              WHERE CAST(T.activity_date as date)> '2022-01-01'
              and what_id in (select id from raw_b2c_sfdc.opportunity where is_deleted=false AND institution_c in ('a0ko0000002BSH4AAO'))

              ) c
       WHERE c.rnk = 1);

CREATE OR REPLACE TEMP TABLE CT_SFDC AS
(
  WITH ADVISOR AS (
    Select DISTINCT u.ID,
           u.federation_identifier DSI_C,
           u.FIRST_NAME,
           u.LAST_NAME
           --c2.NAME Manager_Name,
                    -- c2.DVTAP_COLLEAGUE_ID_C Manager_DSI
    FROM `raw_b2c_sfdc.user` u
       LEFT OUTER JOIN `raw_b2c_sfdc.user` u2 ON u.MANAGER_ID = u2.ID
       where u._fivetran_deleted =  false
       /*LEFT OUTER JOIN (
        Select c_sub.DVTAP_COLLEAGUE_ID_C,
               c_sub.DVTAP_MANAGER_C,
               c_sub.LAST_MODIFIED_DATE,
               MAX(c_sub.LAST_MODIFIED_DATE) OVER (PARTITION BY c_sub.DVTAP_COLLEAGUE_ID_C) Max_Edit_Date
        FROM `stg_l1_salesforce.dvtap_colleague_c` c_sub) c ON
              u.DSI_C = c.DVTAP_COLLEAGUE_ID_C
        AND c.LAST_MODIFIED_DATE = c.Max_Edit_Date
       LEFT OUTER JOIN `stg_l1_salesforce.dvtap_colleague_c` c2 ON
              c.DVTAP_MANAGER_C = c2.ID*/)
       SELECT
              a.banner_id_c DSI_PC,
              a.ID,
              --a.SSA_FIRST_NAME_PC,
              --a.SSA_LAST_NAME_PC,
    --v.FIRST_NAME SFA_FIRST_NAME,
    --v.LAST_NAME SFA_LAST_NAME,
              a.last_ssa_outreach_attempt_date_c,
    --a.LAST_REVIEWED_SF_C,
    --ADNOTES.notes_c Advising_Notes,
    --SFNOTES.notes_c Finance_Notes,
              --a.INACTIVE_STATUS_DATE_C,
              --a.OUTREACH_FOR_TARGET_TERM_PC,
              --a.ETHNICITY_PC,
              --a.RACE_PC,
 CASE
                     WHEN cast(a.last_ssa_outreach_attempt_date_c as date) >=
                     CASE
                     WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE) = 1
                     THEN date_add(CURRENT_DATE, INTERVAL -3 DAY)
                            ELSE date_add(CURRENT_DATE, INTERVAL -1 DAY)
                     END THEN 'Y'
                     ELSE 'N'
              END reviewed_prior_day,
              CASE
                     WHEN cast(a.last_ssa_outreach_attempt_date_c as date) >= date_add(CURRENT_DATE, INTERVAL -7 DAY) THEN 'Y'
                     ELSE 'N'
              END reviewed_prior_7_days,
              date_diff(CURRENT_DATE, cast(a.last_ssa_outreach_attempt_date_c as date), DAY) days_since_review,
              --a.SSA_PC,
              --a.SFC_PC,
              --u.DSI_C SSA_DSI
              --u.Manager_NAME SSA_Manager_Name,
              --u.Manager_DSI SSA_Manager_DSI,
              --v.DSI_C SFA_DSI,
              --v.Manager_NAME SFA_Manager_Name,
              --v.Manager_DSI SFA_Manager_DSI,
             -- a.auto_register_pc
              FROM `raw_b2c_sfdc.contact` a
  /*LEFT OUTER JOIN ADVISOR u ON
    a.SSA_PC = u.ID
  LEFT OUTER JOIN ADVISOR v ON
    a.SFC_PC = v.ID
 LEFT OUTER JOIN (
    Select ADNOTES_SUB.account_c,
           ADNOTES_SUB.notes_c,
           ADNOTES_SUB.created_date,
           MAX(ADNOTES_SUB.created_date) OVER (PARTITION BY ADNOTES_SUB.account_c) MAX_DATE
    FROM `stg_l1_salesforce.advising_notes_c` ADNOTES_SUB
    WHERE ADNOTES_SUB.NOTES_TYPE_C = 'Academic Advising') ADNOTES
  ON ADNOTES.account_c = a.id
  AND ADNOTES.created_date = ADNOTES.MAX_DATE
  LEFT OUTER JOIN (
    Select SFNOTES_SUB.account_c,
           SFNOTES_SUB.notes_c,
           SFNOTES_SUB.created_date,
           MAX(SFNOTES_SUB.created_date) OVER (PARTITION BY SFNOTES_SUB.account_c) MAX_DATE
    FROM `stg_l1_salesforce.advising_notes_c` SFNOTES_SUB
    WHERE SFNOTES_SUB.NOTES_TYPE_C = 'Student Finance Advising') SFNOTES
  ON SFNOTES.account_c = a.id
  AND SFNOTES.created_date = SFNOTES.MAX_DATE*/
       WHERE a.banner_id_c IS NOT NULL and is_deleted=false and lower(institution_code_c)='walden'
       );

CREATE OR REPLACE TEMP TABLE CT_STUDY AS (
    Select DISTINCT STUDY_SUB.PERSON_UID,
         STUDY_SUB.credential_id DSI,
         STUDY_SUB.ACADEMIC_PERIOD,
         MAX(STUDY_SUB.ACADEMIC_PERIOD) OVER (PARTITION BY STUDY_SUB.PERSON_UID) MAX_ACADEMIC_PERIOD,
         STUDY_SUB.STUDENT_LEVEL,
         STUDY_SUB.STUDENT_LEVEL_DESC,
         STUDY_SUB.INSTITUTION,
         STUDY_SUB.STUDENT_STATUS,
         STUDY_SUB.STUDENT_STATUS_DESC,
         STUDY_SUB.STUDENT_POPULATION,
         STUDY_SUB.PROGRAM,
         STUDY_SUB.FIRST_CONCENTRATION,
         STUDY_SUB.ACADEMIC_PERIOD_ADMITTED,
         STUDY_SUB.NEW_STUDENT_IND,
         STUDY_SUB.CAMPUS_DESC,
         STUDY_SUB.SCPC_CODE,
         STUDY_SUB.ACADEMIC_STANDING,
         STUDY_SUB.ACADEMIC_STANDING_DESC
  FROM `rpt_semantic.academic_study` STUDY_SUB
  INNER JOIN CT_TERM STUDY_TERM
  ON STUDY_SUB.ACADEMIC_PERIOD <= STUDY_TERM.TERM_CODE
  AND STUDY_TERM.TERM_START_DATE <= CURRENT_DATE
  WHERE STUDY_SUB.INSTITUTION = 'WLDN'
  AND STUDY_SUB.PRIMARY_PROGRAM_IND = 'Y');

CREATE OR REPLACE TEMP TABLE CT_COURSE_INFO AS
(
  Select COURSE_SUB.PERSON_UID,
         COURSE_SUB.ACADEMIC_PERIOD,
         COURSE_SUB.COURSE_IDENTIFICATION,
         COURSE_SUB.COURSE_REFERENCE_NUMBER,
         COURSE_AUDIT.FIRST_REG REGISTRATION_STATUS_DATE,
         CASE
         WHEN PAST.PERSON_UID IS NOT NULL THEN 'Y'
         ELSE 'N'
         END REPEAT_COURSE_IND,
         COURSE_SUB.COURSE_LEVEL,
         INSTRUCTOR.PRIMARY_INSTRUCTOR_ID,
         INSTRUCTOR.PRIMARY_INSTRUCTOR_FIRST_NAME || ' ' || INSTRUCTOR.PRIMARY_INSTRUCTOR_LAST_NAME PRIMARY_INSTRUCTOR_NAME,
         --COALESCE(COURSE_CANVAS.student_current_score_avg, 0) COURSE_GRADE,
         --MIN(COALESCE(COURSE_CANVAS.student_current_score_avg, 0)) OVER (PARTITION BY COURSE_SUB.PERSON_UID,COURSE_SUB.ACADEMIC_PERIOD,COURSE_SUB.COURSE_LEVEL) LOWEST_CURRENT_SCORE,
         RANK () OVER (PARTITION BY COURSE_SUB.PERSON_UID,COURSE_SUB.ACADEMIC_PERIOD,COURSE_SUB.COURSE_LEVEL ORDER BY COURSE_SUB.COURSE_REFERENCE_NUMBER) RNK
  FROM CT_COURSE COURSE_SUB
  INNER JOIN rpt_semantic.schedule_offering INSTRUCTOR
  ON COURSE_SUB.ACADEMIC_PERIOD = INSTRUCTOR.ACADEMIC_PERIOD
  AND COURSE_SUB.COURSE_REFERENCE_NUMBER = INSTRUCTOR.COURSE_REFERENCE_NUMBER
  AND COURSE_SUB.INSTITUTION = INSTRUCTOR.INSTITUTION
  LEFT OUTER JOIN (
    Select DISTINCT COURSE_AUDIT_SUB.PERSON_UID,
           COURSE_AUDIT_SUB.credential_id DSI,
           COURSE_AUDIT_SUB.COURSE_REFERENCE_NUMBER,
           COURSE_AUDIT_SUB.ACADEMIC_PERIOD,
           MIN(COURSE_AUDIT_SUB.REGISTRATION_STATUS_DATE) OVER (PARTITION BY COURSE_AUDIT_SUB.credential_id,COURSE_AUDIT_SUB.ACADEMIC_PERIOD,COURSE_AUDIT_SUB.COURSE_REFERENCE_NUMBER) FIRST_REG
    FROM rpt_semantic.student_course_reg_audit COURSE_AUDIT_SUB
    WHERE COURSE_AUDIT_SUB.INSTITUTION = 'WLDN'
    AND COURSE_AUDIT_SUB.REGISTRATION_STATUS LIKE 'R%') COURSE_AUDIT
  ON COURSE_SUB.PERSON_UID = COURSE_AUDIT.PERSON_UID
  AND COURSE_SUB.ACADEMIC_PERIOD = COURSE_AUDIT.ACADEMIC_PERIOD
  AND COURSE_SUB.COURSE_REFERENCE_NUMBER = COURSE_AUDIT.COURSE_REFERENCE_NUMBER
  LEFT OUTER JOIN (
    Select DISTINCT PAST_SUB.PERSON_UID,
           PAST_SUB.COURSE_IDENTIFICATION
    FROM CT_COURSE PAST_SUB
    LEFT OUTER JOIN CURRENT_TERM CUR_TERM
    ON PAST_SUB.ACADEMIC_PERIOD < CUR_TERM.CURRENT_TERM
    WHERE (PAST_SUB.REGISTRATION_STATUS LIKE 'R%' OR PAST_SUB.REGISTRATION_STATUS LIKE 'W%' OR PAST_SUB.REGISTRATION_STATUS LIKE 'P%')) PAST
  ON COURSE_SUB.PERSON_UID = PAST.PERSON_UID
  AND COURSE_SUB.COURSE_IDENTIFICATION = PAST.COURSE_IDENTIFICATION
  WHERE (COURSE_SUB.REGISTRATION_STATUS LIKE 'R%' OR COURSE_SUB.REGISTRATION_STATUS LIKE 'P%')
  AND COURSE_SUB.INSTITUTION = 'WLDN'
  AND COURSE_SUB.GRADE_TYPE <> 'U');

CREATE OR REPLACE TABLE tds_analytics_storage.WLDN_student_detail AS (
Select DISTINCT STUDY.PERSON_UID spriden_pidm,
       STUDY.DSI spriden_id,
       PER.name FULL_Name,
       (STUDY.PERSON_UID||STUDY.STUDENT_LEVEL) course_join_key,
       advisor_review.ID SFDC_ID,
       CASE
       WHEN SPBPERS.SPBPERS_ETHN_CDE = '1' THEN 'Not Hispanic or Latino'
       WHEN SPBPERS.SPBPERS_ETHN_CDE = '2' THEN 'Hispanic or Latino'
       END New_Ethnicity,
       STUDY.STUDENT_STATUS sgbstdn_stst_code,
       STUDY.STUDENT_STATUS_DESC stvstst_desc,
       CASE
          WHEN STUDY.STUDENT_POPULATION IN ('N','T') THEN 'New / First Time'
          WHEN STUDY.STUDENT_POPULATION = 'C' THEN 'Continuing'
          WHEN STUDY.STUDENT_POPULATION = 'A' THEN 'Re-Admit'
          END student_type_verbiage,
       --STUDY.PROGRAM_START,
       STUDY.PROGRAM sgbstdn_program_1,
       STUDY.FIRST_CONCENTRATION sgbstdn_majr_code_conc_1,
       STUDY.STUDENT_LEVEL sgbstdn_levl_code,
       SUBSTRING(STUDY.STUDENT_LEVEL_DESC,13) LEVEL,
       STUDY.ACADEMIC_PERIOD_ADMITTED matriculation_term,
       --SFDC_SESSION.SESSION_C,
       STUDY.NEW_STUDENT_IND new_student_ind,
       STUDY.CAMPUS_DESC stvcamp_desc,
       CASE
       WHEN STUDY.CAMPUS_DESC LIKE '%Online%' THEN 'Online'
       ELSE 'On Campus'
       END TYPE,
       STUDY.SCPC_CODE CYCLE,
       ADDR.CITY residence_city,
       ADDR.STATE_PROVINCE residence_state,
       INT_ADDR.INTERNET_ADDRESS email_address,
       1 TERMS_JOIN,
       PHONE.phone_number phone_number,
          PHONE.phone_area phone_area,
       trunc(GPA.GPA,2) GPA,
          CASE
          WHEN TRUNC(GPA.GPA,2) < 2 THEN 'Under 2.00'
          WHEN TRUNC(GPA.GPA,2) >= 2 AND TRUNC(GPA.GPA,2) < 2.5 THEN '2.00 - 2.49'
          WHEN TRUNC(GPA.GPA,2) >= 2.5 AND TRUNC(GPA.GPA,2) < 3 THEN '2.50 - 2.99'
          WHEN TRUNC(GPA.GPA,2) >= 3 AND TRUNC(GPA.GPA,2) < 3.5 THEN '3.00 - 3.49'
          WHEN TRUNC(GPA.GPA,2) >= 3.5 THEN '3.50 - 4.00'
          END GPA_Range,
          CASE
          WHEN TRUNC(GPA.GPA,2) < 2 THEN '1'
          WHEN TRUNC(GPA.GPA,2) >= 2 AND TRUNC(GPA.GPA,2) < 2.5 THEN '2'
          WHEN TRUNC(GPA.GPA,2) >= 2.5 AND TRUNC(GPA.GPA,2) < 3 THEN '3'
          WHEN TRUNC(GPA.GPA,2) >= 3 AND TRUNC(GPA.GPA,2) < 3.5 THEN '4'
          WHEN TRUNC(GPA.GPA,2) >= 3.5 THEN '5'
          END GPA_Range_Sort,
       STUDY.ACADEMIC_STANDING combined_academic_standing,
       STUDY.ACADEMIC_STANDING_DESC,
       BALANCE.total_balance,
          advisor_review.reviewed_prior_day,
          advisor_review.reviewed_prior_7_days,
          CASE
       WHEN cast(advisor_review.last_ssa_outreach_attempt_date_c as date)  >= TERMS.Current_Term_Start THEN 'Y'
          ELSE 'N'
          END reviewed_since_term_start,
          advisor_review.days_since_review,
          CASE
          WHEN advisor_review.days_since_review <= 1 THEN 'Prior Day'
          WHEN advisor_review.days_since_review >= 2 AND advisor_review.days_since_review <= 7 THEN '2-7 Days'
          WHEN advisor_review.days_since_review >= 8 AND advisor_review.days_since_review <= 30 THEN '8-30 Days'
          WHEN advisor_review.days_since_review >= 31 THEN '31+ Days'
          WHEN advisor_review.days_since_review IS NULL THEN 'No Last Review'
          END days_since_review_bucket,
          CASE
          WHEN advisor_review.days_since_review <= 1 THEN '1'
          WHEN advisor_review.days_since_review >= 2 AND advisor_review.days_since_review <= 7 THEN '2'
          WHEN advisor_review.days_since_review >= 8 AND advisor_review.days_since_review <= 30 THEN'3'
          WHEN advisor_review.days_since_review >= 31 THEN '4'
          WHEN advisor_review.days_since_review IS NULL THEN '5'
          END days_since_review_bucket_sort,
       HOLDS.HOLD_FROM_DATE sprhold_from_date,
          HOLDS.HOLD_TO_DATE sprhold_to_date,
          HOLDS.HOLD sprhold_hldd_code,
          HOLDS.HOLD_DESC stvhldd_desc,
          HOLDS.HOLD_EXPLANATION sprhold_reason,
          HOLDS.finaid_hold,
          MAX(CASE
           WHEN HOLDS.finaid_hold = 'N' THEN 'Yes'
           ELSE 'No'
           END) OVER (PARTITION BY STUDY.PERSON_UID) has_academic_hold,
          MAX(CASE
           WHEN HOLDS.finaid_hold = 'Y' THEN 'Yes'
           ELSE 'No'
           END) OVER (PARTITION BY STUDY.PERSON_UID) has_finaid_hold,
       /*advisor_review.LAST_REVIEWED_C last_reviewed_date,
       advisor_review.LAST_REVIEWED_SF_C,
       advisor_review.Advising_Notes,
       advisor_review.Finance_Notes,*/
       contact.last_review_date last_contact_datetime,
          contact.last_review_subject last_contact_subject,
      /* CASE
          WHEN CURRENT_REG.PERSON_UID IS NOT NULL THEN 'Y'
          ELSE 'N'
          END current_term_registration,
          CASE
          WHEN CURRENT_REG.PERSON_UID IS NOT NULL THEN '1'
          ELSE '2'
          END current_term_registration_sort,
       CASE
          WHEN FUTURE_REG.PERSON_UID IS NOT NULL THEN 'Y'
          ELSE 'N'
          END next_term_registration,
          CASE
          WHEN FUTURE_REG.PERSON_UID IS NOT NULL THEN '1'
          ELSE '2'
          END next_term_registration_sort,
       COALESCE(CURRENT_REG.COURSE_COUNT, 0) + COALESCE(PW.PW_COURSE_COUNT, 0) COURSE_COUNT,
       CASE
       WHEN CAST(LE.LATEST_REG AS DATE) > TERMS.Current_Term_Start THEN 'Y'
       ELSE 'N'
       END HAS_LATE_ENROLLMENT,*/
       COALESCE(COMP_CRED.completed_credits,0) + COALESCE(transfer.Transfer_Course_Credits,0) completed_credits,
       TERMS.Current_Term_Code current_term_code,
       TERMS.Current_Term current_term,
       TERMS.Next_Term_Code next_term_code,
       TERMS.Next_Term next_term,
       contact.COMMENTS_C,
       CASE
       WHEN PAYPLAN.TBRMEMO_PIDM IS NOT NULL THEN 'Y'
       ELSE 'N'
       END Has_Payment_Plan,
FROM `rpt_semantic.academic_study` PER
--Bring in maximum ACADEMIC_STUDY record equal to current term or earlier to form base of report and bring in most relevant student demographic data
INNER JOIN CT_STUDY STUDY
ON STUDY.PERSON_UID = PER.PERSON_UID
AND PER.INSTITUTION = STUDY.INSTITUTION
AND STUDY.ACADEMIC_PERIOD = STUDY.MAX_ACADEMIC_PERIOD
--Temporary join to SPBPERS for SPBPERS_ETHN_CDE value which is not currently modelled
INNER JOIN`raw_wldn_bnr.spbpers` SPBPERS
ON STUDY.PERSON_UID = SPBPERS.SPBPERS_PIDM
--Salesforce query to bring in relevant advisor and student data
LEFT OUTER JOIN CT_SFDC advisor_review ON
       STUDY.DSI = advisor_review.DSI_PC
--Bring in STUDENT_COURSE table to filter base population down to just students who were registered in a course in the last 6 terms.
INNER JOIN (
  Select REG_SUB.PERSON_UID,
         REG_SUB.ACADEMIC_PERIOD
  FROM CT_COURSE REG_SUB
  WHERE
   REG_SUB.ACADEMIC_PERIOD IN (
    Select REG_TERM.TERM_CODE
    FROM (
    Select REG_TERM_SUB.TERM_CODE,
           RANK () OVER (ORDER BY REG_TERM_SUB.TERM_CODE DESC) RNK
    FROM CT_TERM REG_TERM_SUB
    WHERE REG_TERM_SUB.TERM_START_DATE <= CURRENT_DATE) REG_TERM
    WHERE REG_TERM.RNK IN (1,2,3,4,5,6))) REG
ON STUDY.PERSON_UID = REG.PERSON_UID
--Basic address info filtered to highest address seq number
LEFT OUTER JOIN CT_ADDRESS ADDR
ON STUDY.PERSON_UID = ADDR.PERSON_UID
AND ADDR.ADDRESS_NUMBER = ADDR.Max_Address_Number
--Basic email info filtered to most recently edited value
LEFT OUTER JOIN CT_INT_ADDRESS INT_ADDR
ON STUDY.PERSON_UID = INT_ADDR.PERSON_UID
AND INT_ADDR.ACTIVITY_DATE = INT_ADDR.Max_Email_Date
--General subquery to establish current and next term values at time of data pull.
LEFT OUTER JOIN CN_TERMS TERMS
ON TERMS_JOIN = TERMS.TERMS_JOIN
--Basic Phone data giving preference to cellphone values and backing up with personal phone data.
LEFT OUTER JOIN CT_PHONE PHONE
ON STUDY.PERSON_UID = PHONE.PERSON_UID
--Bring in GPA value by person and level of study
LEFT OUTER JOIN (
SELECT DISTINCT GPA_SUB.PERSON_UID,
       GPA_SUB.ACADEMIC_STUDY_VALUE,
       GPA_SUB.GPA
FROM `rpt_semantic.gpa_by_level` GPA_SUB
WHERE GPA_SUB.INSTITUTION = 'WLDN'
AND GPA_SUB.GPA_TYPE = 'I') GPA
ON STUDY.PERSON_UID = GPA.PERSON_UID
AND STUDY.STUDENT_LEVEL = GPA.ACADEMIC_STUDY_VALUE
--Bring in current account balance
LEFT OUTER JOIN (
       SELECT DISTINCT BALANCE_SUB.ACCOUNT_UID,
              SUM(CASE
            WHEN BALANCE_SUB.DETAIL_CODE_TYPE = 'P' THEN BALANCE_SUB.AMOUNT * -1
            ELSE BALANCE_SUB.AMOUNT
            END) OVER (PARTITION BY BALANCE_SUB.ACCOUNT_UID) total_balance
       FROM `rpt_semantic.receivable_account_detail` BALANCE_SUB
    WHERE BALANCE_SUB.INSTITUTION = 'WLDN') BALANCE
ON STUDY.PERSON_UID = BALANCE.ACCOUNT_UID
--Bring in individual academic and financial holds (likely source of additional rows for student)
LEFT OUTER JOIN (
       SELECT DISTINCT HOLD_SUB.PERSON_UID,
                 HOLD_SUB.HOLD_FROM_DATE,
                 HOLD_SUB.HOLD_TO_DATE,
              HOLD_SUB.HOLD,
                 HOLD_SUB.HOLD_DESC,
                 HOLD_SUB.HOLD_EXPLANATION,
                 CASE
                 WHEN HOLD_SUB.AR_HOLD_IND = 'Y' THEN 'Y'
                 ELSE 'N'
                 END finaid_hold,
           HOLD_SUB.HOLD_AMOUNT
       FROM `rpt_semantic.hold` HOLD_SUB
       WHERE CURRENT_DATE BETWEEN EXTRACT(DATE FROM HOLD_SUB.HOLD_FROM_DATE)
    AND COALESCE(EXTRACT(DATE FROM HOLD_SUB.HOLD_TO_DATE),'9999-12-31')
    AND HOLD_SUB.INSTITUTION = 'WLDN') HOLDS
ON STUDY.PERSON_UID = HOLDS.PERSON_UID

--Bring in most recent file review data
LEFT OUTER JOIN CT_CONTACT contact
ON STUDY.DSI = contact.DSI_PC
--Establish if student is registered in current term or not
/*LEFT OUTER JOIN (
  Select DISTINCT CURRENT_REG_SUB.PERSON_UID,
         CURRENT_REG_SUB.COURSE_LEVEL,
         CURRENT_REG_SUB.ACADEMIC_PERIOD,
         COUNT(DISTINCT CURRENT_REG_SUB.COURSE_IDENTIFICATION) OVER (PARTITION BY CURRENT_REG_SUB.PERSON_UID,CURRENT_REG_SUB.COURSE_LEVEL,CURRENT_REG_SUB.ACADEMIC_PERIOD) COURSE_COUNT
  FROM CT_COURSE CURRENT_REG_SUB
  WHERE CURRENT_REG_SUB.REGISTRATION_STATUS LIKE 'R%') CURRENT_REG
ON STUDY.PERSON_UID = CURRENT_REG.PERSON_UID
AND STUDY.STUDENT_LEVEL = CURRENT_REG.COURSE_LEVEL
AND TERMS.Current_Term_Code = CURRENT_REG.ACADEMIC_PERIOD
--Establish if student is registered for next term
LEFT OUTER JOIN (
  Select DISTINCT FUTURE_REG_SUB.PERSON_UID,
         FUTURE_REG_SUB.COURSE_LEVEL,
         FUTURE_REG_SUB.ACADEMIC_PERIOD
  FROM CT_COURSE FUTURE_REG_SUB
  WHERE FUTURE_REG_SUB.REGISTRATION_STATUS LIKE 'R%') FUTURE_REG
ON STUDY.PERSON_UID = FUTURE_REG.PERSON_UID
AND STUDY.STUDENT_LEVEL = FUTURE_REG.COURSE_LEVEL
AND TERMS.Next_Term_Code = FUTURE_REG.ACADEMIC_PERIOD*/
--Bring in total completed credit value by level (not counting transfer credits)
LEFT OUTER JOIN (
  Select DISTINCT COMP_CRED_SUB.PERSON_UID,
         COMP_CRED_SUB.COURSE_LEVEL,
         SUM(COMP_CRED_SUB.CREDITS_EARNED) OVER (PARTITION BY COMP_CRED_SUB.PERSON_UID,COMP_CRED_SUB.COURSE_LEVEL) completed_credits
  FROM CT_COURSE COMP_CRED_SUB
  WHERE COMP_CRED_SUB.INSTITUTION_COURSE_IND = 'Y') COMP_CRED
ON STUDY.PERSON_UID = COMP_CRED.PERSON_UID
AND STUDY.STUDENT_LEVEL = COMP_CRED.COURSE_LEVEL
--Bring in awarded transfer credits by level
LEFT OUTER JOIN (
  Select DISTINCT transfer_sub.person_uid,
         transfer_sub.course_level,
         SUM(transfer_sub.credits_earned) OVER (PARTITION BY transfer_sub.person_uid,transfer_sub.course_level) Transfer_Course_Credits
  FROM CT_COURSE transfer_sub
  WHERE transfer_sub.institution_course_ind = 'N'
  AND transfer_sub.in_history_tbl_ind = 'Y') transfer
ON STUDY.PERSON_UID = transfer.person_uid
AND STUDY.STUDENT_LEVEL = transfer.course_level
LEFT OUTER JOIN CT_COURSE_INFO COURSE
ON STUDY.PERSON_UID = COURSE.PERSON_UID
AND STUDY.STUDENT_LEVEL = COURSE.COURSE_LEVEL
AND COURSE.ACADEMIC_PERIOD = TERMS.Current_Term_Code
LEFT OUTER JOIN (
  Select DISTINCT PAYPLAN_SUB.TBRMEMO_PIDM,
         PAYPLAN_SUB.TBRMEMO_TERM_CODE
  FROM `raw_wldn_bnr.tbrmemo` PAYPLAN_SUB
  WHERE PAYPLAN_SUB.TBRMEMO_DETAIL_CODE = 'ZTNP') PAYPLAN
ON STUDY.PERSON_UID = PAYPLAN.TBRMEMO_PIDM
AND TERMS.Current_Term_Code <= PAYPLAN.TBRMEMO_TERM_CODE

--Bring in First Registration Date for each course to see if student has late enrollment
/*LEFT OUTER JOIN (
  Select DISTINCT LE_FINAL.PERSON_UID,
         LE_FINAL.DSI,
         MAX(LE_FINAL.FIRST_COURSE_REG) OVER (PARTITION BY LE_FINAL.DSI) LATEST_REG
  FROM (
    Select DISTINCT LE_SUB.PERSON_UID,
           LE_SUB.DSI,
           LE_SUB.institution,
           LE_SUB.ACADEMIC_PERIOD,
           MIN(LE_SUB.REGISTRATION_STATUS_DATE) OVER (PARTITION BY LE_SUB.DSI,LE_SUB.ACADEMIC_PERIOD,LE_SUB.COURSE_REFERENCE_NUMBER) FIRST_COURSE_REG
    FROM rpt_semantic.student_course_reg_audit LE_SUB
    WHERE LE_SUB.institution ='WLDN'
    LEFT OUTER JOIN CURRENT_TERM LE_TERM
    ON LE_SUB.ACADEMIC_PERIOD = LE_TERM.CURRENT_TERM
    LEFT OUTER JOIN CT_COURSE LE_COURSE
    ON LE_SUB.PERSON_UID = LE_COURSE.PERSON_UID
    AND LE_SUB.ACADEMIC_PERIOD = LE_COURSE.ACADEMIC_PERIOD
    AND LE_SUB.COURSE_REFERENCE_NUMBER = LE_COURSE.COURSE_REFERENCE_NUMBER
    WHERE LE_SUB.INSTITUTION = 'WLDN'
    AND LE_SUB.REGISTRATION_STATUS LIKE 'R%'
    --AND LE_COURSE.SCHEDULE_TYPE_DESC NOT IN ('Clinical','Lab','Precepted')
    ) LE_FINAL) LE
ON STUDY.PERSON_UID = LE.PERSON_UID*/
WHERE STUDY.INSTITUTION = 'WLDN'

);

/*End PBI Query*/
END