CREATE OR REPLACE PROCEDURE `trans_academics.sp_wldn_reserve_list`(IN v_audit_key STRING, OUT result STRING)
begin

declare institution string default 'WLDN';
declare institution_id int64 default 5;
declare dml_mode string default 'scd1'; ----#DialyInsert/Incremental
declare target_dataset string default 'rpt_academics';
declare target_tablename string default 't_wldn_reserve_list';
declare source_tablename string default 'wldn_reserve_list';
declare load_source string default 'trans_academics.sp_wldn_reserve_list';
declare additional_attributes ARRAY<struct<keyword string, value string>>;
declare last_refresh_time timestamp;
declare tgt_table_count int64;

/* common across */
declare job_start_dt timestamp default current_timestamp();
declare job_end_dt timestamp default current_timestamp();
declare job_completed_ind string default null;
declare job_type string default 'ODS';
declare load_method string default 'scheduled query';
declare out_sql string;

begin
SET additional_attributes= [("audit_load_key", v_audit_key),
              ("load_method",load_method),
              ("load_source",load_source),
              ("job_type", job_type)];
    /* end common across */



CREATE or replace TEMP TABLE PFS_TRK_and_PFS_AWARD
AS (
  SELECT PFS_AWARD.student_token
			,PFS_AWARD.tracking_status
			,PFS_AWARD.award_year_token
			,CASE WHEN PFS_AWARD.tracking_status = 'IP' THEN 'Incomplete FA Application'
					WHEN PFS_AWARD.tracking_status = 'RP' THEN 'Completed FA Application'
					WHEN PFS_AWARD.tracking_status in ('AW','AR','ID','DS') THEN 'Awarded FA,  missing info'
					WHEN PFS_AWARD.tracking_status in ('RD', 'RR', 'DM') THEN 'Awarded FA, complete'
					WHEN PFS_AWARD.tracking_status in ('SC', 'NA', 'DA', 'HL') THEN 'Not eligible or declined aid'
					WHEN PFS_AWARD.tracking_status IS NULL THEN 'NO FA Application Yet'
					ELSE 'Completed FA Application'
				END AS FA_APP_STATUS
			,CASE PFS_AWARD.tracking_status WHEN 'IP' then 1 WHEN 'RP' then 2 WHEN 'AW' then 3 WHEN 'AR' then 4
							WHEN 'ID' then 5 WHEN 'RD' then 6 WHEN 'DM' then 7 WHEN 'PD' then 8
							ELSE 0
				END AS Tracking_Status_Rank

		FROM
		`raw_wldn_pfaids.stu_award_year` PFS_AWARD
);

CREATE TEMP TABLE StuCourse
AS (
SELECT
      BNR_Person_Credential_Id
      ,Course_Start_Dt_Quarter
      ,Course_Reference_Nbr
      ,Course_Identification
	    ,Course_Academic_Period
	    ,Course_Section_Nbr
	   -- ,ROW_NUMBER() OVER (PARTITION BY BNR_Person_Credential_Id, Course_Start_Dt_Quarter ORDER BY Course_Identification,cast(course_Section_Nbr as int) asc ) RANK_StuCourse
		 # course scetion number is comng as varchar values hence rownumber changed as below
		 ,ROW_NUMBER() OVER (PARTITION BY BNR_Person_Credential_Id, Course_Start_Dt_Quarter ORDER BY Course_Identification,cast(REGEXP_REPLACE(course_Section_Nbr, r'[a-zA-Z]', '0') as int) asc ) RANK_StuCourse

 FROM `trans_academics.v_wldn_reserve_list_stu_course`
--WHERE BNR_Person_Credential_Id = 'A01042018'
);
CREATE TEMP TABLE Course_Start_Dt_Quarter_Previous
AS
(SELECT DISTINCT --TOP 100
				MAIN.PERSON_UID,
				 MAIN.Student_Start_Dt AS Student_Start_Dt
				,MAIN.Credential_Id AS Applicant_Id
				,MAIN.NAME
				,IFNULL(MAIN.SF_EA,'Unknown') AS AdmRepName
				,coalesce (MAIN.SF_UserManager , MAIN.SF_EnrollmentManager,'Unknown') AS Manager
				,SF_Division as Division
				,Director
				,IFNULL(MAIN.SF_Location,'Unknown') AS EA_Location
				-- ,MAIN.c_College_name AS College
				,CASE WHEN MAIN.c_Academic_Program LIKE '%UVM%' THEN 'LIU PARTNERS' ELSE MAIN.c_College_name END AS College
				,MAIN.c_Audience_Name AS Audience_Name
				,MAIN.c_Level_Desc AS Degree_Level
				,MAIN.c_Program_Name AS Program_Name
				,MAIN.c_Banner_Conc_Desc AS Banner_Conc_Desc
				,MAIN.c_Academic_Program AS Academic_Program
				,MAIN.Address_Type
				,MAIN.Address_line_1
				,MAIN.Address_line_2
				,MAIN.City
				,MAIN.State13 AS State
				,MAIN.country AS Country
				,MAIN.Zip_Cd
				,IFNULL(MAIN.EA_Region_Final,'Unknown') AS EA_Region
				,CASE  WHEN MAIN.Student_Population = 'A' THEN 'Readmit'
						WHEN MAIN.Student_Population = 'B' THEN 'Bridge'
						WHEN MAIN.Student_Population = 'C' THEN 'Continuing'
						WHEN MAIN.Student_Population = 'F' THEN 'New First Time Freshman'
						WHEN MAIN.Student_Population = 'H' THEN 'Place Holder'
						WHEN MAIN.Student_Population = 'N' THEN 'Network'
						WHEN MAIN.Student_Population = 'P' THEN 'New Degree Previous Grad'
						WHEN MAIN.Student_Population = 'R' THEN 'Reinstatement'
						WHEN MAIN.Student_Population = 'S' THEN 'New Student'
						WHEN MAIN.Student_Population = 'T' THEN 'Transfer Undergrad'
						WHEN MAIN.Student_Population = 'U' THEN 'Undergrad Change of Program'
						WHEN MAIN.Student_Population = 'W' THEN 'Continuing Network Student'
						WHEN MAIN.Student_Population = 'X' THEN 'Change of Program'
						WHEN MAIN.Student_Population = 'Z' THEN 'Data Migration'
						ELSE ' '
						END AS Student_Type
				,MAIN.Application_Dt AS Application_Dt
				,MAIN.Status_Dt AS Current_Status_Dt
				,MAIN.Deferred27 AS Deferred
				,MAIN.Prior_Start_Dt AS Prior_Start_Dt
				,CASE WHEN PFS_TRK_and_PFS_AWARD.tracking_status = 'IP' THEN 'Incomplete FA Application'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status = 'RP' THEN 'Completed FA Application'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status in ('AW','AR','ID','DS') THEN 'Awarded FA,  missing info'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status in ('RD', 'RR', 'DM') THEN 'Awarded FA, complete'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status in ('SC', 'NA', 'DA', 'HL') THEN 'Not eligible or declined aid'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status IS NULL THEN 'No FA Application Yet'
							ELSE 'Completed FA Application'
						END AS FA_App_Status
				,PFS_TRK_and_PFS_AWARD.tracking_status AS Tracking_Status
				,MAIN.Term_Cd AS Term_Cd
				,PFS_TRK_and_PFS_AWARD.award_year_token AS Fin_Aid_Year
				-- ,MAIN.FinAid_Year AS Fin_Aid_Year
				,Course_1
				,Course_2
				,Course_3
				,Course_4
				,Course_5
				,Course_6
				,MAIN.Phone_Nbr_Home
				,MAIN.Phone_Nbr_Business
				,MAIN.Email AS Main_Email
				,MAIN.Personal_Email AS Personal_Email
			--	,MAIN.Application_ID,
				,concat(MAIN.credential_id,'-',MAIN.APPLICATION_NUMBER,'-',MAIN.term_cd)Application_ID
				--,CONCAT(MAIN.Credential_Id,'-',SUBSTRING(MAIN.Application_ID,STRPOS(MAIN.Application_ID, '-')+1, STRPOS(MAIN.Application_ID, '-', STRPOS( MAIN.Application_ID, '-') + 1) - (STRPOS( MAIN.Application_ID, '-')+1)),'-',MAIN.Term_Cd) AS Application_ID
				,MAIN.OppSfid
				,MAIN.SGASTDN_Program
				,Course_Reference_Nbr_1
				,Course_Reference_Nbr_2
				,Course_Reference_Nbr_3
				,Course_Reference_Nbr_4
				,Course_Reference_Nbr_5
				,Course_Reference_Nbr_6
				,Course_Academic_Period_1
				,Course_Academic_Period_2
				,Course_Academic_Period_3
				,Course_Academic_Period_4
				,Course_Academic_Period_5
				,Course_Academic_Period_6
				,Course_Section_Nbr_1
				,Course_Section_Nbr_2
				,Course_Section_Nbr_3
				,Course_Section_Nbr_4
				,Course_Section_Nbr_5
				,Course_Section_Nbr_6
				,CASE WHEN MAIN.Student_Start_Dt_Quarter = 'previous' THEN 'Previous Quarter'
					  WHEN MAIN.Student_Start_Dt_Quarter = '0' THEN 'Present Quarter'
					  WHEN MAIN.Student_Start_Dt_Quarter = '1' THEN 'Future Quarter'
					  WHEN MAIN.Student_Start_Dt_Quarter = 'further 2' THEN 'Further 2 Future Quarters'
					END AS Selected_Quarter
				,campaign_id
				,Channel
				,SQ_Flag
				,Days_RS_Start_CR_StatusDt

FROM ( SELECT	GEN_STU.PERSON_UID, GEN_STU.Student_Population,	GEN_STU.SGASTDN_Program,	GEN_STU.Credential_Id, 	Application_Id, OppSfid, SO.campaign_id, c.channel_c as channel, SF_Location, Region,SF_EA, SF_UserManager, SF_EnrollmentManager,Director,
				SF_EA_InternationalFlag, SF_Division, SF_Team, EA_Region_Final, GEN_STU.Name,
				IFNULL(OPP_APP.c_Academic_Program,GEN_STU.c_Academic_Program) AS c_Academic_Program,
				IFNULL(OPP_APP.c_Audience_Name,GEN_STU.c_Audience_Name) AS c_Audience_Name,
				IFNULL(OPP_APP.c_Program_Name,GEN_STU.c_Program_Name) AS c_Program_Name,
				IFNULL(OPP_APP.c_Banner_Conc_Desc,GEN_STU.c_Banner_Conc_Desc) AS c_Banner_Conc_Desc, Current_Status,
				Address_Type, Address_line_1, Address_line_2, City, State13, country, country_desc, Zip_Cd,  Student_Start_Dt, Term_Cd,  Application_Dt,   5 AS Institution_ID, 'WLDN' AS Institution,
				Status_Dt  , Student_Start_Dt_Quarter, Application_Dt_Quarter ,
				FinAid_Year,
				 Deferred27, Prior_Start_Dt, Phone_Nbr_Home, Phone_Nbr_Business, Personal_Email, Email,
				IFNULL(OPP_APP.c_College_name,GEN_STU.c_College_name) AS c_College_name,
				IFNULL(safe_cast(OPP_APP.c_Level_Desc as string),GEN_STU.c_Level_Desc) AS c_Level_Desc,
				Case when Substring(cast(Term_Cd as string), 5, 1) in ('1','3','5','7') then 'Q'
				when Substring(cast(Term_Cd as string), 5, 1) in ('2','4','6') then 'S'
				End as SQ_Flag,
				date_diff(cast(Student_Start_Dt as date), cast(Status_Dt as date), day) as Days_RS_Start_CR_StatusDt, GEN_STU.APPLICATION_NUMBER
			FROM `trans_academics.wldn_reserve_list_gen_student` GEN_STU
			LEFT JOIN `trans_academics.wldn_reserve_list_opp_app` OPP_APP
			ON GEN_STU.Credential_Id = OPP_APP.Credential_Id
			AND GEN_STU.APPLICATION_NUMBER = OPP_APP.APPLICATION_NUMBER
			and GEN_STU.institution_id =5
			and OPP_APP.institution_id=5
			LEFT JOIN `raw_b2c_sfdc.opportunity` SO ON OPP_APP.OppSfid = So.ID and SO.is_deleted = false and SO.institution_c='a0ko0000002BSH4AAO'
			left join `raw_b2c_sfdc.campaign` C on SO.campaign_id = c.id  and C.is_deleted = false and C.institution_c='a0ko0000002BSH4AAO'
			WHERE
			COALESCE(OPP_APP.c_Program_Name,GEN_STU.c_Program_Name,'UNKNOWN') NOT LIKE '%LIU%'
			)  MAIN

LEFT JOIN (
	SELECT BNR_Person_Credential_Id
		,MAX(Course_Start_Dt_Quarter) AS Course_Start_Dt_Quarter

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Identification ELSE NULL END) AS Course_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Identification ELSE NULL END) AS Course_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Identification ELSE NULL END) AS Course_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Identification ELSE NULL END) AS Course_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Identification ELSE NULL END) AS Course_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Identification ELSE NULL END) AS Course_6

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_6

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_6

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_6

  FROM StuCourse
  WHERE RANK_StuCourse < 7
  AND Course_Start_Dt_Quarter = 'previous'
  GROUP BY BNR_Person_Credential_Id
	 ) STUCOURSE
ON MAIN.Credential_Id = STUCOURSE.BNR_Person_Credential_Id

LEFT JOIN `raw_wldn_pfaids.student` PFS_STU
  ON MAIN.Credential_Id = PFS_STU.alternate_id
LEFT JOIN PFS_TRK_and_PFS_AWARD
  ON PFS_TRK_and_PFS_AWARD.student_token = PFS_STU.student_token
  AND PFS_TRK_and_PFS_AWARD.award_year_token = MAIN.FinAid_Year
WHERE MAIN.Institution_ID = 5
 AND MAIN.Student_Start_Dt IS NOT NULL
 AND MAIN.Student_Start_Dt_Quarter = 'previous'

-- AND MAIN.Credential_Id = 'A01042492'
);

CREATE TEMP TABLE Course_Start_Dt_Quarter_0 AS
(
SELECT DISTINCT --TOP 100
         MAIN.PERSON_UID,
				 MAIN.Student_Start_Dt AS Student_Start_Dt
				,MAIN.Credential_Id AS Applicant_Id
				,MAIN.NAME
				,IFNULL(MAIN.SF_EA,'Unknown') AS AdmRepName
				,coalesce (MAIN.SF_UserManager , MAIN.SF_EnrollmentManager,'Unknown') AS Manager
				,SF_Division as Division
				,Director
				,IFNULL(MAIN.SF_Location,'Unknown') AS EA_Location
				-- ,MAIN.c_College_name AS College
				,CASE WHEN MAIN.c_Academic_Program LIKE '%UVM%' THEN 'LIU PARTNERS' ELSE MAIN.c_College_name END AS College
				,MAIN.c_Audience_Name AS Audience_Name
				,MAIN.c_Level_Desc AS Degree_Level
				,MAIN.c_Program_Name AS Program_Name
				,MAIN.c_Banner_Conc_Desc AS Banner_Conc_Desc
				,MAIN.c_Academic_Program AS Academic_Program
				,MAIN.Address_Type
				,MAIN.Address_line_1
				,MAIN.Address_line_2
				,MAIN.City
				,MAIN.State13 AS State
				,MAIN.country AS Country
				,MAIN.Zip_Cd
				,IFNULL(MAIN.EA_Region_Final,'Unknown') AS EA_Region
				,CASE  WHEN MAIN.Student_Population = 'A' THEN 'Readmit'
						WHEN MAIN.Student_Population = 'B' THEN 'Bridge'
						WHEN MAIN.Student_Population = 'C' THEN 'Continuing'
						WHEN MAIN.Student_Population = 'F' THEN 'New First Time Freshman'
						WHEN MAIN.Student_Population = 'H' THEN 'Place Holder'
						WHEN MAIN.Student_Population = 'N' THEN 'Network'
						WHEN MAIN.Student_Population = 'P' THEN 'New Degree Previous Grad'
						WHEN MAIN.Student_Population = 'R' THEN 'Reinstatement'
						WHEN MAIN.Student_Population = 'S' THEN 'New Student'
						WHEN MAIN.Student_Population = 'T' THEN 'Transfer Undergrad'
						WHEN MAIN.Student_Population = 'U' THEN 'Undergrad Change of Program'
						WHEN MAIN.Student_Population = 'W' THEN 'Continuing Network Student'
						WHEN MAIN.Student_Population = 'X' THEN 'Change of Program'
						WHEN MAIN.Student_Population = 'Z' THEN 'Data Migration'
						ELSE ' '
						END AS Student_Type
				,MAIN.Application_Dt AS Application_Dt
				,MAIN.Status_Dt AS Current_Status_Dt
				,MAIN.Deferred27 AS Deferred
				,MAIN.Prior_Start_Dt AS Prior_Start_Dt
				,CASE WHEN PFS_TRK_and_PFS_AWARD.tracking_status = 'IP' THEN 'Incomplete FA Application'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status = 'RP' THEN 'Completed FA Application'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status in ('AW','AR','ID','DS') THEN 'Awarded FA,  missing info'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status in ('RD', 'RR', 'DM') THEN 'Awarded FA, complete'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status in ('SC', 'NA', 'DA', 'HL') THEN 'Not eligible or declined aid'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status IS NULL THEN 'No FA Application Yet'
							ELSE 'Completed FA Application'
						END AS FA_App_Status
				,PFS_TRK_and_PFS_AWARD.tracking_status AS Tracking_Status
				,MAIN.Term_Cd AS Term_Cd
				,PFS_TRK_and_PFS_AWARD.award_year_token AS Fin_Aid_Year
				-- ,MAIN.FinAid_Year AS Fin_Aid_Year
				,Course_1
				,Course_2
				,Course_3
				,Course_4
				,Course_5
				,Course_6
				,MAIN.Phone_Nbr_Home
				,MAIN.Phone_Nbr_Business
				,MAIN.Email AS Main_Email
				,MAIN.Personal_Email AS Personal_Email
				--,MAIN.Application_ID
				--,CONCAT(MAIN.Credential_Id,'-',SUBSTRING(MAIN.Application_ID,STRPOS(MAIN.Application_ID, '-')+1, STRPOS(MAIN.Application_ID, '-', STRPOS( MAIN.Application_ID, '-') + 1) - (STRPOS( MAIN.Application_ID, '-')+1)),'-',MAIN.Term_Cd) AS Application_ID
				,concat(MAIN.credential_id,'-',MAIN.APPLICATION_NUMBER,'-',MAIN.term_cd)Application_ID
				,MAIN.OppSfid
				,MAIN.SGASTDN_Program
				,Course_Reference_Nbr_1
				,Course_Reference_Nbr_2
				,Course_Reference_Nbr_3
				,Course_Reference_Nbr_4
				,Course_Reference_Nbr_5
				,Course_Reference_Nbr_6
				,Course_Academic_Period_1
				,Course_Academic_Period_2
				,Course_Academic_Period_3
				,Course_Academic_Period_4
				,Course_Academic_Period_5
				,Course_Academic_Period_6
				,Course_Section_Nbr_1
				,Course_Section_Nbr_2
				,Course_Section_Nbr_3
				,Course_Section_Nbr_4
				,Course_Section_Nbr_5
				,Course_Section_Nbr_6
				,CASE WHEN MAIN.Student_Start_Dt_Quarter = 'previous' THEN 'Previous Quarter'
					  WHEN MAIN.Student_Start_Dt_Quarter = '0' THEN 'Present Quarter'
					  WHEN MAIN.Student_Start_Dt_Quarter = '1' THEN 'Future Quarter'
					  WHEN MAIN.Student_Start_Dt_Quarter = 'further 2' THEN 'Further 2 Future Quarters'
					END AS Selected_Quarter
				,campaign_id
				,Channel
				,SQ_Flag
				,Days_RS_Start_CR_StatusDt

FROM ( SELECT	GEN_STU.PERSON_UID, GEN_STU.Student_Population,	GEN_STU.SGASTDN_Program,	GEN_STU.Credential_Id, 	Application_Id, OppSfid, SO.campaign_id, c.channel_c as channel, SF_Location, Region,SF_EA, SF_UserManager, SF_EnrollmentManager,Director,
				SF_EA_InternationalFlag, SF_Division, SF_Team, EA_Region_Final, GEN_STU.Name,
				IFNULL(OPP_APP.c_Academic_Program,GEN_STU.c_Academic_Program) AS c_Academic_Program,
				IFNULL(OPP_APP.c_Audience_Name,GEN_STU.c_Audience_Name) AS c_Audience_Name,
				IFNULL(OPP_APP.c_Program_Name,GEN_STU.c_Program_Name) AS c_Program_Name,
				IFNULL(OPP_APP.c_Banner_Conc_Desc,GEN_STU.c_Banner_Conc_Desc) AS c_Banner_Conc_Desc, Current_Status,
				Address_Type, Address_line_1, Address_line_2, City, State13, country, country_desc, Zip_Cd,  Student_Start_Dt, Term_Cd,  Application_Dt,  5 AS Institution_ID, 'WLDN' AS Institution,
				Status_Dt  , Student_Start_Dt_Quarter, Application_Dt_Quarter ,
				FinAid_Year,
				 Deferred27, Prior_Start_Dt, Phone_Nbr_Home, Phone_Nbr_Business, Personal_Email, Email,
				IFNULL(OPP_APP.c_College_name,GEN_STU.c_College_name) AS c_College_name,
				IFNULL(safe_cast(OPP_APP.c_Level_Desc as string),GEN_STU.c_Level_Desc) AS c_Level_Desc,
				Case when Substring(cast(Term_Cd as string), 5, 1) in ('1','3','5','7') then 'Q'
				when Substring(cast(Term_Cd as string), 5, 1) in ('2','4','6') then 'S'
				End as SQ_Flag,
				date_diff(cast(Student_Start_Dt as date), cast(Status_Dt as date), day) as Days_RS_Start_CR_StatusDt, GEN_STU.APPLICATION_NUMBER
			FROM `trans_academics.wldn_reserve_list_gen_student` GEN_STU
			LEFT JOIN `trans_academics.wldn_reserve_list_opp_app` OPP_APP
			ON GEN_STU.Credential_Id = OPP_APP.Credential_Id
			AND GEN_STU.APPLICATION_NUMBER = OPP_APP.APPLICATION_NUMBER
				and GEN_STU.institution_id =5
			and OPP_APP.institution_id=5
			LEFT JOIN `raw_b2c_sfdc.opportunity` SO ON OPP_APP.OppSfid = So.ID and SO.is_deleted = false and SO.institution_c='a0ko0000002BSH4AAO'
			left join `raw_b2c_sfdc.campaign` C on SO.campaign_id = c.id and C.is_deleted = false and C.institution_c='a0ko0000002BSH4AAO'
			WHERE COALESCE(OPP_APP.c_Program_Name,GEN_STU.c_Program_Name,'UNKNOWN') NOT LIKE '%LIU%'
			)  MAIN

LEFT JOIN (
	SELECT BNR_Person_Credential_Id
		,MAX(Course_Start_Dt_Quarter) AS Course_Start_Dt_Quarter

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Identification ELSE NULL END) AS Course_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Identification ELSE NULL END) AS Course_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Identification ELSE NULL END) AS Course_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Identification ELSE NULL END) AS Course_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Identification ELSE NULL END) AS Course_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Identification ELSE NULL END) AS Course_6

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_6

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_6

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_6

  FROM StuCourse
  WHERE RANK_StuCourse < 7
  AND Course_Start_Dt_Quarter = '0'
  GROUP BY BNR_Person_Credential_Id
	 ) STUCOURSE
ON MAIN.Credential_Id = STUCOURSE.BNR_Person_Credential_Id

LEFT JOIN `raw_wldn_pfaids.student` PFS_STU
  ON MAIN.Credential_Id = PFS_STU.alternate_id
LEFT JOIN PFS_TRK_and_PFS_AWARD
  ON PFS_TRK_and_PFS_AWARD.student_token = PFS_STU.student_token
  AND PFS_TRK_and_PFS_AWARD.award_year_token = MAIN.FinAid_Year

 WHERE MAIN.Institution_ID = 5
 AND MAIN.Student_Start_Dt IS NOT NULL
 AND MAIN.Student_Start_Dt_Quarter = '0'

-- AND MAIN.Credential_Id = 'A01042492'
);

CREATE TEMP TABLE Course_Start_Dt_Quarter_1 AS
(
SELECT DISTINCT --TOP 100
         MAIN.PERSON_UID,
				 MAIN.Student_Start_Dt AS Student_Start_Dt
				,MAIN.Credential_Id AS Applicant_Id
				,MAIN.NAME
				,IFNULL(MAIN.SF_EA,'Unknown') AS AdmRepName
				,coalesce (MAIN.SF_UserManager , MAIN.SF_EnrollmentManager,'Unknown') AS Manager
				,SF_Division as Division
				,Director
				,IFNULL(MAIN.SF_Location,'Unknown') AS EA_Location
				--,MAIN.c_College_name AS College
				,CASE WHEN MAIN.c_Academic_Program LIKE '%UVM%' THEN 'LIU PARTNERS' ELSE MAIN.c_College_name END AS College
				,MAIN.c_Audience_Name AS Audience_Name
				,MAIN.c_Level_Desc AS Degree_Level
				,MAIN.c_Program_Name AS Program_Name
				,MAIN.c_Banner_Conc_Desc AS Banner_Conc_Desc
				,MAIN.c_Academic_Program AS Academic_Program
				,MAIN.Address_Type
				,MAIN.Address_line_1
				,MAIN.Address_line_2
				,MAIN.City
				,MAIN.State13 AS State
				,MAIN.country AS Country
				,MAIN.Zip_Cd
				,IFNULL(MAIN.EA_Region_Final,'Unknown') AS EA_Region
				,CASE  WHEN MAIN.Student_Population = 'A' THEN 'Readmit'
						WHEN MAIN.Student_Population = 'B' THEN 'Bridge'
						WHEN MAIN.Student_Population = 'C' THEN 'Continuing'
						WHEN MAIN.Student_Population = 'F' THEN 'New First Time Freshman'
						WHEN MAIN.Student_Population = 'H' THEN 'Place Holder'
						WHEN MAIN.Student_Population = 'N' THEN 'Network'
						WHEN MAIN.Student_Population = 'P' THEN 'New Degree Previous Grad'
						WHEN MAIN.Student_Population = 'R' THEN 'Reinstatement'
						WHEN MAIN.Student_Population = 'S' THEN 'New Student'
						WHEN MAIN.Student_Population = 'T' THEN 'Transfer Undergrad'
						WHEN MAIN.Student_Population = 'U' THEN 'Undergrad Change of Program'
						WHEN MAIN.Student_Population = 'W' THEN 'Continuing Network Student'
						WHEN MAIN.Student_Population = 'X' THEN 'Change of Program'
						WHEN MAIN.Student_Population = 'Z' THEN 'Data Migration'
						ELSE ' '
						END AS Student_Type
				,MAIN.Application_Dt AS Application_Dt
				,MAIN.Status_Dt AS Current_Status_Dt
				,MAIN.Deferred27 AS Deferred
				,MAIN.Prior_Start_Dt AS Prior_Start_Dt
				,CASE WHEN PFS_TRK_and_PFS_AWARD.tracking_status = 'IP' THEN 'Incomplete FA Application'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status = 'RP' THEN 'Completed FA Application'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status in ('AW','AR','ID','DS') THEN 'Awarded FA,  missing info'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status in ('RD', 'RR', 'DM') THEN 'Awarded FA, complete'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status in ('SC', 'NA', 'DA', 'HL') THEN 'Not eligible or declined aid'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status IS NULL THEN 'No FA Application Yet'
							ELSE 'Completed FA Application'
						END AS FA_App_Status
				,PFS_TRK_and_PFS_AWARD.tracking_status AS Tracking_Status
				,MAIN.Term_Cd AS Term_Cd
				,PFS_TRK_and_PFS_AWARD.award_year_token AS Fin_Aid_Year
				-- ,MAIN.FinAid_Year AS Fin_Aid_Year
				,Course_1
				,Course_2
				,Course_3
				,Course_4
				,Course_5
				,Course_6
				,MAIN.Phone_Nbr_Home
				,MAIN.Phone_Nbr_Business
				,MAIN.Email AS Main_Email
				,MAIN.Personal_Email AS Personal_Email
				--,MAIN.Application_ID
				--,CONCAT(MAIN.Credential_Id,'-',SUBSTRING(MAIN.Application_ID,STRPOS(MAIN.Application_ID, '-')+1, STRPOS(MAIN.Application_ID, '-', STRPOS( MAIN.Application_ID, '-') + 1) - (STRPOS( MAIN.Application_ID, '-')+1)),'-',MAIN.Term_Cd) AS Application_ID
				,concat(MAIN.credential_id,'-',MAIN.APPLICATION_NUMBER,'-',MAIN.term_cd)Application_ID
				,MAIN.OppSfid
				,MAIN.SGASTDN_Program
				,Course_Reference_Nbr_1
				,Course_Reference_Nbr_2
				,Course_Reference_Nbr_3
				,Course_Reference_Nbr_4
				,Course_Reference_Nbr_5
				,Course_Reference_Nbr_6
				,Course_Academic_Period_1
				,Course_Academic_Period_2
				,Course_Academic_Period_3
				,Course_Academic_Period_4
				,Course_Academic_Period_5
				,Course_Academic_Period_6
				,Course_Section_Nbr_1
				,Course_Section_Nbr_2
				,Course_Section_Nbr_3
				,Course_Section_Nbr_4
				,Course_Section_Nbr_5
				,Course_Section_Nbr_6
				,CASE WHEN MAIN.Student_Start_Dt_Quarter = 'previous' THEN 'Previous Quarter'
					  WHEN MAIN.Student_Start_Dt_Quarter = '0' THEN 'Present Quarter'
					  WHEN MAIN.Student_Start_Dt_Quarter = '1' THEN 'Future Quarter'
					  WHEN MAIN.Student_Start_Dt_Quarter = 'further 2' THEN 'Further 2 Future Quarters'
					END AS Selected_Quarter
				,campaign_id
				,Channel
				,SQ_Flag
				,Days_RS_Start_CR_StatusDt


FROM ( SELECT	GEN_STU.PERSON_UID, GEN_STU.Student_Population,	GEN_STU.SGASTDN_Program, GEN_STU.Credential_Id, 	Application_Id, OppSfid, SO.campaign_id, c.channel_c as channel, SF_Location, Region,SF_EA, SF_UserManager, SF_EnrollmentManager,Director,
				SF_EA_InternationalFlag, SF_Division, SF_Team, EA_Region_Final, GEN_STU.Name,
				IFNULL(OPP_APP.c_Academic_Program,GEN_STU.c_Academic_Program) AS c_Academic_Program,
				IFNULL(OPP_APP.c_Audience_Name,GEN_STU.c_Audience_Name) AS c_Audience_Name,
				IFNULL(OPP_APP.c_Program_Name,GEN_STU.c_Program_Name) AS c_Program_Name,
				IFNULL(OPP_APP.c_Banner_Conc_Desc,GEN_STU.c_Banner_Conc_Desc) AS c_Banner_Conc_Desc, Current_Status,
				Address_Type, Address_line_1, Address_line_2, City, State13, country, country_desc, Zip_Cd,  Student_Start_Dt, Term_Cd,  Application_Dt,   5 AS Institution_ID, 'WLDN' AS Institution,
				Status_Dt  , Student_Start_Dt_Quarter, Application_Dt_Quarter ,
				FinAid_Year,
				 Deferred27, Prior_Start_Dt, Phone_Nbr_Home, Phone_Nbr_Business, Personal_Email, Email,
				IFNULL(OPP_APP.c_College_name,GEN_STU.c_College_name) AS c_College_name,
				IFNULL(safe_cast(OPP_APP.c_Level_Desc as string),GEN_STU.c_Level_Desc) AS c_Level_Desc,
				Case when Substring(cast(Term_Cd as string), 5, 1) in ('1','3','5','7') then 'Q'
				when Substring(cast(Term_Cd as string), 5, 1) in ('2','4','6') then 'S'
				End as SQ_Flag,
				date_diff(cast(Student_Start_Dt as date), cast(Status_Dt as date), day) as Days_RS_Start_CR_StatusDt, GEN_STU.APPLICATION_NUMBER
			FROM `trans_academics.wldn_reserve_list_gen_student` GEN_STU
			LEFT JOIN `trans_academics.wldn_reserve_list_opp_app` OPP_APP
			ON GEN_STU.Credential_Id = OPP_APP.Credential_Id
			AND GEN_STU.APPLICATION_NUMBER = OPP_APP.APPLICATION_NUMBER
				and GEN_STU.institution_id =5
			and OPP_APP.institution_id=5
			LEFT JOIN `raw_b2c_sfdc.opportunity` SO ON OPP_APP.OppSfid = So.ID and SO.is_deleted = false and SO.institution_c='a0ko0000002BSH4AAO'
			left join `raw_b2c_sfdc.campaign` C on SO.campaign_id = c.id and C.is_deleted = false and C.institution_c='a0ko0000002BSH4AAO'
			WHERE COALESCE(OPP_APP.c_Program_Name,GEN_STU.c_Program_Name,'UNKNOWN') NOT LIKE '%LIU%'
			)  MAIN

LEFT JOIN (
	SELECT BNR_Person_Credential_Id
		,MAX(Course_Start_Dt_Quarter) AS Course_Start_Dt_Quarter

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Identification ELSE NULL END) AS Course_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Identification ELSE NULL END) AS Course_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Identification ELSE NULL END) AS Course_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Identification ELSE NULL END) AS Course_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Identification ELSE NULL END) AS Course_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Identification ELSE NULL END) AS Course_6

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_6

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_6

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_6

  FROM StuCourse
  WHERE RANK_StuCourse < 7
  AND Course_Start_Dt_Quarter = '1'
  GROUP BY BNR_Person_Credential_Id
	 ) STUCOURSE
ON MAIN.Credential_Id = STUCOURSE.BNR_Person_Credential_Id

LEFT JOIN `raw_wldn_pfaids.student` PFS_STU
  ON MAIN.Credential_Id = PFS_STU.alternate_id
LEFT JOIN PFS_TRK_and_PFS_AWARD
  ON PFS_TRK_and_PFS_AWARD.student_token = PFS_STU.student_token
  AND PFS_TRK_and_PFS_AWARD.award_year_token = MAIN.FinAid_Year

 WHERE MAIN.Institution_ID = 5
 AND MAIN.Student_Start_Dt IS NOT NULL
 AND MAIN.Student_Start_Dt_Quarter = '1'

-- AND MAIN.Credential_Id = 'A01042492'
);

CREATE TEMP TABLE Course_Start_Dt_Quarter_Further_2 AS
(
SELECT DISTINCT
         MAIN.PERSON_UID,
				 MAIN.Student_Start_Dt AS Student_Start_Dt
				,MAIN.Credential_Id AS Applicant_Id
				,MAIN.NAME
				,IFNULL(MAIN.SF_EA,'Unknown') AS AdmRepName
				,coalesce (MAIN.SF_UserManager , MAIN.SF_EnrollmentManager,'Unknown') AS Manager
				,SF_Division as Division
				,Director
				,IFNULL(MAIN.SF_Location,'Unknown') AS EA_Location
				-- ,MAIN.c_College_name AS College
				,CASE WHEN MAIN.c_Academic_Program LIKE '%UVM%' THEN 'LIU PARTNERS' ELSE MAIN.c_College_name END AS College
				,MAIN.c_Audience_Name AS Audience_Name
				,MAIN.c_Level_Desc AS Degree_Level
				,MAIN.c_Program_Name AS Program_Name
				,MAIN.c_Banner_Conc_Desc AS Banner_Conc_Desc
				,MAIN.c_Academic_Program AS Academic_Program
				,MAIN.Address_Type
				,MAIN.Address_line_1
				,MAIN.Address_line_2
				,MAIN.City
				,MAIN.State13 AS State
				,MAIN.country AS Country
				,MAIN.Zip_Cd
				,IFNULL(MAIN.EA_Region_Final,'Unknown') AS EA_Region
				,CASE  WHEN MAIN.Student_Population = 'A' THEN 'Readmit'
						WHEN MAIN.Student_Population = 'B' THEN 'Bridge'
						WHEN MAIN.Student_Population = 'C' THEN 'Continuing'
						WHEN MAIN.Student_Population = 'F' THEN 'New First Time Freshman'
						WHEN MAIN.Student_Population = 'H' THEN 'Place Holder'
						WHEN MAIN.Student_Population = 'N' THEN 'Network'
						WHEN MAIN.Student_Population = 'P' THEN 'New Degree Previous Grad'
						WHEN MAIN.Student_Population = 'R' THEN 'Reinstatement'
						WHEN MAIN.Student_Population = 'S' THEN 'New Student'
						WHEN MAIN.Student_Population = 'T' THEN 'Transfer Undergrad'
						WHEN MAIN.Student_Population = 'U' THEN 'Undergrad Change of Program'
						WHEN MAIN.Student_Population = 'W' THEN 'Continuing Network Student'
						WHEN MAIN.Student_Population = 'X' THEN 'Change of Program'
						WHEN MAIN.Student_Population = 'Z' THEN 'Data Migration'
						ELSE ' '
						END AS Student_Type
				,MAIN.Application_Dt AS Application_Dt
				,MAIN.Status_Dt AS Current_Status_Dt
				,MAIN.Deferred27 AS Deferred
				,MAIN.Prior_Start_Dt AS Prior_Start_Dt
				,CASE WHEN PFS_TRK_and_PFS_AWARD.tracking_status = 'IP' THEN 'Incomplete FA Application'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status = 'RP' THEN 'Completed FA Application'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status in ('AW','AR','ID','DS') THEN 'Awarded FA,  missing info'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status in ('RD', 'RR', 'DM') THEN 'Awarded FA, complete'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status in ('SC', 'NA', 'DA', 'HL') THEN 'Not eligible or declined aid'
							WHEN PFS_TRK_and_PFS_AWARD.tracking_status IS NULL THEN 'No FA Application Yet'
							ELSE 'Completed FA Application'
						END AS FA_App_Status
				,PFS_TRK_and_PFS_AWARD.tracking_status AS Tracking_Status
				,MAIN.Term_Cd AS Term_Cd
				,PFS_TRK_and_PFS_AWARD.award_year_token AS Fin_Aid_Year
				-- ,MAIN.FinAid_Year AS Fin_Aid_Year
				,Course_1
				,Course_2
				,Course_3
				,Course_4
				,Course_5
				,Course_6
				,MAIN.Phone_Nbr_Home
				,MAIN.Phone_Nbr_Business
				,MAIN.Email AS Main_Email
				,MAIN.Personal_Email AS Personal_Email
			--	,MAIN.Application_ID
				--,CONCAT(MAIN.Credential_Id,'-',SUBSTRING(MAIN.Application_ID,STRPOS(MAIN.Application_ID, '-')+1, STRPOS(MAIN.Application_ID, '-', STRPOS( MAIN.Application_ID, '-') + 1) - (STRPOS( MAIN.Application_ID, '-')+1)),'-',MAIN.Term_Cd) AS Application_ID
				,concat(MAIN.credential_id,'-',MAIN.APPLICATION_NUMBER,'-',MAIN.term_cd)Application_ID
				,MAIN.OppSfid
				,MAIN.SGASTDN_Program
				,Course_Reference_Nbr_1
				,Course_Reference_Nbr_2
				,Course_Reference_Nbr_3
				,Course_Reference_Nbr_4
				,Course_Reference_Nbr_5
				,Course_Reference_Nbr_6
				,Course_Academic_Period_1
				,Course_Academic_Period_2
				,Course_Academic_Period_3
				,Course_Academic_Period_4
				,Course_Academic_Period_5
				,Course_Academic_Period_6
				,Course_Section_Nbr_1
				,Course_Section_Nbr_2
				,Course_Section_Nbr_3
				,Course_Section_Nbr_4
				,Course_Section_Nbr_5
				,Course_Section_Nbr_6
				,CASE WHEN MAIN.Student_Start_Dt_Quarter = 'previous' THEN 'Previous Quarter'
					  WHEN MAIN.Student_Start_Dt_Quarter = '0' THEN 'Present Quarter'
					  WHEN MAIN.Student_Start_Dt_Quarter = '1' THEN 'Future Quarter'
					  WHEN MAIN.Student_Start_Dt_Quarter = 'further 2' THEN 'Further 2 Future Quarters'
					END AS Selected_Quarter
				,campaign_id
				,Channel
				,SQ_Flag
				,Days_RS_Start_CR_StatusDt


FROM ( SELECT	GEN_STU.PERSON_UID, GEN_STU.Student_Population,	GEN_STU.SGASTDN_Program,	GEN_STU.Credential_Id, 	Application_Id, OppSfid, SO.campaign_id, c.channel_c as channel, SF_Location, Region,SF_EA, SF_UserManager, SF_EnrollmentManager,Director,
				SF_EA_InternationalFlag, SF_Division, SF_Team, EA_Region_Final, GEN_STU.Name,
				IFNULL(OPP_APP.c_Academic_Program,GEN_STU.c_Academic_Program) AS c_Academic_Program,
				IFNULL(OPP_APP.c_Audience_Name,GEN_STU.c_Audience_Name) AS c_Audience_Name,
				IFNULL(OPP_APP.c_Program_Name,GEN_STU.c_Program_Name) AS c_Program_Name,
				IFNULL(OPP_APP.c_Banner_Conc_Desc,GEN_STU.c_Banner_Conc_Desc) AS c_Banner_Conc_Desc, Current_Status,
				Address_Type, Address_line_1, Address_line_2, City, State13, country, country_desc, Zip_Cd,  Student_Start_Dt, Term_Cd,  Application_Dt,   5 AS Institution_ID, 'WLDN' AS Institution,
				Status_Dt  , Student_Start_Dt_Quarter, Application_Dt_Quarter
                ,FinAid_Year,
				 Deferred27, Prior_Start_Dt, Phone_Nbr_Home, Phone_Nbr_Business, Personal_Email, Email,
				IFNULL(OPP_APP.c_College_name,GEN_STU.c_College_name) AS c_College_name,
				IFNULL(safe_cast(OPP_APP.c_Level_Desc as string),GEN_STU.c_Level_Desc) AS c_Level_Desc,
				Case when Substring(cast(Term_Cd as string), 5, 1) in ('1','3','5','7') then 'Q'
				when Substring(cast(Term_Cd as string), 5, 1) in ('2','4','6') then 'S'
				End as SQ_Flag,
				date_diff(cast(Student_Start_Dt as date), cast(Status_Dt as date), day) as Days_RS_Start_CR_StatusDt, GEN_STU.APPLICATION_NUMBER
			FROM `trans_academics.wldn_reserve_list_gen_student` GEN_STU
			LEFT JOIN `trans_academics.wldn_reserve_list_opp_app` OPP_APP
			ON GEN_STU.Credential_Id = OPP_APP.Credential_Id
			AND GEN_STU.APPLICATION_NUMBER = OPP_APP.APPLICATION_NUMBER
				and GEN_STU.institution_id =5
			and OPP_APP.institution_id=5
			LEFT JOIN `raw_b2c_sfdc.opportunity` SO ON OPP_APP.OppSfid = So.ID and SO.is_deleted = false and SO.institution_c='a0ko0000002BSH4AAO'
			left join `raw_b2c_sfdc.campaign` C on SO.campaign_id = c.id and C.is_deleted = false and C.institution_c='a0ko0000002BSH4AAO'

			WHERE COALESCE(OPP_APP.c_Program_Name,GEN_STU.c_Program_Name,'UNKNOWN') NOT LIKE '%LIU%'
			)  MAIN

LEFT JOIN (
	SELECT BNR_Person_Credential_Id
		,MAX(Course_Start_Dt_Quarter) AS Course_Start_Dt_Quarter
		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Identification ELSE NULL END) AS Course_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Identification ELSE NULL END) AS Course_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Identification ELSE NULL END) AS Course_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Identification ELSE NULL END) AS Course_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Identification ELSE NULL END) AS Course_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Identification ELSE NULL END) AS Course_6

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Reference_Nbr ELSE NULL END) AS Course_Reference_Nbr_6

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Academic_Period ELSE NULL END) AS Course_Academic_Period_6

		,MAX(CASE WHEN RANK_StuCourse = 1 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_1
		,MAX(CASE WHEN RANK_StuCourse = 2 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_2
		,MAX(CASE WHEN RANK_StuCourse = 3 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_3
		,MAX(CASE WHEN RANK_StuCourse = 4 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_4
		,MAX(CASE WHEN RANK_StuCourse = 5 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_5
		,MAX(CASE WHEN RANK_StuCourse = 6 THEN Course_Section_Nbr ELSE NULL END) AS Course_Section_Nbr_6

  FROM StuCourse
  WHERE RANK_StuCourse < 7
  AND Course_Start_Dt_Quarter = 'further 2'
  GROUP BY BNR_Person_Credential_Id
	 ) STUCOURSE
ON MAIN.Credential_Id = STUCOURSE.BNR_Person_Credential_Id

LEFT JOIN `raw_wldn_pfaids.student` PFS_STU
  ON MAIN.Credential_Id = PFS_STU.alternate_id
LEFT JOIN PFS_TRK_and_PFS_AWARD
  ON PFS_TRK_and_PFS_AWARD.student_token = PFS_STU.student_token
  AND PFS_TRK_and_PFS_AWARD.award_year_token = MAIN.FinAid_Year

 WHERE MAIN.Institution_ID = 5
 AND MAIN.Student_Start_Dt IS NOT NULL
 AND MAIN.Student_Start_Dt_Quarter = 'further 2'

-- AND MAIN.Credential_Id = 'A01042492'
);

CREATE TEMP TABLE `wldn_reserve_list` as
(
with src as(
SELECT *, current_date() as process_dt FROM Course_Start_Dt_Quarter_Previous
UNION ALL
SELECT *, current_date() as process_dt FROM Course_Start_Dt_Quarter_0
UNION ALL
SELECT *, current_date() as process_dt FROM Course_Start_Dt_Quarter_1
UNION ALL
SELECT *, current_date() as process_dt FROM Course_Start_Dt_Quarter_Further_2
)
select
		src.*,
		5 as Institution_id,
		'WLDN' as institution,
		'WLDN_BNR' as source_system_name,
		job_start_dt as etl_created_date,
        job_start_dt as etl_updated_date,
        load_source as etl_resource_name,
        v_audit_key as etl_ins_audit_key,
        v_audit_key as etl_upd_audit_key,
		farm_fingerprint(format('%T', (src.Applicant_Id, src.Academic_Program, Student_Start_Dt,Application_ID, process_dt))) AS etl_pk_hash,
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
(v_audit_key,'ODS_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;


set result = @@error.message;
RAISE USING message  = @@error.message;

END;

END
