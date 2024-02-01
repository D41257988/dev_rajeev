CREATE or REPLACE VIEW `rpt_crm_mart.v_wldn_case_aa_outreach`
OPTIONS(
  description="v_case_aa_outreach",
  labels=[("source", "salesforce"), ("institution", "wldn"), ("type", "case"), ("sub-type", "")]
)
AS
with temp_cte as (
	    SELECT
		CR.id as CaseSFId
	    ,CR.brand_profile_c as BrandProfileSFId
		,CR.owner_id as OwnerSFId
	    ,CR.Product_c as product
		,CR.Status
		,CR.Created_date  as CreatedDt
		,CR.last_modified_by_id as LastModifiedBySFId
		,CR.Last_Modified_date as LastModifiedDt
		,CR.Case_Number as CaseNumber
		,CR.Subject
		,CR.type as RecordType
		,STRPOS(Subject,'|') as FirstDelimiter,
		case when (STRPOS(substr(CR.Subject, STRPOS(CR.Subject,'|')+1 ),'|')= 0 ) then 0 else STRPOS(CR.Subject,'|')+STRPOS(substr(CR.Subject, STRPOS(CR.Subject,'|')+1 ),'|') end as SecondDelimiter
    from
				(	SELECT c.*,u.name as collector,u.username,m.name as manager_name,u.alias,EXTRACT(YEAR from c.last_modified_date) as year,				EXTRACT(MONTH from c.last_modified_date) as month
 							FROM `raw_b2c_sfdc.case` c
								left join `raw_b2c_sfdc.user` u on c.owner_id=u.id
								left join `raw_b2c_sfdc.user` m on upper(u.manager_id) = upper(m.id)
								left join `raw_b2c_sfdc.opportunity` o on c.opportunity_c = o.id -- RS - new condition added to filter in Walden data
								where
									c.is_deleted = false
									and c.record_type_id in ('012o00000012ZrkAAE')
									and o.institution_c='a0ko0000002BSH4AAO'
									) CR -- RS - Deriving the case retention directly from the table
		where CR.institution_brand_c = 'a0ko0000002BSH4AAO'
    ),


	FirstDelimiter_SecondDelimiter__0 as(
			select
			*,
			 SPLIT(subject, '|')[safe_ordinal(1)] AS `CampaignName`,
       case when SPLIT(subject, '|')[safe_ordinal(2)]  = '//' then null else SPLIT(subject, '|')[safe_ordinal(2)] end as TermStart,
  SPLIT(subject, '|')[safe_ordinal(3)] AS TermPOT
	from temp_cte where FirstDelimiter > 0 AND SecondDelimiter > 0
	),
	--	select * from FirstDelimiter_SecondDelimiter__0
 base_case_temp as(

	select *except(CampaignName,TermStart,TermPOT),

     case when length(CampaignName) > 0 AND TermStart IS NOT NULL AND length(TermPOT) = 8 AND SAFE_CAST(TermPOT AS FLOAT64) is not null then CampaignName else null end as CampaignName,
	 case when length(CampaignName) > 0 AND TermStart IS NOT NULL AND length(TermPOT) = 8 AND SAFE_CAST(TermPOT AS FLOAT64) is not null then TermStart else null end as TermStart,
	 case when length(CampaignName) > 0 AND TermStart IS NOT NULL AND length(TermPOT) = 8 AND SAFE_CAST(TermPOT AS FLOAT64) is not null then LEFT(TermPOT, 6) else null end as TermID,
	 case when length(CampaignName) > 0 AND TermStart IS NOT NULL AND length(TermPOT) = 8 AND SAFE_CAST(TermPOT AS FLOAT64) is not null then cast( SUBSTRING(TermPOT, 7, 2) as INT64) else null end as POT
	 from FirstDelimiter_SecondDelimiter__0

),
 -- select * from base_case_temp
base_case as(
  select * from base_case_temp where
		TermID IS NOT NULL
		AND POT IS NOT NULL
		AND CampaignName IN (
			'Onboarding'
			,'Non Registration'
			,'Failure to Log In'
			,'Failure to Post'
			,'First Term Outreach'
			,'Leave of Absence'
			,'Retention Risk Outreach'
			,'Ongoing Attendance'
		)
),

rnt_cte as(
		SELECT
			CaseSFId
			,BrandProfileSFId
			,OwnerSFId
			,Product
			,Status
			,CreatedDt
			,LastModifiedBySFId
			,LastModifiedDt
			,CaseNumber
			,Subject
			,RecordType
			,CampaignName
			,TermStart
			,TermID
			,POT
			,ROW_NUMBER() OVER (PARTITION BY CampaignName, TermID, POT, BrandProfileSFId ORDER BY LastModifiedDt DESC, CaseNumber) AS Rnt
		FROM BASE_CASE
),
-- Remove duplicate cases for students in a campaign
UNIQUE_CASE AS
(
	SELECT
		CaseSFId
		,BrandProfileSFId
		,OwnerSFId
		,Product
		,Status
		,CreatedDt
		,LastModifiedBySFId
		,LastModifiedDt
		,CaseNumber
		,Subject
		,RecordType
		,CampaignName
		,TermStart
		,TermID
		,POT
	FROM
	rnt_cte
	WHERE Rnt = 1
) ,
--select * from UNIQUE_CASE
BASE_STUDENT AS
(
	SELECT
		BP.id as BrandProfileSFId
		,c.id as ContactSFId
		,C.Email
		,BP.banner_id_c
		,BP.First_Name_C
		,BP.Last_name_c
		,BP.Preferred_Email_c
		,BP.academic_advisor_name_c as walden_academic_advisor_c -- rs - getting the column from new table
	FROM `raw_b2c_sfdc.contact` C
	INNER JOIN `raw_b2c_sfdc.brand_profile_c` BP
	on c.id = BP.contact_c
	WHERE
		BP.Institution_c = 'a0ko0000002BSH4AAO'
),


BASE_ADVISOR AS
(
	-- Advisors
	SELECT
		id as ServiceUsersfid
		,UserName
		,First_Name
		,Last_Name
		,NULL AS Assignment_Group -- This column is removed
		,NULL AS Domestic
		,Manager_name
	FROM `rpt_crm_mart.v_wldn_service_user` 

	UNION all
	SELECT
		id AS ServiceUsersfid
		,NULL AS UserName
		,concat(Type ,':' )AS FirstName
		,Name AS LastName
		,NULL AS AssignmentGroup
		,NULL AS Domestic
		,NULL AS Manager
	FROM `raw_b2c_sfdc.group` where Type like '%Queue%' 
),

BASE_PRODUCT AS
(
	SELECT
		id as ProductSfId,
		name as ProductNbr
	FROM `raw_b2c_sfdc.product_2`
	WHERE
		Institution_c = 'a0ko0000002BSH4AAO'
)

SELECT distinct
	BASE_STUDENT.banner_id_c
	,ifnull(UNIQUE_CASE.Status, 'Unknown') AS Status
	,ifnull(BASE_STUDENT.walden_academic_advisor_c, 'Unknown') AS AssignedToAdvisor
	, ifnull(concat(concat(CASE_OWNER.First_Name,' '), CASE_OWNER.Last_Name),'Unknown') as CaseOwner
	,ifnull(CASE_OWNER.Manager_name, 'Unknown') AS AcademicManager
	,CASE_OWNER.Domestic AS Domestic
	,concat(concat(BASE_STUDENT.First_Name_C,' '), BASE_STUDENT.Last_Name_C) as StudentName
	,BASE_STUDENT.Preferred_Email_c AS WaldenEmail
	,BASE_STUDENT.Email
	,UNIQUE_CASE.CaseNumber
	,BASE_PRODUCT.ProductNbr
	,UNIQUE_CASE.CampaignName
	,CASE_OWNER.Assignment_Group AS AssignmentGroup
	,UNIQUE_CASE.TermID
	,UNIQUE_CASE.POT
	,UNIQUE_CASE.TermStart
	,UNIQUE_CASE.CaseSFId
	,BASE_STUDENT.ContactSFId
	,UNIQUE_CASE.Subject
	,UNIQUE_CASE.CreatedDt
	,ifnull(concat(concat(MODIFIED_BY.First_Name,' '),MODIFIED_BY.last_name),'Unknown') as LastModifiedBy
	-- ,UNIQUE_CASE.BrandProfileSFId
FROM UNIQUE_CASE
left JOIN BASE_STUDENT
ON BASE_STUDENT.BrandProfileSFId = UNIQUE_CASE.BrandProfileSFId
LEFT JOIN BASE_PRODUCT
ON BASE_PRODUCT.ProductSfId = UNIQUE_CASE.Product
LEFT JOIN BASE_ADVISOR CASE_OWNER
ON CASE_OWNER.ServiceUsersfid = UNIQUE_CASE.OwnerSFId
LEFT JOIN BASE_ADVISOR MODIFIED_BY
ON MODIFIED_BY.ServiceUsersfid = UNIQUE_CASE.LastModifiedBySFId
