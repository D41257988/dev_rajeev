create or replace view rpt_crm_mart.v_cu_google_api_integration as
select distinct
'CU' as Institution
,LEAD.Id as Lead_Id
,LEAD.Id as Inquiry_Id
,OPP.id as Opp_Id
--,IFNULL(INQ.flex_field_20_c, REPLACE(REGEXP_SUBSTR(INQ.URL_US_C,'&gclid=([^&#]*)'), '&gclid=','')) as gclid
,COALESCE(lead.msclkid_c, lead.gclid_c, REPLACE(REGEXP_SUBSTR(lead.converted_page_url_c,'&gclid=([^&#]*)'), '&gclid=','')) as gclid
,REPLACE(REGEXP_SUBSTR(lead.converted_page_url_c,'&gclsrc=([^&#]*)'), '&gclsrc=','') as gclsrc
,UNIX_SECONDS(lead.Created_Date) * 1000000 AS Inquired
,CASE WHEN APP.PAGE_6_SUBMITTED_C IS NOT NULL THEN UNIX_SECONDS(APP.PAGE_6_SUBMITTED_C)* 1000000
	ELSE UNIX_SECONDS(COALESCE(opp.app_submitted_date_time_c,cast(OPP.app_submitted_date_time_c as timestamp) ))* 1000000
    END AS Applied
/*
,CASE WHEN HIST.CREATEDDATE IS NOT NULL THEN DATE_PART(EPOCH_SECOND,HIST.CREATEDDATE)* 1000000
	--ELSE DATE_PART(EPOCH_SECOND,OPP.CREATEDDATE)* 1000000
    END AS Started
*/
,CASE WHEN OPP.STAGE_NAME in ('Closed - Started') AND OPP.ATTENDED_DATE_C >= cast(OPP.SESSION_START_DATE_C as timestamp) AND OPP.ATTENDED_DATE_C < date_add(cast(OPP.SESSION_START_DATE_C as timestamp), INTERVAL 15 DAY)
		THEN UNIX_SECONDS(OPP.ATTENDED_DATE_C)* 1000000 END STARTED
--,CASE WHEN Started IS NOT NULL THEN '1800' WHEN applied IS NOT NULL THEN '900' ELSE '100' END AS Revenue
/*,CASE WHEN (CASE WHEN HIST.CREATED_DATE IS NOT NULL THEN UNIX_SECONDS(HIST.CREATED_DATE)* 1000000 END) IS NOT NULL THEN '1800'
	WHEN (CASE WHEN APP.PAGE_6_SUBMITTED_C IS NOT NULL THEN UNIX_SECONDS(APP.PAGE_6_SUBMITTED_C)* 1000000
			ELSE UNIX_SECONDS(COALESCE(OPP.Approved_Application_Date_c,cast(OPP.Application_Date_c as timestamp)) )* 1000000
			END) IS NOT NULL THEN '900'
	ELSE '100' END AS Revenue */
,CASE WHEN (CASE WHEN APP.PAGE_6_SUBMITTED_C IS NOT NULL THEN UNIX_SECONDS(APP.PAGE_6_SUBMITTED_C)* 1000000
	ELSE UNIX_SECONDS(COALESCE(opp.app_submitted_date_time_c,cast(OPP.app_submitted_date_time_c as timestamp) ))* 1000000
    END) IS NOT NULL THEN '1800'
	WHEN (CASE WHEN OPP.STAGE_NAME in ('Closed - Started') AND OPP.ATTENDED_DATE_C >= cast(OPP.SESSION_START_DATE_C as timestamp) AND OPP.ATTENDED_DATE_C < date_add(cast(OPP.SESSION_START_DATE_C as timestamp), INTERVAL 15 DAY)
		THEN UNIX_SECONDS(OPP.ATTENDED_DATE_C)* 1000000 END) IS NOT NULL THEN '900'
	ELSE '100' END AS Revenue
--,COALESCE(ltrim(TROAS.Revenue, '$'),'0') as Revenue
,CASE
	WHEN LEAD.Is_Converted = false and LEAD.raw_primary_program_of_interest_c is not null then LEAD.raw_primary_program_of_interest_c
	WHEN LEAD.Is_Converted = true THEN OPP.PROGRAM_CODE_C END AS Program
,LEAD.IQ_QUALITY_GRADE_C AS IQ_Quality_Grade
,COALESCE(LOC.Location_Code_c,bp.home_location_code_c) as Campus
--,COALESCE(LOC.Name,ACCOUNT.student_location_name_c) as Campus_Desc
,COALESCE(LOC.NAME,bp.student_location_name_c,CMP.StvCamp_Desc) as Campus_Desc
FROM raw_b2c_sfdc.lead
LEFT JOIN raw_b2c_sfdc.account ACCOUNT ON LEAD.CONVERTED_ACCOUNT_ID = ACCOUNT.Id
LEFT JOIN raw_b2c_sfdc.opportunity OPP ON OPP.credited_lead_c = lead.id
LEFT JOIN raw_b2c_sfdc.location_c LOC ON opp.LOCATION_C=LOC.Id
LEFT JOIN raw_cu_bnr.stvcamp CMP ON CMP.StvCamp_Code = LOC.Location_Code_c
LEFT JOIN (SELECT * FROM -- Get the latest CreatedDate record from OpportunityHistory
    (SELECT RANK() OVER (PARTITION BY Opportunity_Id ORDER BY Created_Date DESC) AS RANK, Opportunity_Id, Stage_Name, Created_Date
	FROM  raw_cu_sfdc.opportunity_history
	WHERE STAGE_NAME in ('Closed - Started')
	)WHERE RANK=1
	) HIST ON OPP.Id=HIST.Opportunity_Id
LEFT JOIN (SELECT * FROM -- Get the latest CreatedDate record from Application_Pending
    (SELECT RANK() OVER (PARTITION BY Opportunity_c ORDER BY Created_Date DESC) AS RANK, Opportunity_c, PAGE_6_SUBMITTED_C, Created_Date
	FROM  raw_cu_sfdc.application_pending_c
	)WHERE RANK=1
	) APP ON OPP.Id=APP.Opportunity_c
LEFT JOIN raw_b2c_sfdc.brand_profile_c bp on opp.brand_profile_c = bp.id -- raj - added new join to get the home_location_code column

/*
LEFT JOIN DISCOVERY_PROD.ATGE_PM_TROAS.ATGE_GGL_TROAS TROAS -- Get the Revenue for matching Activity,Program, Campus, Inquiry_Score etc.
	ON 'CU'=TROAS.Institution
		AND UPPER(COALESCE(ACCOUNT.student_location_name__c,CMP.StvCamp_Desc,LOC.NAME,'N/A')) = UPPER(TROAS.Campus)
		AND (CASE WHEN INQ.PROGRAM_OF_INTEREST_FROM_INQUIRY_SCHEMA__C IS NOT NULL THEN INQ.PROGRAM_OF_INTEREST_FROM_INQUIRY_SCHEMA__C
			WHEN LEAD.IsConverted = 0 and LEAD.PROGRAM_OF_INTEREST__C is not null then LEAD.PROGRAM_OF_INTEREST__C
			WHEN LEAD.IsConverted = 1 THEN OPP.PROGRAM_CODE__C END) = TROAS.Program
		AND COALESCE(INQ.IQ_QUALITY_GRADE__C,LEAD.IQ_QUALITY_GRADE__C) = TROAS.Inquiry_Score
		AND UPPER(LEAD.Campaign_Marketing_Grouping__c) = UPPER(TROAS.Channel)
		AND (CASE --WHEN OPP.Stagename = 'Closed - Started' THEN 'Start'
					WHEN HIST.OpportunityId IS NOT NULL THEN 'Start'
    				WHEN (CASE WHEN APP.PAGE6SUBMITTED__C IS NOT NULL THEN DATE_PART(EPOCH_SECOND,APP.PAGE6SUBMITTED__C)* 1000000
						ELSE DATE_PART(EPOCH_SECOND,NVL(OPP.Approved_Application_Date__c,OPP.Application_Date__c) )* 1000000
    					END) is not null THEN 'Application'
				ELSE 'Inquiry' END)	= TROAS.Activity
*/

--WHERE IFNULL(INQ.flex_field_20_c, REPLACE(REGEXP_SUBSTR(INQ.URL_US_C,'&gclid=([^&#]*)'), '&gclid=','')) is not NULL
WHERE COALESCE(lead.msclkid_c,lead.gclid_c, REPLACE(REGEXP_SUBSTR(lead.converted_page_url_c,'&gclid=([^&#]*)'), '&gclid=','')) is not NULL
 -- AND REPLACE(REGEXP_SUBSTR(INQ.URL_US_C,'&gclsrc=([^&#]*)'), '&gclsrc=','') in ('aw.ds','ds','3p.ds')
	and lead.is_deleted=false AND (lead.institution_c in ('a0kDP000008l7bvYAA') OR lead.company in ('Chamberlain'))
ORDER BY LEAD.Id, Inquired desc
