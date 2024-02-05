create or replace view rpt_crm_mart.v_cu_facebook_api_integration as
SELECT distinct 'CU' AS Institution
,LEAD.Id as Lead_Id
--,INQ.Id as Inquiry_Id -- raj - inquiry table is going away in sandbox
,CASE
	WHEN Opp.CAMPAIGN_MARKETING_GROUPING_C = 'Phone In/Call Center' THEN 'phone_call' -- raj - removed the code for Lead's column
	WHEN Opp.CAMPAIGN_MARKETING_GROUPING_C = 'Direct Mail/Email' THEN 'email'  -- raj - removed the code for Lead's column
	WHEN Opp.CAMPAIGN_MARKETING_GROUPING_C in ('Internal Website','Internet Display','Other Marketing','Paid Search','Social Media') THEN 'website'  -- raj - removed the code for Lead's column
	WHEN Opp.CAMPAIGN_MARKETING_GROUPING_C in ('Aggregators','Event','Military','Referral','Other Channels') THEN 'other'   -- raj - removed the code for Lead's column
	WHEN Opp.CAMPAIGN_MARKETING_GROUPING_C is NULL
		OR Opp.CAMPAIGN_MARKETING_GROUPING_C = 'unidentified' THEN 'other'
	END as Action_Source
, 'N/A' AS User_Source --Pending
,COALESCE(LEAD.client_user_agent_c,Opp.Client_User_Agent_c) as User_Agent	 -- rs - picking a new col from Opp table
,COALESCE(LEAD.raw_first_name_c,ACCOUNT.FIRST_NAME) AS First_Name
,COALESCE(LEAD.raw_last_name_c,ACCOUNT.LAST_NAME) AS Last_Name
,REGEXP_REPLACE(COALESCE(lead.click_to_dial_country_code_c) || COALESCE(Lead.PHONE,ACCOUNT.Phone), '[^0-9]', '') AS Phone
,COALESCE(lead.raw_country_c, Lead.COUNTRY,ACCOUNT.person_mailing_country) AS Country
,COALESCE(lead.raw_state_c,Lead.State,ACCOUNT.person_mailing_state) AS State
,COALESCE(lead.raw_city_c,Lead.City,ACCOUNT.person_mailing_City) AS City
,COALESCE(lead.raw_postal_Code_c,Lead.Postal_Code,ACCOUNT.person_mailing_Postal_Code) AS Zip
,LOWER(COALESCE(Lead.Email,ACCOUNT.Person_Email)) AS Email -- raj - there is no substitue for "Inq.Email_c" in the sfdc column mapping sheet
,lead.converted_page_url_c AS URL
-- Convert Aplied, Started & Inquired to UNIX timestamp
,UNIX_SECONDS(lead.Created_Date)  AS Inquired
,CASE WHEN App.page_6_submitted_c IS NOT NULL THEN UNIX_SECONDS(App.page_6_submitted_c)
	ELSE UNIX_SECONDS(Opp.app_submitted_date_time_c )
    END AS Applied
,UNIX_SECONDS(Hist.Created_Date) AS Started
-- Default the timestamps to '1900-01-01 00:00:00' when blank
,GREATEST(UNIX_SECONDS(lead.Created_Date)
          , (
						CASE WHEN App.page_6_submitted_c IS NOT NULL THEN UNIX_SECONDS(App.page_6_submitted_c)
							ELSE UNIX_SECONDS(COALESCE(Opp.app_submitted_date_time_c,parse_timestamp('%Y-%m-%d %H:%M:%S', '1900-01-01 00:00:00')))  END)
          , UNIX_SECONDS(COALESCE(Hist.Created_Date ,parse_timestamp('%Y-%m-%d %H:%M:%S', '1900-01-01 00:00:00'))) ) AS SystemModStamp

, CASE WHEN lead.raw_primary_program_of_interest_c IS NOT NULL THEN lead.raw_primary_program_of_interest_c
	WHEN Lead.Is_Converted = false and opp.program_of_interest_c is not null then opp.program_of_interest_c
	WHEN Lead.Is_Converted = true THEN Opp.Program_Code_c
  END AS Program
-- ,COALESCE(LEAD.CAMPAIGN_MARKETING_GROUPING_C,Opp.CAMPAIGN_MARKETING_GROUPING_C) AS Marketing_Grouping
,Opp.CAMPAIGN_MARKETING_GROUPING_C AS Marketing_Grouping
,COALESCE(lead.iq_quality_grade_c,Opp.credited_lead_c) AS IQ_Quality_Grade -- raj - remvoed the following "Inq.IQ_QUALITY_GRADE_C"

FROM raw_b2c_sfdc.lead lead
LEFT JOIN raw_cu_sfdc.account ACCOUNT ON LEAD.CONVERTED_ACCOUNT_ID = ACCOUNT.Id
LEFT JOIN raw_b2c_sfdc.opportunity Opp ON Opp.credited_lead_c = lead.id
LEFT JOIN raw_cu_sfdc.opportunity_history Hist ON Hist.OPPORTUNITY_ID = Opp.Id AND Opp.STAGE_NAME = 'Closed - Started'
LEFT JOIN raw_cu_sfdc.application_pending_c App ON Opp.Id = App.Opportunity_c
-- WHERE  applied > started AND Opp.NAME LIKE 'Kirsten%Sutton%'
where  lead.is_deleted=false AND (lead.institution_c in ('a0kDP000008l7bvYAA') OR lead.company in ('Chamberlain'))
ORDER BY LEAD.Id, Inquired desc
