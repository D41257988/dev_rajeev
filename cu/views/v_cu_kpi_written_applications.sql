select distinct
	 opp.id                                                                                        as opportunity_id
    ,opp.owner_id                                                                                as ownerid
    ,original_intended_start_date_c                                                              as original_intended_start_date
    ,opp.app_submitted_date_time_c                   											                       as approved_application_date
    ,opp.session_code_c                                                                          as term_code
    ,opp.location_code                                                                           as  location_code
    ,ifnull(opp.program_code_c, 'Unknown')                                                       as  program_code
    ,opp.program_group_code                                                                      as  program_group_code
	  ,opp.concentration_c                                                                         as  concentration
	--,opp.concentration2																			                                     as  concentration replaced with concentration_c need clarity
    ,opp.payment_status_2_c                                                                      as  payment_status
    ,opp.admission_committee_decision_c                                                          as  admission_committee_decision
    ,opp.admission_committee_review_date_c                                                       as  admission_committee_review_date
    ,opp.attendance_preference_c                                                                 as  attendance_preference
    ,opp.financially_cleared_date_c                                                              as  financially_cleared_date
    ,opp.waiver_c                                                                                as  waiver_id
    ,wvr.name                                                                                    as  waiver_description
    ,inq.id                                                                                      as  inquiry_id
    ,inq.campaign_c                                                                              as  campaign_id
    ,inq.quality_grade                                                                           as  inquiry_scoring_tier
    ,inq.response_score                                                                          as  response_score
    ,inq.how_did_you_hear_about_us_c                                                             as  hdyhau
    ,inq.type                                                                                    as  prospect_type
    ,inq.country                                                                                 as  address_country_inquiry
    ,inq.state                                                                                   as  address_state_inquiry
    ,inq.postal_code                                                                             as  address_postal_code_inquiry
from (
    select opp.id, opp.owner_id, original_intended_start_date_c,
app_submitted_date_time_c,--approved_application_date,
 session_code_c,
 LOCATION.Location_Cd AS Location_Code,
 program_code_c,
 CASE
	WHEN OPP.PROGRAM_CODE_C LIKE 'BSN%' AND LOCATION.Location_Cd IN ('ON1', 'ON2') THEN 'BSN_ONLINE'
	WHEN OPP.PROGRAM_CODE_C LIKE 'ADN%' OR OPP.PROGRAM_CODE_C LIKE 'ASN%' OR
		 OPP.PROGRAM_CODE_C LIKE 'LPN%' OR OPP.PROGRAM_CODE_C LIKE 'BSN%' OR
		 OPP.PROGRAM_CODE_C LIKE 'MIL_BSN%'THEN 'BSN'
	WHEN OPP.PROGRAM_CODE_C like 'NM_%' then 'Non-Matric'
	WHEN (OPP.PROGRAM_CODE_C LIKE '%DNP%' AND OPP.PROGRAM_CODE_C NOT LIKE 'PR_DNP') OR (OPP.PROGRAM_CODE_C LIKE 'NM_DR_NP%' AND OPP.CONCENTRATION_C LIKE
		 '%Nursing Practice%') THEN 'DNP'
	WHEN  (OPP.CONCENTRATION_C LIKE 'Adult Gerontology%' ) OR (OPP.PROGRAM_CODE_C LIKE '%AG%') THEN 'AG'
	WHEN OPP.PROGRAM_CODE_C LIKE '%FNP%' OR (OPP.PROGRAM_CODE_C LIKE '%MSN%' AND
	OPP.CONCENTRATION_C LIKE
		 '%Family Nurse Practitioner%')  THEN 'FNP'
	WHEN OPP.CONCENTRATION_C LIKE '%Psychiatric-Mental%' OR OPP.PROGRAM_CODE_C LIKE '%PMH%' THEN 'PMH'
	WHEN (OPP.PROGRAM_CODE_C LIKE 'RN%' AND OPP.PROGRAM_CODE_C NOT IN ('RN_MSN_US', 'RNMSNA_US'))
		 OR (OPP.PROGRAM_CODE_C = 'NM_NR' AND OPP.CONCENTRATION_C LIKE 'Nursing%') OR (OPP.PROGRAM_CODE_C LIKE 'RN_BSN%')
		 OR (OPP.PROGRAM_CODE_C LIKE '%RN to BSN%') THEN 'RNBSN'
	WHEN (OPP.PROGRAM_CODE_C = 'NM_NN') OR (OPP.PROGRAM_CODE_C LIKE 'NM_JST%') THEN 'Non-Matric'
	WHEN OPP.PROGRAM_CODE_C IS NULL THEN 'UNKNOWN'
	WHEN OPP.PROGRAM_CODE_C LIKE '%MPH%' OR OPP.PROGRAM_CODE_C LIKE 'GC_EP_US%' OR
		 OPP.PROGRAM_CODE_C LIKE '%PHG%' OR OPP.PROGRAM_CODE_C LIKE 'GC_GH_US%' OR
		 OPP.PROGRAM_CODE_C LIKE 'GC_GHF_US%' OR OPP.PROGRAM_CODE_C LIKE 'Master%Public Health%'
		 OR OPP.PROGRAM_CODE_C LIKE '%NM_GR_HP%' THEN 'MPH'
	WHEN OPP.PROGRAM_CODE_C LIKE 'MSW%' OR OPP.PROGRAM_CODE_C LIKE '%Social Work Advance Standing Track%' THEN 'MSW'
	WHEN OPP.PROGRAM_CODE_C LIKE 'MHAAS%' THEN 'MHA'
	WHEN OPP.PROGRAM_CODE_C LIKE 'MSNA%' OR OPP.PROGRAM_CODE_C LIKE '%Accelerated MSN Option%' THEN 'MSNA'
	WHEN OPP.PROGRAM_CODE_C LIKE 'RNMSNA%' THEN 'RNMSNA'
	WHEN OPP.PROGRAM_CODE_C LIKE ('MPA%') THEN 'MPAS'
	WHEN OPP.PROGRAM_CODE_C NOT LIKE 'MKTG%' AND OPP.PROGRAM_CODE_C NOT IN ('AZ', 'RUSSI', 'West KY KCTCS', 'AHIT_US', 'BTHM_US', 'UC_MBC')
		 THEN 'MSN (NC)'
	ELSE 'OTHERS'
END AS PROGRAM_GROUP_CODE,opp.concentration_c as concentration_c , --concentration_c,----
			 payment_status_2_c, admission_committee_decision_c, admission_committee_review_date_c, attendance_preference_c, financially_cleared_date_c, waiver_c, credited_lead_c
			 from raw_b2c_sfdc.opportunity opp
       left join raw_b2c_sfdc.session_c sess on  sess.id=opp.session_c
			 LEFT JOIN (SELECT DISTINCT LOC1.ID ID, COALESCE(LOC1.LOCATION_CODE_C, LOC2.LOCATION_CODE_C) AS LOCATION_CD
                    FROM raw_b2c_sfdc.location_c LOC1
            LEFT JOIN raw_b2c_sfdc.location_c LOC2 ON LOC1.APPLY_LOCATION_C=LOC2.ID) LOCATION
On OPP.Location_c=LOCATION.Id
 where
			 upper(stage_name) not like 'PENDING%' and opp.is_deleted=false AND opp.institution_c in ('a0kDP000008l7bvYAA')
			 and app_submitted_date_time_c is not null and
			 record_type_id = '0121I000000UeYLQA0' -- approved application in the Submitted RecordType
     ) opp
left join (select id, campaign_c,
--CASE WHEN CAST(CREATED_DATE AS DATE) < '2021-04-01' THEN Tier_Score_Level_c  (Not used any more)
  --  ELSE
    IQ_Quality_Grade_c AS QUALITY_GRADE,
   -- CAST(CASE WHEN CAST(CREATED_DATE AS DATE) < '2021-04-01' THEN COALESCE(split(RI.flex_Field_7_c, '|')[SAFE_OFFSET(0)],'0') (in notes mentioned as field is removed)
	 -- ELSE
     COALESCE(IQ_RESPONSE_SCORE_C,'O') AS  RESPONSE_SCORE , how_did_you_hear_about_us_c, prospect_type_c as type,raw_country_c as country, raw_state_c as state,RAW_Postal_Code_c as  postal_code from raw_b2c_sfdc.lead  WHERE is_deleted=false AND institution_c in ('a0kDP000008l7bvYAA')) inq on opp.credited_lead_c = inq.id
left join raw_b2c_sfdc.waiver_c wvr on opp.waiver_c=wvr.id