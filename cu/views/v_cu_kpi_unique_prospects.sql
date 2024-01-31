select distinct prospect_id, contact_id, 
         inquiry_id, case when assign_opp=0 then null else opportunity_id end as opportunity_id, 
         -- account_id, 
         inquiry_created_date, campaign_id,
		     -- ri_contact_date,  -- rs - no need of contact date from lead. will be taken from Oppo
        case when assign_opp=0 then null else opp_contact_date end as opp_contact_date, 
        -- l_contact_date,  -- rs - no need of contact date from lead. will be taken from Oppo
    prospect_owner_id, prospect_status, inquiry_scoring_tier,
		response_score, hdyhau, attendance_preference, prospect_type, address_country_inquiry, address_state_inquiry, address_postal_code_inquiry, location_code, modality_type,
		program_group_code, opp_program_group_code, program_code, next_inquiry_date, prospect_created_date, opp_create_date, session_start_date, original_intended_start_date,
		approved_application_date, ri_attempted_contact_date, l_attempted_contact_date, opp_attempted_contact_date, assign_opp
		from
(select distinct prospect_id, contact_id, inquiry_id, opportunity_id, 
        -- account_id, 
    inquiry_created_date, 
    campaign_id, 
    -- ri_contact_date, -- rs - no need of contact date from lead. will be taken from Oppo
    opp_contact_date, 
    -- l_contact_date, -- rs - no need of contact date from lead. will be taken from Oppo
		prospect_owner_id, prospect_status, inquiry_scoring_tier, response_score, hdyhau, attendance_preference, prospect_type, address_country_inquiry, address_state_inquiry,
		address_postal_code_inquiry, location_code, modality_type, program_group_code, opp_program_group_code, next_inquiry_date, prospect_created_date, program_code, opp_create_date, ri_attempted_contact_date, l_attempted_contact_date, opp_attempted_contact_date,
		case when (min(ifnull(opp_create_date, timestamp(parse_date('%d/%m/%Y','01/01/9999')) )) over (partition by prospect_id) > inquiry_created_date) and
		(min(ifnull(opp_create_date, timestamp(parse_date('%d/%m/%Y','01/01/9999')) )) over (partition by prospect_id) = opp_create_date) then 1 else 0 end as assign_opp,
        max(session_start_date) over (partition by prospect_id, inquiry_id, inquiry_created_date, location_code, program_group_code) session_start_date,
	    max(original_intended_start_date) over (partition by prospect_id, inquiry_id, inquiry_created_date, location_code, program_group_code) original_intended_start_date,
	    max(approved_application_date) over (partition by prospect_id, inquiry_id, inquiry_created_date, location_code, program_group_code) approved_application_date
		from
-- Get the APPROVED_APPLICATION_DATE for each inquiry, NULL is not found
(select distinct prospect_id, contact_id, inquiry_id, opportunity_id, 
        -- account_id, 
    inquiry_created_date, campaign_id, 
    -- ri_contact_date, -- rs - no need of contact date from lead. will be taken from Oppo
    opp_contact_date, 
    -- contact_date as l_contact_date,  -- rs - no need of contact date from lead. will be taken from Oppo
	    prospect_owner_id, prospect_status, inquiry_scoring_tier, response_score, hdyhau, attendance_preference, prospect_type, address_country_inquiry, address_state_inquiry,
		address_postal_code_inquiry, location_code, modality_type, opp_program_group_code, program_group_code, program_code,
		ifnull(next_inquiry_date, timestamp(parse_date('%d/%m/%Y','01/01/9999')) ) next_inquiry_date, prospect_created_date,
        case when oppt_create_date>inquiry_created_date then oppt_create_date else null end as opp_create_date,
		case when oppt_create_date between inquiry_created_date and ifnull(next_inquiry_date, timestamp(parse_date('%d/%m/%Y','01/01/9999')) ) then approved_application_date
			 else null end as approved_application_date,
		session_start_date, original_intended_start_date, ri_attempted_contact_date, l_attempted_contact_date, opp_attempted_contact_date
		from
(select distinct inquiry_id inquiry_id, lead_id prospect_id,converted_opportunity_id, createddate as inquiry_created_date, campaign_id as campaign_id, 
        --contact_date as ri_contact_date, -- rs - no need of contact date from lead. will be taken from Oppo
        prospect_created_date, lead(createddate) over (partition by lead_id order by createddate) next_inquiry_date,
				prospect_owner_id, prospect_status, inquiry_scoring_tier, response_score, hdyhau, attendance_preference, prospect_type, address_country_inquiry, address_state_inquiry,
		address_postal_code_inquiry, ri.original_assigned_location_code as location_code, contact_id contact_id, modality_type as modality_type,
		program_group_code as program_group_code, program_code
		from
		-- Unique Inquiries/prospect/per day
		(
		select
                lead.id as inquiry_id
                ,lead.id as lead_id
                ,lead.converted_opportunity_id as converted_opportunity_id
                ,lead.created_date as createddate
                ,lead.campaign_c as campaign_id
                ,lead.iq_quality_grade_c as quality_grade
                ,loc.location_code_c as original_assigned_location_code
                ,bp.contact_c as contact_id -- rs - based on data_migration_workbook_pt2
                ,CASE
                  WHEN lead.raw_primary_program_of_interest_c LIKE 'BSN%' AND loc.location_code_c IN ('ON1', 'ON2') THEN 'BSN_ONLINE'
                  WHEN lead.raw_primary_program_of_interest_c LIKE 'ADN%' OR lead.raw_primary_program_of_interest_c LIKE 'ASN%' OR
                    lead.raw_primary_program_of_interest_c LIKE 'LPN%' OR lead.raw_primary_program_of_interest_c LIKE 'BSN%' OR
                    lead.raw_primary_program_of_interest_c LIKE 'MIL_BSN%'THEN 'BSN'
                  WHEN lead.raw_primary_program_of_interest_c like 'NM_%' then 'Non-Matric'
                  WHEN (lead.raw_primary_program_of_interest_c LIKE '%DNP%' AND lead.raw_primary_program_of_interest_c NOT LIKE 'PR_DNP') OR (lead.raw_primary_program_of_interest_c LIKE 'NM_DR_NP%' AND lead.concentration_c LIKE
                    '%Nursing Practice%') THEN 'DNP'
                  WHEN  (lead.concentration_c LIKE 'Adult Gerontology%' ) OR (lead.raw_primary_program_of_interest_c LIKE '%AG%') THEN 'AG'
                  WHEN lead.raw_primary_program_of_interest_c LIKE '%FNP%' OR (lead.raw_primary_program_of_interest_c LIKE '%MSN%' AND
                  lead.concentration_c LIKE
                    '%Family Nurse Practitioner%')  THEN 'FNP'
                  WHEN lead.concentration_c LIKE '%Psychiatric-Mental%' OR lead.raw_primary_program_of_interest_c LIKE '%PMH%' THEN 'PMH'
                  WHEN (lead.raw_primary_program_of_interest_c LIKE 'RN%' AND lead.raw_primary_program_of_interest_c NOT IN ('RN_MSN_US', 'RNMSNA_US'))
                    OR (lead.raw_primary_program_of_interest_c = 'NM_NR' AND lead.concentration_c LIKE 'Nursing%') OR (lead.raw_primary_program_of_interest_c LIKE 'RN_BSN%')
                    OR (lead.raw_primary_program_of_interest_c LIKE '%RN to BSN%') THEN 'RNBSN'
                  WHEN lead.raw_primary_program_of_interest_c IS NULL THEN 'UNKNOWN'
                  WHEN lead.raw_primary_program_of_interest_c LIKE '%MPH%' OR lead.raw_primary_program_of_interest_c LIKE 'GC_EP_US%' OR
                    lead.raw_primary_program_of_interest_c LIKE '%PHG%' OR lead.raw_primary_program_of_interest_c LIKE 'GC_GH_US%' OR
                    lead.raw_primary_program_of_interest_c LIKE 'GC_GHF_US%' OR lead.raw_primary_program_of_interest_c LIKE 'Master%Public Health%'
                    OR lead.raw_primary_program_of_interest_c LIKE '%NM_GR_HP%' THEN 'MPH'
                  WHEN lead.raw_primary_program_of_interest_c LIKE 'MSW%' OR lead.raw_primary_program_of_interest_c LIKE '%Social Work Advance Standing Track%' THEN 'MSW'
                  WHEN lead.raw_primary_program_of_interest_c LIKE 'MHAAS%' THEN 'MHA'
                  WHEN lead.raw_primary_program_of_interest_c LIKE 'MSNA%' OR lead.raw_primary_program_of_interest_c LIKE '%Accelerated MSN Option%' THEN 'MSNA'
                  WHEN lead.raw_primary_program_of_interest_c LIKE 'RNMSNA%' THEN 'RNMSNA'
                  WHEN lead.raw_primary_program_of_interest_c LIKE ('MPA%') THEN 'MPAS'
                  WHEN lead.raw_primary_program_of_interest_c NOT LIKE 'MKTG%' AND lead.raw_primary_program_of_interest_c NOT IN ('AZ', 'RUSSI', 'West KY KCTCS')
                    THEN 'MSN (NC)'
                  ELSE 'OTHERS'
                END AS PROGRAM_GROUP_CODE
                --,lead.contact_date_c as contact_date -- rs - there will be only one contact date and that will come from opportunity table
                ,lead.iq_quality_grade_c as inquiry_scoring_tier
                ,COALESCE(lead.iq_response_score_c,'O') as response_score
                ,lead.how_did_you_hear_about_us_c as hdyhau
                ,lead.attendance_preference_c as attendance_preference
                ,lead.prospect_type_c as prospect_type
                ,lead.country as address_country_inquiry
                ,lead.state as address_state_inquiry
                ,lead.postal_code as address_postal_code_inquiry
                ,lead.raw_primary_program_of_interest_c as program_code
                ,CASE
                    WHEN loc.location_code_c IS NULL AND (lead.raw_primary_program_of_interest_c like 'BSN%' OR
                          lead.raw_primary_program_of_interest_c is NULL OR upper(lead.raw_valid_rn_license_c) = 'FALSE') THEN upper('Onsite')
                    WHEN lead.raw_primary_program_of_interest_c LIKE 'BSN%' AND
                      UPPER(lead.channel_c) = 'ONLINE' THEN upper('Online')
                    WHEN lead.raw_primary_program_of_interest_c LIKE 'ADN%' OR lead.raw_primary_program_of_interest_c LIKE 'ASN%'    OR
                        lead.raw_primary_program_of_interest_c LIKE 'LPN%' OR lead.raw_primary_program_of_interest_c LIKE 'BSN%' OR
                        lead.raw_primary_program_of_interest_c LIKE 'MIL_BSN%' OR lead.raw_primary_program_of_interest_c IS NULL
                    THEN upper('Onsite') ELSE upper('Online') END AS MODALITY_TYPE
                ,lead.Created_Date as prospect_created_date -- rs - To get Contact Dates from Lead
                ,lead.owner_id as prospect_owner_id  -- rs - To get Contact Dates from Lead
                ,lead.status as prospect_status  -- rs - To get Contact Dates from Lead
                --,lead.last_contacted_date_c as last_contact_date  -- rs - To get Contact Dates from Lead


            from raw_b2c_sfdc.lead lead
              left join raw_b2c_sfdc.location_c loc on lead.location_c = loc.id
              left join raw_b2c_sfdc.brand_profile_c bp on lead.brand_profile_c = bp.id
              where
                lead.is_deleted=false AND (lead.institution_c in ('a0kDP000008l7bvYAA') OR lead.company in ('Chamberlain'))
                and extract(date from lead.created_date) > date_add(current_date, interval -10 year)
                and (
                        ((upper(initial_assignment_c) not like '%TEST%' or initial_assignment_c is null)
						  and (upper(first_name) not like '%TEST%' or first_name is null)
						  and (upper(last_name) not like '%TEST%' or last_name is null)
						  ))
		        and ((upper(coalesce(email, raw_email_address_c)) not like '%TEST%'
						  and upper(coalesce(email, raw_email_address_c)) not like '%@ADTALEM.COM%'
						  and upper(coalesce(email, raw_email_address_c)) not like '%LFO.COM%')
						  or upper(coalesce(email, raw_email_address_c)) is null
              )
		) ri

)inq
 -- Get all Accounts with Opportunities and keep contacted tasks only before an application is approved
left join

(
  select
          -- '' as account_id, -- rs - Removing the account_id column as everything as it is not needed in consolidated SFDC
          opp.id as opportunity_id
          ,opp.created_date as oppt_create_date
          ,coalesce(opp.first_ea_two_way_contact_c, first_phone_two_way_contact_c) as opp_contact_date -- rs - contact date taken from oppo table
          ,opp.first_contact_attempt_c as opp_attempted_contact_date -- rs - replacing with another field as the CU dictionary Pt2 has not mention the corresponding field
          ,opp.program_code_c as opp_program_code
          ,opp.app_submitted_date_time_c as approved_application_date
          ,opp.session_start_date_c  as session_start_date
          ,opp.original_intended_start_date_c as original_intended_start_date
          ,CASE
                WHEN OPP.PROGRAM_CODE_C LIKE 'BSN%' AND loc.location_code_c IN ('ON1', 'ON2') THEN 'BSN_ONLINE'
                WHEN OPP.PROGRAM_CODE_C LIKE 'ADN%' OR OPP.PROGRAM_CODE_C LIKE 'ASN%' OR
                    OPP.PROGRAM_CODE_C LIKE 'LPN%' OR OPP.PROGRAM_CODE_C LIKE 'BSN%' OR
                    OPP.PROGRAM_CODE_C LIKE 'MIL_BSN%'THEN 'BSN'
                WHEN OPP.PROGRAM_CODE_C like 'NM_%' then 'Non-Matric'
                WHEN (OPP.PROGRAM_CODE_C LIKE '%DNP%' AND OPP.PROGRAM_CODE_C NOT LIKE 'PR_DNP') OR (OPP.PROGRAM_CODE_C LIKE 'NM_DR_NP%' AND COALESCE(OPP.concentration_c,prd.concentration_code_c) LIKE
                    '%Nursing Practice%') THEN 'DNP'
                WHEN  (COALESCE(OPP.concentration_c,prd.concentration_code_c) LIKE 'Adult Gerontology%' ) OR (OPP.PROGRAM_CODE_C LIKE '%AG%') THEN 'AG'
                WHEN OPP.PROGRAM_CODE_C LIKE '%FNP%' OR (OPP.PROGRAM_CODE_C LIKE '%MSN%' AND
                COALESCE(OPP.concentration_c,prd.concentration_code_c) LIKE
                    '%Family Nurse Practitioner%')  THEN 'FNP'
                WHEN OPP.concentration_c LIKE '%Psychiatric-Mental%' OR OPP.PROGRAM_CODE_C LIKE '%PMH%' THEN 'PMH'
                WHEN (OPP.PROGRAM_CODE_C LIKE 'RN%' AND OPP.PROGRAM_CODE_C NOT IN ('RN_MSN_US', 'RNMSNA_US'))
                    OR (OPP.PROGRAM_CODE_C = 'NM_NR' AND COALESCE(OPP.concentration_c,prd.concentration_code_c) LIKE 'Nursing%') OR (OPP.PROGRAM_CODE_C LIKE 'RN_BSN%')
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
          END  as opp_program_group_code
          ,opp.first_ea_outreach_attempt_c as ri_attempted_contact_date -- rs - getting the date from Oppo
          ,opp.first_ea_outreach_attempt_c as l_attempted_contact_date -- rs - getting the date from Oppo
					

       from raw_b2c_sfdc.opportunity opp
       left join (select distinct loc1.id id, coalesce(loc1.location_code_c, loc2.location_code_c) as location_code_c
                    from raw_b2c_sfdc.location_c loc1
                         left join raw_b2c_sfdc.location_c loc2 on loc1.apply_location_c=loc2.id) loc on opp.location_c = loc.id
       left join raw_b2c_sfdc.product_2 prd on opp.program_of_interest_c = prd.id
       where opp.is_deleted=False
            and (banner_id_c like 'D%' OR opp.institution_c in ('a0kDP000008l7bvYAA'))
            and extract(date from opp.created_date) > date_add(current_date, interval -8 year)

)ao

on inq.converted_opportunity_id = ao.opportunity_id ) )
