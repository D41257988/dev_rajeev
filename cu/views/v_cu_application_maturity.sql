SELECT
  'CU' AS institution,
  2 AS institution_id,
  dsi,
  program_at_registration,
  session_at_registration,
  original_registration_date,
  location_at_registration,
  zipcode_at_registration,
  opportunity_id,
  program_at_application,
  session_at_application,
  application_stage,
  original_start_date,
  approved_application_date,
  current_start_date,
  location_at_application,
  zipcode_at_application,
  country_at_application,
  inquiry_id,
  campaign_id,
  new_stu.lead_id,
  program_at_inquiry,
  inquiry_created_date,
  inquiry_scoring_tier,
  inquiry_score,
  location_at_inquiry,
  state_at_inquiry,
  zipcode_at_inquiry,
  country_at_inquiry,
  SPLIT(prior_inq_cmp_1, '|')[ORDINAL(1)] AS prior_inquiry_1,
  SPLIT(prior_inq_cmp_1, '|')[ORDINAL(2)] AS prior_campaign_1,
  SPLIT(prior_inq_cmp_2, '|')[ORDINAL(1)] AS prior_inquiry_2,
  SPLIT(prior_inq_cmp_2, '|')[ORDINAL(2)] AS prior_campaign_2,
  SPLIT(prior_inq_cmp_3, '|')[ORDINAL(1)] AS prior_inquiry_3,
  SPLIT(prior_inq_cmp_3, '|')[ORDINAL(2)] AS prior_campaign_3,
  student_status
FROM (
  SELECT
    dsi,
    program_at_registration,
    session_at_registration,
    original_registration_date,
    location_at_registration,
    zipcode_at_registration,
    sf.*,
    student_status
  FROM
    -- Getting New students dataset from cohort
    (
    SELECT
      DISTINCT dsi,
      guid,
      academic_period AS session_at_registration,
      program AS program_at_registration,
      original_registration_date,
      registered_ind,
      location_code AS location_at_registration,
      zip_code AS zipcode_at_registration,
      student_status
    FROM (
      SELECT
        DISTINCT scd.dsi,
        guid,
        academic_period,
        program,
        original_registration_date,
        registered_ind,
        location_code,
        CASE
          WHEN ((original_registration_date BETWEEN address_start_date AND address_end_date) AND address_type_desc IN ('Permanent Home Address', 'Mailing', 'Billing') ) THEN postal_code
          WHEN ((address_end_date IS NULL
            OR address_end_date >= original_registration_date)
          AND address_type_desc IN ('Permanent Home Address',
            'Mailing',
            'Billing') ) THEN postal_code
          WHEN (original_registration_date >= address_start_date AND address_type_desc IN ('Permanent Home Address', 'Mailing', 'Billing') ) THEN postal_code
        ELSE
        postal_code
      END
        AS zip_code,
        ROW_NUMBER() OVER (PARTITION BY scd.dsi, original_registration_date, guid, academic_period, program ORDER BY address_number DESC) AS rnk,
        address_number,
        address_start_date,
        address_end_date,
        scd.student_status
      FROM (
        SELECT
          DISTINCT dsi,
          CASE
            WHEN app_guid = 'NA' THEN legacy_app_guid
          ELSE
          app_guid
        END
          AS guid,
          academic_period,
          program,
          original_registration_date,
          registered_ind,
          campus AS location_code,
          CASE
            WHEN (cast(ACADEMIC_PERIOD as int64) < 202130) THEN (LEGACY_SESN_STATUS_CENSUS)
          ELSE
          (IFNULL(STUDENT_STATUS_CENSUS, STUDENT_STATUS))
        END
          AS Student_Status
        FROM
          rpt_academics.t_student_cohort_detail
        WHERE
          institution ='CU'
          AND registered_ind = "Y"
          AND LOWER(
            CASE
              WHEN (cast(ACADEMIC_PERIOD as int64) < 202130 ) THEN (LEGACY_SESN_STATUS_CENSUS)
            ELSE
            (IFNULL(STUDENT_STATUS_CENSUS, STUDENT_STATUS))
          END
            ) IN ("new",
            "readmit")

            and cast(academic_period as int64) >= 201240

              ) scd
      LEFT JOIN

        (
        SELECT
          DISTINCT dsi,
          SPLIT(postal_code, '-')[ORDINAL(1)] AS postal_code,
          address_type_desc,
          address_number,
          address_start_date,
          address_end_date
        FROM
          rpt_academics.t_address
        WHERE
          institution = 'CU'
          AND institution_id = 2
          AND postal_code IS NOT NULL ) da
      ON
        scd.dsi=da.dsi )
    WHERE
      rnk=1 ) cohort

  LEFT JOIN (
    SELECT
       DISTINCT
      opp.id as opportunity_id,
      opp.program_code_c AS program_at_application,
      sess.session_code_c AS session_at_application,
      opp.stage_name AS application_stage,
      opp.original_intended_start_date_c AS original_start_date,
      DATETIME(opp.app_submitted_date_time_c,"America/Chicago") AS approved_application_date,
      opp.session_start_date_c AS current_start_date,
      opp.location_c AS location_at_application,
      SPLIT(contact.mailing_postal_code, '-') [ORDINAL(1)] AS zipcode_at_application,
      bp.citizenship_type_c AS country_at_application,
      inq.*
     FROM
      raw_b2c_sfdc.opportunity opp
      left join raw_b2c_sfdc.contact contact on opp.contact_c = contact.id
      left join raw_b2c_sfdc.brand_profile_c bp on opp.brand_profile_c = bp.id
      left join raw_b2c_sfdc.session_c sess on opp.session_c = sess.id
      left join (
      select  distinct
        lead.id as inquiry_id, -- rs - should we remove it coz we using lead's id as inquiry's id?
        lead.campaign_c as campaign_id,
        lead.id as lead_id,
        lead.program_of_interest_from_inquiry_schema_c as program_at_inquiry,
        DATETIME(lead.created_date, "America/Chicago") AS inquiry_created_date,
        lead.iq_quality_grade_c AS inquiry_scoring_tier,
        lead.iq_response_score_c AS inquiry_score,
        loc.location_code_c as location_at_inquiry,
        lead.raw_state_c AS state_at_inquiry,
        lead.raw_postal_code_c AS zipcode_at_inquiry,
        lead.raw_country_c AS country_at_inquiry,
        lead.converted_opportunity_id as converted_opportunity_id

          from raw_b2c_sfdc.lead lead
            left join raw_b2c_sfdc.location_c loc on lead.location_c = loc.id
        where lead.is_deleted=false AND (lead.institution_c in ('a0kDP000008l7bvYAA') OR lead.company in ('Chamberlain'))

    ) inq ON opp.id = inq.converted_opportunity_id
    WHERE
       --opp.credited_raw_inquiry IS NOT NULL -- rs - this condition might not be needed. Check again in future
       opp.is_deleted = False
      and opp.institution_c in ('a0kDP000008l7bvYAA')
  ) sf
  ON cohort.guid = sf.opportunity_id ) new_stu
  /* Get previous campaigns & corresponding inquries for past 180 days */
LEFT JOIN (
  SELECT
    lead_id,
    p._1 AS prior_inq_cmp_1,
    p._2 AS prior_inq_cmp_2,
    p._3 AS prior_inq_cmp_3
  FROM (
    SELECT
      lead_id,
      inq_cmp,
      rnk
    FROM (
       SELECT
        CONCAT(id, '|', campaign_c) AS inq_cmp,
        id as lead_id,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_date DESC) AS rnk
      FROM
        raw_b2c_sfdc.lead
      WHERE
        is_deleted = False
        AND created_date >= DATE_SUB( created_date, INTERVAL 180 day)
        )
    WHERE
      rnk<=3 ) PIVOT( MAX(inq_cmp) FOR rnk IN (1,
        2,
        3)) AS p ) camp
ON
  new_stu.lead_id = camp.lead_id