WITH
  writing_period AS (
  SELECT
    UPPER(d.cu_onsite_writing_period) AS writing_period,
    MIN(d.cal_dt) AS wp_begin_date,
    MAX(d.cal_dt) AS wp_end_date
  FROM
    daas-cdw-prod.mdm.dim_date d
  WHERE
    d.cu_onsite_writing_period IS NOT NULL
  GROUP BY
    d.cu_onsite_writing_period ),
  activity_tasks AS (
  SELECT
    DISTINCT who_id as whoid,
    what_id as whatid,
    created_date AS activity_created_date,
    CASE
      WHEN skill_c LIKE ('RN_%') OR skill_c LIKE ('Grad_%') OR skill_c LIKE ('Pre_%') THEN 1
    ELSE
    0
  END
    AS dialer_touched
  FROM
    raw_b2c_sfdc.task
  WHERE
    is_deleted=False
    AND EXTRACT(date FROM created_date) > DATE_ADD(current_date, INTERVAL -8 year)
    and what_id in (select id from raw_b2c_sfdc.opportunity where is_deleted=false AND institution_c in ('a0kDP000008l7bvYAA'))
  )

select
  t_1.*
  , sfdc_lead.drips_state_c
from (
SELECT
  DISTINCT prospect_id,
  inquiry_id,
  inquiry_created_date,
  location_code,
  program_code,
  program_group_code,
  campaign_id,
  prospect_owner_id,
  prospect_status,
  inquiry_scoring_tier,
  response_score,
  hdyhau,
  attendance_preference,
  prospect_type,
  address_country_inquiry,
  address_state_inquiry,
  address_postal_code_inquiry,
  writing_period,
  CASE
    WHEN MAX(dialer_touched)=1 THEN 'TRUE'
    WHEN MAX(dialer_touched)=0 THEN 'FALSE'
  ELSE
  NULL
END
  AS dialer_touched_ind
FROM (
  SELECT
    up.*,
    t.dialer_touched
  FROM ( (
      SELECT
        DISTINCT prospect_id,
        inquiry_id,
        contact_id,
        opportunity_id,
        inquiry_created_date,
        location_code,
        program_code,
        program_group_code,
        campaign_id,
        prospect_owner_id,
        prospect_status,
        inquiry_scoring_tier,
        response_score,
        hdyhau,
        attendance_preference,
        prospect_type,
        address_country_inquiry,
        address_state_inquiry,
        address_postal_code_inquiry,
        writing_period,
        wp_begin_date,
        wp_end_date
      FROM (
          -- Linking only to the first inquiry within the writing period
        SELECT
          DISTINCT np.*,
          writing_period,
          wp_begin_date,
          wp_end_date,
          ROW_NUMBER() OVER (PARTITION BY prospect_id, writing_period ORDER BY inquiry_created_date, IFNULL(opp_create_date, TIMESTAMP(PARSE_DATE('%d/%m/%Y','01/01/9999')) ) ASC) AS rank
        FROM
          rpt_crm_mart.v_cu_kpi_unique_prospects np
          -- Get writing period
        LEFT JOIN
          writing_period d
        ON
          EXTRACT(date
          FROM
            CAST(DATETIME(inquiry_created_date, "US/Central") AS datetime)) BETWEEN wp_begin_date
          AND wp_end_date
        WHERE
          (modality_type='ONSITE'
            AND UPPER(program_group_code) IN ('BSN',
              'UNKNOWN',
              'OTHERS'))
          OR program_group_code IN ('BSN_ONLINE') )
      WHERE
        rank=1 ) up
      -- Get all the tasks with dialer activities and falls within the writing period
    LEFT JOIN
      activity_tasks t
    ON
      up.prospect_id = t.whoid
      AND EXTRACT(date
      FROM
        CAST(DATETIME(t.activity_created_date, "US/Central") AS datetime)) BETWEEN wp_begin_date
      AND wp_end_date )
  UNION ALL
  SELECT
    up.*,
    t.dialer_touched
  FROM ( (
      SELECT
        DISTINCT prospect_id,
        inquiry_id,
        contact_id,
        opportunity_id,
        inquiry_created_date,
        location_code,
        program_code,
        program_group_code,
        campaign_id,
        prospect_owner_id,
        prospect_status,
        inquiry_scoring_tier,
        response_score,
        hdyhau,
        attendance_preference,
        prospect_type,
        address_country_inquiry,
        address_state_inquiry,
        address_postal_code_inquiry,
        writing_period,
        wp_begin_date,
        wp_end_date
      FROM (
          -- Linking only to the first inquiry within the writing period
        SELECT
          DISTINCT np.*,
          writing_period,
          wp_begin_date,
          wp_end_date,
          ROW_NUMBER() OVER (PARTITION BY prospect_id, writing_period ORDER BY inquiry_created_date, IFNULL(opp_create_date, TIMESTAMP(PARSE_DATE('%d/%m/%Y','01/01/9999')) ) ASC) AS rank
        FROM
          rpt_crm_mart.v_cu_kpi_unique_prospects np
          -- Get writing period
        LEFT JOIN
          writing_period d
        ON
          EXTRACT(date
          FROM
            CAST(DATETIME(inquiry_created_date, "US/Central") AS datetime)) BETWEEN wp_begin_date
          AND wp_end_date
        WHERE
          (modality_type='ONSITE'
            AND UPPER(program_group_code) IN ('BSN',
              'UNKNOWN',
              'OTHERS'))
          OR program_group_code IN ('BSN_ONLINE') )
      WHERE
        rank=1 ) up
      -- Get all the tasks with dialer activities and falls within the writing period
    LEFT JOIN
      activity_tasks t
    ON
      up.contact_id=t.whoid
      AND EXTRACT(date
      FROM
        CAST(DATETIME(t.activity_created_date, "US/Central") AS datetime)) BETWEEN wp_begin_date
      AND wp_end_date )
  UNION ALL
  SELECT
    up.*,
    t.dialer_touched
  FROM ( (
      SELECT
        DISTINCT prospect_id,
        inquiry_id,
        contact_id,
        opportunity_id,
        inquiry_created_date,
        location_code,
        program_code,
        program_group_code,
        campaign_id,
        prospect_owner_id,
        prospect_status,
        inquiry_scoring_tier,
        response_score,
        hdyhau,
        attendance_preference,
        prospect_type,
        address_country_inquiry,
        address_state_inquiry,
        address_postal_code_inquiry,
        writing_period,
        wp_begin_date,
        wp_end_date
      FROM (
          -- Linking only to the first inquiry within the writing period
        SELECT
          DISTINCT np.*,
          writing_period,
          wp_begin_date,
          wp_end_date,
          ROW_NUMBER() OVER (PARTITION BY prospect_id, writing_period ORDER BY inquiry_created_date, IFNULL(opp_create_date, TIMESTAMP(PARSE_DATE('%d/%m/%Y','01/01/9999')) ) ASC) AS rank
        FROM
          rpt_crm_mart.v_cu_kpi_unique_prospects np
          -- Get writing period
        LEFT JOIN
          writing_period d
        ON
          EXTRACT(date
          FROM
            CAST(DATETIME(inquiry_created_date, "US/Central") AS datetime)) BETWEEN wp_begin_date
          AND wp_end_date
        WHERE
          (modality_type='ONSITE'
            AND UPPER(program_group_code) IN ('BSN',
              'UNKNOWN',
              'OTHERS'))
          OR program_group_code IN ('BSN_ONLINE') )
      WHERE
        rank=1 ) up
      -- Get all the tasks with dialer activities and falls within the writing period
    LEFT JOIN
      activity_tasks t
    ON
      up.opportunity_id = t.whatid
      AND EXTRACT(date
      FROM
        CAST(DATETIME(t.activity_created_date, "US/Central") AS datetime)) BETWEEN wp_begin_date
      AND wp_end_date ) )
GROUP BY
  prospect_id,
  inquiry_id,
  inquiry_created_date,
  location_code,
  program_code,
  program_group_code,
  campaign_id,
  prospect_owner_id,
  prospect_status,
  inquiry_scoring_tier,
  response_score,
  hdyhau,
  attendance_preference,
  prospect_type,
  address_country_inquiry,
  address_state_inquiry,
  address_postal_code_inquiry,
  writing_period
) as t_1
left join `raw_b2c_sfdc.lead` as sfdc_lead
on t_1.prospect_id = sfdc_lead.id and sfdc_lead.is_deleted=false AND (sfdc_lead.institution_c in ('a0kDP000008l7bvYAA') OR sfdc_lead.company in ('Chamberlain')) -- rs - added CU filter