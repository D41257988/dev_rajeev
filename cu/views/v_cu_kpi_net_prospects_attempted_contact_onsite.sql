create or replace view rpt_crm_mart.v_cu_kpi_net_prospects_attempted_contact_onsite as
SELECT
  DISTINCT TRIM(prospect_id) prospect_id,
  TRIM(inquiry_id) inquiry_id,
  TRIM(opportunity_id) opportunity_id,
  TRIM(task_id) task_id,
  task_owner_id,
  campaign_id,
  prospect_owner_id,
  inquiry_scoring_tier,
  inquiry_created_date,
  task_created_date,
  original_intended_start_date,
  session_start_date,
  approved_application_date,
  DATE_DIFF(DATETIME(task_created_date, "US/Central"), DATETIME(inquiry_created_date, "US/Central"), day) inq_task_day_diff,
  TIMESTAMP_DIFF(DATETIME(task_created_date, "US/Central"), DATETIME(inquiry_created_date, "US/Central"), hour) inq_task_hours_diff,
  DATE_DIFF(DATETIME(approved_application_date, "US/Central"), DATETIME(inquiry_created_date, "US/Central"), day) inq_appl_day_diff,
  TIMESTAMP_DIFF(DATETIME(approved_application_date, "US/Central"), DATETIME(inquiry_created_date, "US/Central"), hour) inq_appl_hours_diff,
  DATE_DIFF(DATETIME(approved_application_date, "US/Central"), DATETIME(task_created_date, "US/Central"), day) task_appl_day_diff,
  TIMESTAMP_DIFF(DATETIME(approved_application_date, "US/Central"), DATETIME(task_created_date, "US/Central"), hour) task_appl_hours_diff,
  prospect_status,
  location_code,
  modality_type,
  program_group_code,
  CASE
    WHEN dialer_touched_ind=1 THEN 'TRUE'
    WHEN dialer_touched_ind=0 THEN 'FALSE'
  ELSE
  NULL
END
  AS dialer_touched_ind,
  asc_touched_ind,
  writing_period,
  sfdc_lead.drips_state_c
FROM
  -- Get the very first contacted tasks after the valid inquiry
  (
  SELECT
    DISTINCT prospect_id,
    inquiry_id,
    opportunity_id,
    task_id,
    task_owner_id,
    campaign_id,
    prospect_owner_id,
    inquiry_scoring_tier,
    inquiry_created_date,
    task_created_date,
    ROW_NUMBER() OVER (PARTITION BY prospect_id, d.writing_period ORDER BY inquiry_created_date, IFNULL(task_created_date, TIMESTAMP(PARSE_DATE('%d/%m/%Y','01/01/9999')) ) ASC) AS inq_task_rank,
    original_intended_start_date,
    session_start_date,
    MAX(approved_application_date) OVER (PARTITION BY prospect_id, d.writing_period, opportunity_id) AS approved_application_date,
    prospect_status,
    location_code,
    modality_type,
    program_group_code,
    MAX(CASE
        WHEN np.dialer_touched_ind='TRUE' THEN 1
        WHEN np.dialer_touched_ind='FALSE' THEN 0
      ELSE
      NULL
    END
      ) OVER (PARTITION BY prospect_id, d.writing_period ) AS dialer_touched_ind,
    asc_touched_ind,
    d.writing_period
  FROM
    rpt_crm_mart.v_cu_kpi_total_attempted_contacts_net_prospects v
  LEFT JOIN (
    SELECT
      UPPER(d.cu_onsite_writing_period) AS writing_period,
      MIN(d.cal_dt) AS wp_begin_date,
      MAX(d.cal_dt) AS wp_end_date
    FROM
      mdm.dim_date d
    WHERE
      d.cu_onsite_writing_period IS NOT NULL
    GROUP BY
      d.cu_onsite_writing_period ) d
  ON
    ( EXTRACT(date
      FROM
        CAST(DATETIME(inquiry_created_date, "US/Central") AS datetime)) BETWEEN wp_begin_date
      AND wp_end_date )
  LEFT JOIN (
    SELECT
      DISTINCT prospect_id AS lead_id,
      dialer_touched_ind,
      writing_period
    FROM
      rpt_crm_mart.v_cu_kpi_net_prospects_onsite) np
  ON
    v.prospect_id = np.lead_id
    AND d.writing_period = np.writing_period
  WHERE
    (v.modality_type='ONSITE'
      AND UPPER(program_group_code) IN ('BSN',
        'UNKNOWN',
        'OTHERS'))
    OR program_group_code IN ('BSN_ONLINE') ) as t_1
LEFT JOIN `raw_b2c_sfdc.lead` as sfdc_lead
    on t_1.prospect_id = sfdc_lead.id and sfdc_lead.is_deleted=false AND (sfdc_lead.institution_c in ('a0kDP000008l7bvYAA') OR sfdc_lead.company in ('Chamberlain')) -- rs - added CU filter
WHERE
  inq_task_rank = 1
  AND writing_period IS NOT NULL
