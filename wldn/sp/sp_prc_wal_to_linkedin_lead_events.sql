CREATE OR REPLACE PROCEDURE `rpt_external.sp_prc_wal_to_linkedin_lead_events`(IN AUDITKEY STRING, OUT RESULT STRING)
begin

 DECLARE
    v_startdate timestamp DEFAULT CURRENT_TIMESTAMP(); /* run pipeline for past 2 hours of data. pipeline runs every 10 mins */
  DECLARE
    fromdate timestamp DEFAULT TIMESTAMP_SUB(TIMESTAMP(DATETIME(CURRENT_TIMESTAMP(),"America/New_York")), INTERVAL 5 HOUR); --'2022-02-25 02:45:36.442828 UTC';

begin


  create or replace temp table final_cte as (

    WITH
      opps AS ( /* pull opps created on fromdate */
      SELECT
        opp.id,
        opp.created_date,
        opp.stage_name,
        opp.disposition_c,
        opp.contact_c,
        opp.system_modstamp,
        opp.Program_of_Interest_c,
        opp.channel_c,
        opp.client_user_agent_c,
        opp.event_source_url_c,
        NULL AS field,
        opp.transaction_id_c,
        opp.app_start_event_id_c,
        opp.app_submit_event_id_c,
        opp.fb_leadgen_id_c,
        '' as li_fat_id_c -- rs - deleted from Oppo
      FROM
        `raw_b2c_sfdc.opportunity` opp /* where left(string(opp.created_date,"America/New_York"), 10) = cast(fromdate as string) */
        left join `raw_b2c_sfdc.campaign` h on opp.campaign_id = h.id and h.is_deleted = false

      WHERE
        TIMESTAMP(DATETIME(opp.created_date,"America/New_York")) >= fromdate
        AND opp.institution_c = 'a0ko0000002BSH4AAO'
        AND opp.is_deleted = false
      UNION DISTINCT
        /* pull opps that had a stage change on fromdate */
      SELECT
        opp.id,
        opp.created_date,
        opp.stage_name,
        opp.disposition_c,
        opp.contact_c,
        opp.system_modstamp,
        opp.Program_of_Interest_c,
        opp.channel_c,
        opp.client_user_agent_c,
        opp.event_source_url_c,
        ofh.field,
        opp.transaction_id_c,
        opp.app_start_event_id_c,
        opp.app_submit_event_id_c,
        opp.fb_leadgen_id_c,
        '' as li_fat_id_c -- rs - deleted from Oppo
      FROM
        `raw_b2c_sfdc.opportunity` opp
      JOIN
        `raw_b2c_sfdc.opportunity_field_history` ofh
      ON
        opp.id = ofh.opportunity_id /* and left(string(ofh.created_date,"America/New_York"), 10) = cast(fromdate as string) */
        AND TIMESTAMP(DATETIME(ofh.created_date,"America/New_York")) >= fromdate
        AND ofh.field IN ( 'StageName','Disposition__c')
      left join `raw_b2c_sfdc.campaign` h on opp.campaign_id = h.id and h.is_deleted = false
      where
         opp.institution_c = 'a0ko0000002BSH4AAO'
        AND opp.is_deleted = false


      UNION DISTINCT
        /* pull opps that had a Application Started on fromdate */
      SELECT
        opp.id,
        opp.created_date,
        'Applicant' AS stage_name,
        ofh_disp.old_value AS disposition_c,
        opp.contact_c,
        opp.system_modstamp,
        opp.Program_of_Interest_c,
        opp.channel_c,
        opp.client_user_agent_c,
        opp.event_source_url_c,
        ofh.field,
        opp.transaction_id_c,
        opp.app_start_event_id_c,
        opp.app_submit_event_id_c,
        opp.fb_leadgen_id_c,
        '' as li_fat_id_c -- rs - deleted from Oppo
      FROM
        `raw_b2c_sfdc.opportunity` opp
      JOIN
        `raw_b2c_sfdc.opportunity_field_history` ofh
      ON
        opp.id = ofh.opportunity_id
        AND ofh.field = 'Application_Started__c'
        AND ofh.new_value ='true'
      JOIN (
        SELECT
          opportunity_id,
          created_date,
          field,
          old_value,
          ROW_NUMBER() OVER(PARTITION BY opportunity_id ORDER BY created_date ) rn
        FROM
          `raw_b2c_sfdc.opportunity_field_history` ofh_disp1
        WHERE
          ofh_disp1.field IN ( 'Disposition__c') )ofh_disp
      ON
        opp.id = ofh_disp.opportunity_id
        AND rn = 1 /* where left(string(ofh.created_date,"America/New_York"), 10) = cast(fromdate as string) */
      left join `raw_b2c_sfdc.campaign` h on opp.campaign_id = h.id and h.is_deleted = false
      WHERE
        opp.institution_c = 'a0ko0000002BSH4AAO'
        AND opp.is_deleted = false
        AND TIMESTAMP(DATETIME(ofh.created_date,"America/New_York")) >= fromdate ),
      opp_stage AS (
      SELECT
        opps.id,
        ofh.created_date,
        ofh.field,
        new_value
      FROM
        opps
      JOIN
        `raw_b2c_sfdc.opportunity_field_history` ofh
      ON
        opps.id = ofh.opportunity_id
      WHERE
        /* left(string(ofh.created_date,"America/New_York"), 10) <= cast(fromdate as string) */ TIMESTAMP(DATETIME(ofh.created_date,"America/New_York")) <= fromdate
        AND ofh.field = 'StageName' ),
      stage_disposition_combined AS (
      SELECT
        opportunity_id,
        created_date,
        MAX(ofh_created_date) AS ofh_created_date,
        disposition,
        stage,
        contact_c,
        Program_of_Interest_c,
        channel_c,
        client_user_agent_c,
        event_source_url_c,
        transaction_id_c,
        app_start_event_id_c,
        app_submit_event_id_c,
        fb_leadgen_id_c,
        li_fat_id_c
      FROM (
        SELECT
          ofh.opportunity_id,
          opps.created_date,
          GREATEST(IFNULL(ofh.created_date,opps.created_date),IFNULL(opp_stage.created_date,opps.created_date)) AS ofh_created_date,
          CASE
            WHEN ofh.field = 'Disposition__c' THEN ofh.new_value
            WHEN ofh.field = 'Application_Started__c' THEN opps.disposition_c
          ELSE
          NULL
        END
          AS disposition,
          CASE
            WHEN ofh.field = 'Disposition__c' THEN COALESCE(opp_stage.new_value, opps.stage_name)
            WHEN ofh.field = 'Application_Started__c' THEN 'Applicant'
          ELSE
          NULL
        END
          AS stage,
          opps.contact_c,
          opps.Program_of_Interest_c,
          opps.channel_c,
          opps.client_user_agent_c,
          opps.event_source_url_c,
          opps.field,
          opps.transaction_id_c,
          opps.app_start_event_id_c,
          opps.app_submit_event_id_c,
          opps.fb_leadgen_id_c,
          opps.li_fat_id_c
        FROM
          `raw_b2c_sfdc.opportunity_field_history` ofh
        JOIN
          opps
        ON
          opps.id = ofh.opportunity_id
        LEFT JOIN
          opp_stage
        ON
          opp_stage.id = ofh.opportunity_id
          AND opp_stage.field = 'StageName'
        WHERE
          ofh.field IN ( 'Disposition__c',
            'Application_Started__c') /* and left(string(ofh.created_date,"America/New_York"), 10) = cast(fromdate as string) */
          AND TIMESTAMP(DATETIME(ofh.created_date,"America/New_York")) >= fromdate )
      GROUP BY
        opportunity_id,
        created_date,
        disposition,
        stage,
        contact_c,
        Program_of_Interest_c,
        channel_c,
        client_user_agent_c,
        event_source_url_c,
        field,
        transaction_id_c,
        app_start_event_id_c,
        app_submit_event_id_c,
        fb_leadgen_id_c,
        li_fat_id_c ),


      Begin_opp_stage AS (
      SELECT
        opps.id,
        ofh.created_date,
        ofh.field,
        old_value,
        ROW_NUMBER() OVER(PARTITION BY ofh.opportunity_id ORDER BY ofh.created_date ) rn
      FROM
        opps
      JOIN
        `raw_b2c_sfdc.opportunity_field_history` ofh
      ON
        opps.id = ofh.opportunity_id
      WHERE
        ofh.field = 'StageName' ),


      Begin_opp_disposition AS (
      SELECT
        opps.id,
        ofh.created_date,
        ofh.field,
        old_value,
        ROW_NUMBER() OVER(PARTITION BY ofh.opportunity_id ORDER BY ofh.created_date ) rn
      FROM
        opps
      JOIN
        `raw_b2c_sfdc.opportunity_field_history` ofh
      ON
        opps.id = ofh.opportunity_id
      WHERE
        ofh.field = 'Disposition__c' ),


      Begin_stage_disposition_combined AS (
      SELECT
        DISTINCT opps.id AS opportunity_id,
        opps.created_date,
        opps.created_date AS ofh_created_date,
        IFNULL(Begin_opp_disposition.old_value,opps.disposition_c) AS Begin_disposition,
        IFNULL(Begin_opp_stage.old_value,opps.stage_name) AS Begin_stage,
        opps.contact_c,
        opps.Program_of_Interest_c,
        opps.channel_c,
        opps.client_user_agent_c,
        opps.event_source_url_c,
        opps.transaction_id_c,
        opps.app_start_event_id_c,
        opps.app_submit_event_id_c,
        opps.fb_leadgen_id_c,
        opps.li_fat_id_c
      FROM
        opps
      LEFT JOIN
        Begin_opp_stage
      ON
        Begin_opp_stage.id = opps.id
        AND Begin_opp_stage.rn=1
      LEFT JOIN
        Begin_opp_disposition
      ON
        Begin_opp_disposition.id = opps.id
        AND Begin_opp_disposition.rn =1 ),


      all_stage_disposition AS (
      SELECT
        opportunity_id,
        created_date,
        ofh_created_date,
        disposition,
        CASE
          WHEN disposition = 'Complete - EA Ready for Review' THEN 'Applicant'
        ELSE
        stage
      END
        AS stage,
        contact_c,
        Program_of_Interest_c,
        channel_c,
        client_user_agent_c,
        event_source_url_c,
        transaction_id_c,
        app_start_event_id_c,
        app_submit_event_id_c,
        fb_leadgen_id_c,
        li_fat_id_c
      FROM
        stage_disposition_combined
      WHERE
        ((disposition = 'Complete - EA Ready for Review')
          OR (stage = 'Pre-enroll'
            AND disposition = 'Reserved')) /* and left(string(ofh_created_date,"America/New_York"), 10) = cast(fromdate as string) */
        AND TIMESTAMP(DATETIME(ofh_created_date,"America/New_York")) >= fromdate
      UNION ALL
        /* pick both New and In-Process Dispositions for initiatecheckout */
      SELECT
        * EXCEPT (rownum)
      FROM (
        SELECT
          opportunity_id,
          created_date,
          ofh_created_date,
          disposition,
          stage,
          contact_c,
          Program_of_Interest_c,
          channel_c,
          client_user_agent_c,
          event_source_url_c,
          transaction_id_c,
          app_start_event_id_c,
          app_submit_event_id_c,
          fb_leadgen_id_c,
          li_fat_id_c,
          ROW_NUMBER() OVER (PARTITION BY opportunity_id, stage ORDER BY created_date ) AS rownum
        FROM
          stage_disposition_combined
        WHERE
          ( stage = 'Applicant'
            AND disposition IN ( 'New',
              'In Process',
              'Uncontacted',
              'New - No Outreach',
              "Admitted",
              "Admissions Review" ) ) )
      WHERE
        rownum = 1
      UNION ALL
        /* pick only one row if state is student. we dont need to care of disposition value. - I doubt though */
      SELECT
        * EXCEPT (rownum)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY opportunity_id, stage ORDER BY created_date, disposition) AS rownum
        FROM
          stage_disposition_combined
        WHERE
          stage IN ( 'Student' /* ,'Closed Lost' */ /*We do not need Closed lost data for LinkedIn. Just keeping this detail if in case a need arises in coming days.*/ ) /* and left(string(ofh_created_date,"America/New_York"), 10) = cast(fromdate as string) */
          AND TIMESTAMP(DATETIME(ofh_created_date,"America/New_York")) >= fromdate )
      WHERE
        rownum = 1
      UNION ALL
        /* pick begin stage and disposition */
      SELECT
        *
      FROM
        Begin_stage_disposition_combined ),
      contact_phone_list AS (
      SELECT
        opps.id,
        [COALESCE(CAST(REGEXP_REPLACE(preferred_phone_number_c, '[^[:digit:]]', '') AS string),NULL),
        COALESCE(CAST(REGEXP_REPLACE(home_phone, '[^[:digit:]]', '' ) AS string),NULL),
        COALESCE(CAST(REGEXP_REPLACE(mobile_phone, '[^[:digit:]]', '' ) AS string),NULL),
        COALESCE(CAST(REGEXP_REPLACE(other_phone, '[^[:digit:]]', '' ) AS string),NULL),
        COALESCE(CAST(REGEXP_REPLACE(phone, '[^[:digit:]]', '' ) AS string),NULL),
        COALESCE(CAST(REGEXP_REPLACE(c.work_phone_c, '[^[:digit:]]', '' ) AS string),NULL)] AS phone_numbers_list
      FROM
        `raw_b2c_sfdc.contact` c
      JOIN
        opps
      ON
        c.id = opps.contact_c ),
      contact_phone_list_final AS (
      SELECT
        id,
        ARRAY_AGG(DISTINCT string IGNORE NULLS) AS phone_numbers
      FROM
        contact_phone_list,
        UNNEST(phone_numbers_list) string
      GROUP BY
        id ),
      contact_email_list AS (
      SELECT
        opps.id,
        [ifnull(email, ""),
        ifnull(alternate_email_c, ""),
        ifnull(personal_email_c, "")] AS emails_list
      FROM
        `raw_b2c_sfdc.contact` c
      JOIN
        opps
      ON
        c.id = opps.contact_c ),
      contact_email_list_final AS (
      SELECT
        id,
        ARRAY_AGG(DISTINCT string IGNORE NULLS) AS emails
      FROM
        contact_email_list,
        UNNEST(emails_list) string
      GROUP BY
        id ),
      Final AS (
      SELECT
        ifnull(c.first_name, "") as first_name,
        ifnull(c.last_name, "") as last_name,
        array_to_string(contact_phone_list_final.phone_numbers, "|") AS phone,
        array_to_string(contact_email_list_final.emails, "|") AS email,
        ifnull(c.mailing_city, "") AS city,
        ifnull(c.mailing_state, "") AS state,
        ifnull(CAST(c.mailing_postal_code AS string), "") AS zip,
        EXTRACT(year
        FROM
          birthdate) AS dob_year,
        EXTRACT(month
        FROM
          birthdate) AS dob_month,
        EXTRACT(day
        FROM
          birthdate) AS dob_day,
        c.mailing_country AS country,
        ifnull(
          CASE /* when all_stage_disposition.stage = 'Closed Lost' then 'ClosedLost' */
            WHEN all_stage_disposition.stage = 'Student' THEN 'Student' /*Should there be a disposition filter for "Active (1st term)"*/
            WHEN all_stage_disposition.stage = 'Applicant'
          AND all_stage_disposition.disposition = 'New' THEN 'ApplicationStart'
            WHEN all_stage_disposition.stage = 'Applicant' AND all_stage_disposition.disposition = 'In Process' THEN 'ApplicationStart'
            WHEN all_stage_disposition.stage = 'Applicant'
          AND all_stage_disposition.disposition = 'Uncontacted' THEN 'ApplicationStart'
            WHEN all_stage_disposition.stage = 'Applicant' AND all_stage_disposition.disposition = 'New - No Outreach' THEN 'ApplicationStart'
            WHEN all_stage_disposition.stage = 'Applicant'
          AND all_stage_disposition.disposition = 'Admitted' THEN 'Admitted'
            WHEN all_stage_disposition.stage = 'Applicant' AND all_stage_disposition.disposition = 'Admissions Review' THEN 'App Complete'
            WHEN all_stage_disposition.stage = 'Applicant'
          AND all_stage_disposition.disposition = 'Complete - EA Ready for Review' THEN 'App Complete'
            WHEN all_stage_disposition.stage = 'Pre-enroll' AND all_stage_disposition.disposition = 'Reserved' THEN 'Reserve'
          else all_stage_disposition.disposition /* For all the dispositions we are not interested in. */

        END, "")
        AS event_name,
        ifnull(all_stage_disposition.channel_c, "") AS event_source,
        ifnull(p.name, "") AS program_name,
        /* event_id should be unique. It is used for matching on linkedin side */
        ifnull(
          CASE
            WHEN all_stage_disposition.fb_leadgen_id_c IS NOT NULL THEN NULL -- pass null event_id for fb AD generated ones
          /* when all_stage_disposition.stage = 'Closed Lost' then concat(all_stage_disposition.opportunity_id, 'CL') */
            WHEN all_stage_disposition.stage = 'Student' THEN CONCAT(all_stage_disposition.opportunity_id, 'S')
            WHEN all_stage_disposition.stage = 'Applicant' THEN CASE /* possible: remove app_start_event_id_c | On hold for now. - Prasanna */
            WHEN all_stage_disposition.disposition = 'New' THEN COALESCE(all_stage_disposition.app_start_event_id_c, CONCAT(all_stage_disposition.opportunity_id, 'AN'))
            WHEN all_stage_disposition.disposition = 'In Process' THEN COALESCE(all_stage_disposition.app_start_event_id_c, CONCAT(all_stage_disposition.opportunity_id, 'AI'))
            WHEN all_stage_disposition.disposition = 'Uncontacted' THEN COALESCE(all_stage_disposition.app_start_event_id_c, CONCAT(all_stage_disposition.opportunity_id, 'AU'))
            WHEN all_stage_disposition.disposition = 'New - No Outreach' THEN COALESCE(all_stage_disposition.app_start_event_id_c, CONCAT(all_stage_disposition.opportunity_id, 'ANN'))
            WHEN all_stage_disposition.disposition = 'Complete - EA Ready for Review' THEN COALESCE(all_stage_disposition.app_submit_event_id_c, CONCAT(all_stage_disposition.opportunity_id, 'AR'))
            WHEN all_stage_disposition.disposition = 'Admitted' THEN COALESCE(all_stage_disposition.app_submit_event_id_c, CONCAT(all_stage_disposition.opportunity_id, 'AAD'))
            WHEN all_stage_disposition.disposition = 'Admissions Review' THEN COALESCE(all_stage_disposition.app_submit_event_id_c, CONCAT(all_stage_disposition.opportunity_id, 'AAR'))
          END
            WHEN all_stage_disposition.stage = 'Pre-enroll' AND all_stage_disposition.disposition = 'Reserved' THEN CONCAT(all_stage_disposition.opportunity_id, 'PR')
          END
        ,
        concat(all_stage_disposition.opportunity_id, 'OTHR') /* cast(generate_uuid() as string) */
        )
        AS event_id,
        CASE
          WHEN /* all_stage_disposition.stage = 'Closed Lost' or all_stage_disposition.stage = 'Student'

    or */ (all_stage_disposition.stage = 'Applicant' AND all_stage_disposition.disposition IN ( 'New', 'In Process', 'Uncontacted', 'New - No Outreach', 'Complete - EA Ready for Review', "Admitted", "Admissions Review" ) ) OR (all_stage_disposition.stage = 'Pre-enroll' AND all_stage_disposition.disposition = 'Reserved') OR (all_stage_disposition.stage = 'Student' AND all_stage_disposition.disposition = 'Student') THEN UNIX_SECONDS(all_stage_disposition.ofh_created_date)
        ELSE
        UNIX_SECONDS(all_stage_disposition.created_date)
      END
        AS event_time,
        0 AS lead_score_tof,
        0 AS lead_score_dynamic,
        CASE
          WHEN all_stage_disposition.stage = 'Student' THEN 'Student' /* when all_stage_disposition.stage = 'Closed Lost' then 'Closed Lost' */ /* when c.is_walden_alum_c then 'Alumni' */
        ELSE
        CONCAT(all_stage_disposition.stage, '-', all_stage_disposition.disposition)
      END
        crm_stage_disposition,
        c.is_walden_alum_c AS is_walden_alum,
        ifnull(all_stage_disposition.client_user_agent_c, "") AS client_user_agent,
        CASE
          WHEN all_stage_disposition.fb_leadgen_id_c IS NOT NULL THEN 'system_generated'
          WHEN all_stage_disposition.client_user_agent_c IS NOT NULL THEN 'Website'
        ELSE
        'Other'
      END
        AS action_source,
        ifnull(all_stage_disposition.event_source_url_c, "") AS event_source_url,
        CURRENT_TIMESTAMP() AS etl_created_date,
        /* ------------for manually inserting (Testing) ------------------------ '22f51867-f40b-4d95-b4f2-3fdb3ac9930c' as etl_ins_audit_key , -- */ AUDITKEY AS etl_ins_audit_key,
        'BigQuery: stored procedure loads' AS etl_resource_name,
        ifnull(all_stage_disposition.fb_leadgen_id_c, "") as fb_leadgen_id_c,
        all_stage_disposition.opportunity_id,
        ifnull(all_stage_disposition.li_fat_id_c, "") as li_fat_id_c,
      FROM
        all_stage_disposition
      JOIN
        `raw_b2c_sfdc.contact` c
      ON
        all_stage_disposition.contact_c = c.id
      JOIN
        `raw_b2c_sfdc.product_2` p
      ON
        all_stage_disposition.Program_of_Interest_c = p.id
      LEFT JOIN
        contact_phone_list_final
      ON
        all_stage_disposition.opportunity_id = contact_phone_list_final.id
      LEFT JOIN
        contact_email_list_final
      ON
        all_stage_disposition.opportunity_id = contact_email_list_final.id
      WHERE
        TIMESTAMP(DATETIME(IFNULL(all_stage_disposition.ofh_created_date,all_stage_disposition.created_date),"America/New_York")) >= fromdate
    )

    /* FINAL JOINS */
    select * except(wfl_event_id, wfl_event_name, wfl_event_time)
    from (
      SELECT
        f.* EXCEPT (rn), coalesce(country.iso_code_c, "") as iso_code_c, wfl.event_id as wfl_event_id, wfl.event_name as wfl_event_name, wfl.event_time as wfl_event_time
      FROM (
          SELECT
            *
          FROM (
            SELECT
              ROW_NUMBER() OVER (PARTITION BY opportunity_id, event_name ORDER BY event_time) AS rn,
              *
            FROM
              Final
            WHERE
              event_name IS NOT NULL )
          WHERE
            rn = 1
        ) as f
        left join `raw_b2c_sfdc.country_c` as country
          on lower(f.country) = lower(country.name)
        left join (
          select *
          from `rpt_external.prc_wal_to_linkedin_lead_events`
          where
            api_response LIKE "%201%" /* or lower(api_response) like "%email%empty%" or lower(api_response) like "%record%older%than%90%day%" */
              and etl_created_date >= fromdate
        ) as wfl
          on left(wfl.event_id,18) = left(f.event_id,18)
            -- and wfl.event_name = f.event_name
            and (case when wfl.event_name = 'ApplicationCompleted' then 'App Complete' else wfl.event_name end) = f.event_name
            and wfl.event_time = f.event_time
        -- where f.email is not null
    )
    where wfl_event_id is null and wfl_event_name is null and wfl_event_time is null
  )
  ;

  insert into `rpt_external.prc_wal_to_linkedin_lead_events`
  (
    first_name, last_name,phone,email,city,state,zip,dob_year,dob_month,dob_day,country,
    event_name, event_source,program_name,event_id,event_time,lead_score_tof,lead_score_dynamic,
    crm_stage_disposition,is_walden_alum, client_user_agent,action_source,event_source_url,
    etl_created_date,etl_ins_audit_key,etl_resource_name, fb_leadgen_id_c, opportunity_id,li_fat_id_c,iso_code_c, api_response
  )
  select *,
    case when email = "" or email is null then "email_empty"
      when timestamp_diff(timestamp_millis(unix_millis(current_timestamp)), timestamp_millis(cast(event_time as int64) * 1000), day) > 90 then "record old than 90 days"
    end as api_response
  from final_cte
  where email is null or email = "" or (timestamp_diff(timestamp_millis(unix_millis(current_timestamp)), timestamp_millis(cast(event_time as int64) * 1000), day) > 90)
  limit 1
  ;

  EXPORT DATA OPTIONS(
    -- field_delimiter=";",
    format="JSON",
    -- header=TRUE,
    OVERWRITE=TRUE,
    uri="gs://gcp-atge-dev-data-export/wldn_linkedin_capi/wldn_linkedin_capi-export-*-.txt"
    -- uri="gs://gcp-atge-dev-data-export/test_wldn_linkedin_capi/wldn_linkedin_capi-export-*-.txt"
  ) AS
  select *
  from final_cte
  where (email is not null) and (email != "") and  (timestamp_diff(timestamp_millis(unix_millis(current_timestamp)), timestamp_millis(cast(event_time as int64) * 1000), day) < 90) limit 1
  -- limit 99999999
  ;

  -- EXPORT DATA OPTIONS(
  --   -- field_delimiter=";",
  --   format="JSON",
  --   -- header=TRUE,
  --   OVERWRITE=TRUE,
  --   uri="gs://gcp-atge-dev-gcs-external-transfer-trigger/wldn_linkedin_capi/start_pipeline-wldn_linkedin_capi-export-*-.txt"
  -- ) AS
  -- select 1;

  insert into `audit_cdw_log.audit_load_details` (audit_load_key,job_name,job_start_dt,job_end_dt,job_completed_ind,job_type,load_method)
  values (AUDITKEY,'LEADS_TO_linkedin',v_startdate, current_timestamp(),'COMPLETED','send_lead_events_to_linkedin_conversion_api','sp_wal_to_linkedin_lead_events');
  set RESULT = 'SUCCESS';




EXCEPTION when ERROR then

  insert into `audit_cdw_log.audit_load_details` (audit_load_key, job_name, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method)
  values (
    AUDITKEY,'WLDN_LEADS_TO_LINKEDIN',(
      select job_start_dt
      from `audit_cdw_log.audit_load_details`
      where audit_load_key = AUDITKEY
        and job_name = "WLDN_LEADS_TO_LINKEDIN"
        and job_type = "send_lead_events_to_linkedin_conversion_api"
        and load_method = "sp_wal_to_linkedin_lead_events" limit 1
      ),
      current_timestamp(),
      'FAILED',
      'send_lead_events_to_linkedin_conversion_api',
      'sp_wal_to_linkedin_lead_events'
    )
  ;

set RESULT = @@error.message;


end;

end
