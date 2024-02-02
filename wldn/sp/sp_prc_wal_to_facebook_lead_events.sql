CREATE OR REPLACE PROCEDURE `trans_bi_offline_conversion.sp_prc_wal_to_facebook_lead_events`(IN AUDITKEY STRING, OUT RESULT STRING)
begin

declare v_startdate timestamp default current_timestamp();

/* run pipeline for past 2 hours of data. pipeline runs every 15 mins */
declare fromdate timestamp default timestamp_sub(timestamp(datetime(current_timestamp(),"America/New_York")), interval 2 hour); --'2022-02-25 02:45:36.442828 UTC';


/* truncate prc table and load using query below. */

    truncate table `trans_bi_offline_conversion.prc_wal_to_facebook_lead_events`;

    insert into `trans_bi_offline_conversion.prc_wal_to_facebook_lead_events`
        (
            first_name, last_name,phone,email,city,state,zip,dob_year,dob_month,dob_day,country,
            event_name, event_source,program_name,event_id,event_time,lead_score_tof,lead_score_dynamic,
            crm_stage_disposition,is_walden_alum, client_user_agent,action_source,event_source_url,
            etl_created_date,etl_ins_audit_key,etl_resource_name, fb_leadgen_id_c, opportunity_id
        )



    with opps as (

    /* pull opps created on fromdate */
        select
            opp.id, opp.created_date, opp.stage_name,
            opp.disposition_c, opp.contact_c,
            opp.system_modstamp, opp.Program_of_Interest_c,
            opp.channel_c, opp.client_user_agent_c,
            opp.event_source_url_c, null as field,
            opp.transaction_id_c, opp.app_start_event_id_c,
            opp.app_submit_event_id_c, opp.fb_leadgen_id_c
        from `raw_b2c_sfdc.opportunity` opp
        /* where left(string(opp.created_date,"America/New_York"), 10) = cast(fromdate as string) */
        where timestamp(datetime(opp.created_date,"America/New_York")) >= fromdate
            and opp.institution_c = 'a0ko0000002BSH4AAO' and opp.is_deleted = false


        union distinct


    /* pull opps that had a stage change on fromdate */
        select
            opp.id, opp.created_date, opp.stage_name,
            opp.disposition_c, opp.contact_c,
            opp.system_modstamp, opp.Program_of_Interest_c,
            opp.channel_c, opp.client_user_agent_c,
            opp.event_source_url_c ,ofh.field,
            opp.transaction_id_c, opp.app_start_event_id_c,
            opp.app_submit_event_id_c, opp.fb_leadgen_id_c
        from `raw_b2c_sfdc.opportunity` opp
        join `raw_b2c_sfdc.opportunity_field_history` ofh
            on opp.id = ofh.opportunity_id
            and timestamp(datetime(ofh.created_date,"America/New_York")) >= fromdate
            and ofh.field in ( 'StageName' , 'Disposition__c')

        where opp.is_deleted=false and opp.institution_c='a0ko0000002BSH4AAO'


            union distinct

    /* pull opps that had a Application Started on fromdate */
        select
            opp.id, opp.created_date, 'Applicant' as stage_name,
            ofh_disp.old_value as disposition_c, opp.contact_c,
            opp.system_modstamp, opp.Program_of_Interest_c,
            opp.channel_c, opp.client_user_agent_c,
            opp.event_source_url_c ,ofh.field,
            opp.transaction_id_c, opp.app_start_event_id_c,
            opp.app_submit_event_id_c, opp.fb_leadgen_id_c
        from `raw_b2c_sfdc.opportunity` opp
        join `raw_b2c_sfdc.opportunity_field_history` ofh
            on opp.id = ofh.opportunity_id
            and  ofh.field = 'Application_Started__c' and ofh.new_value ='true'
        join (
                select  opportunity_id,
                        created_date,
                        field,  old_value ,
                        row_number() over(partition by opportunity_id order by created_date ) rn
                from `raw_b2c_sfdc.opportunity_field_history` ofh_disp1
                where ofh_disp1.field in ( 'Disposition__c')
             )ofh_disp
            on opp.id = ofh_disp.opportunity_id
            and rn = 1
        /* where left(string(ofh.created_date,"America/New_York"), 10) = cast(fromdate as string) */
        where timestamp(datetime(ofh.created_date,"America/New_York")) >= fromdate
        and opp.is_deleted=false and opp.institution_c='a0ko0000002BSH4AAO'


        ),


     opp_stage as (

        select  opps.id,
                ofh.created_date,
                ofh.field, new_value
        from opps
        join `raw_b2c_sfdc.opportunity_field_history` ofh
            on opps.id = ofh.opportunity_id
        where
            /* left(string(ofh.created_date,"America/New_York"), 10) <= cast(fromdate as string) */
            timestamp(datetime(ofh.created_date,"America/New_York")) <= fromdate
            and ofh.field = 'StageName'

        ),

    stage_disposition_combined as (

        select  opportunity_id, created_date, max(ofh_created_date) as ofh_created_date,
                disposition, stage, contact_c, Program_of_Interest_c, channel_c,
                client_user_agent_c, event_source_url_c ,transaction_id_c,
                app_start_event_id_c, app_submit_event_id_c, fb_leadgen_id_c
        from(
                select
                    ofh.opportunity_id,
                    opps.created_date,
                    Greatest(ifnull(ofh.created_date,opps.created_date),ifnull(opp_stage.created_date,opps.created_date)) as ofh_created_date,
                    case when ofh.field = 'Disposition__c' then ofh.new_value
                        when ofh.field = 'Application_Started__c' then opps.disposition_c
                        else null end  as disposition,
                    case when ofh.field = 'Disposition__c' then coalesce(opp_stage.new_value, opps.stage_name)
                        when ofh.field = 'Application_Started__c' then 'Applicant'
                        else null end as stage,
                    opps.contact_c,
                    opps.Program_of_Interest_c,
                    opps.channel_c,
                    opps.client_user_agent_c,
                    opps.event_source_url_c,
                    opps.field,
                    opps.transaction_id_c,
                    opps.app_start_event_id_c,
                    opps.app_submit_event_id_c,
                    opps.fb_leadgen_id_c
                from `raw_b2c_sfdc.opportunity_field_history` ofh
                join opps on opps.id = ofh.opportunity_id
                left join opp_stage on opp_stage.id =  ofh.opportunity_id
                    and opp_stage.field = 'StageName'
                where ofh.field in ( 'Disposition__c','Application_Started__c')
                    /* and left(string(ofh.created_date,"America/New_York"), 10) = cast(fromdate as string) */
                    and timestamp(datetime(ofh.created_date,"America/New_York")) >= fromdate

            )

            group by opportunity_id, created_date,  disposition, stage, contact_c,
                    Program_of_Interest_c, channel_c, client_user_agent_c, event_source_url_c ,
                    field, transaction_id_c, app_start_event_id_c, app_submit_event_id_c, fb_leadgen_id_c

        ),


    Begin_opp_stage as (

        select
            opps.id,
            ofh.created_date,
            ofh.field,
            old_value,
            row_number() over(partition by ofh.opportunity_id order by ofh.created_date ) rn
        from opps
        join `raw_b2c_sfdc.opportunity_field_history` ofh
            on opps.id = ofh.opportunity_id
        where ofh.field = 'StageName'

        ),

    Begin_opp_disposition as (

        select
            opps.id,
            ofh.created_date,
            ofh.field,
            old_value,
            row_number() over(partition by ofh.opportunity_id order by ofh.created_date ) rn
        from opps
        join `raw_b2c_sfdc.opportunity_field_history` ofh
            on opps.id = ofh.opportunity_id
        where ofh.field = 'Disposition__c'

        ),


    Begin_stage_disposition_combined as (

        select Distinct
            opps.id as opportunity_id,
            opps.created_date,
            opps.created_date as ofh_created_date,
            ifnull(Begin_opp_disposition.old_value,opps.disposition_c) as Begin_disposition,
            ifnull(Begin_opp_stage.old_value,opps.stage_name) as Begin_stage,
            opps.contact_c,
            opps.Program_of_Interest_c,
            opps.channel_c,
            opps.client_user_agent_c,
            opps.event_source_url_c,
            opps.transaction_id_c,
            opps.app_start_event_id_c,
            opps.app_submit_event_id_c,
			opps.fb_leadgen_id_c

        from opps
        left join Begin_opp_stage on Begin_opp_stage.id =  opps.id and Begin_opp_stage.rn=1
        left join Begin_opp_disposition on Begin_opp_disposition.id =  opps.id and Begin_opp_disposition.rn =1

        ) ,

    all_stage_disposition as (

        select opportunity_id, created_date, ofh_created_date,  disposition,
                -- case when disposition = 'Complete - EA Ready for Review' then 'Applicant' else stage end as stage,
                case when disposition in ('Complete - EA Ready for Review','Admissions Review in Progress') then 'Applicant' else stage end as stage,
                contact_c, Program_of_Interest_c, channel_c, client_user_agent_c,
                event_source_url_c, transaction_id_c,
                app_start_event_id_c, app_submit_event_id_c, fb_leadgen_id_c
        from stage_disposition_combined
        -- where ((disposition = 'Complete - EA Ready for Review') or (stage = 'Pre-Enroll' and disposition = 'Reserved'))
        where ((disposition in ('Complete - EA Ready for Review','Admissions Review in Progress')) or (stage = 'Pre-enroll' and disposition = 'Reserved'))
             /* and left(string(ofh_created_date,"America/New_York"), 10) = cast(fromdate as string) */
             and timestamp(datetime(ofh_created_date,"America/New_York")) >= fromdate


        union all

        /* pick both New and In-Process Dispositions for initiatecheckout */
        select * except (rownum)
        from (
                select opportunity_id, created_date, ofh_created_date, disposition,
                        stage, contact_c, Program_of_Interest_c, channel_c,
                        client_user_agent_c, event_source_url_c ,
                        transaction_id_c, app_start_event_id_c,
                        app_submit_event_id_c, fb_leadgen_id_c,
                        row_number()  over (partition by opportunity_id,
                        stage order by created_date ) as rownum
                from stage_disposition_combined
                where ( stage = 'Applicant' and disposition in ( 'New' , 'In Process','Uncontacted' ,'New - No Outreach' ) )
            )
        where rownum = 1



        union all

        /* pick only one row if state is student or closed lost. we dont need to care of disposition value. */
        select * except (rownum)
        from (
                select *,
                    row_number()  over (partition by opportunity_id, stage order by created_date, disposition) as rownum
                from stage_disposition_combined
                where stage in ('Student', 'Closed Lost')
                    /* and left(string(ofh_created_date,"America/New_York"), 10) = cast(fromdate as string)  */
                    and timestamp(datetime(ofh_created_date,"America/New_York")) >= fromdate
            )
        where rownum = 1


        union all


	    /* pick begin stage and disposition */
        select *
        from Begin_stage_disposition_combined

        ),


    contact_phone_list as (

        select
            opps.id,
            [coalesce(safe_cast(regexp_replace(preferred_phone_number_c, '[^[:digit:]]', '') as int64),null),
            coalesce(safe_cast(regexp_replace(home_phone, '[^[:digit:]]', '' ) as int64),null),
            coalesce(safe_cast(regexp_replace(mobile_phone, '[^[:digit:]]', '' ) as int64),null),
            coalesce(safe_cast(regexp_replace(other_phone, '[^[:digit:]]', '' ) as int64),null),
            coalesce(safe_cast(regexp_replace(phone, '[^[:digit:]]', '' ) as int64),null),
            coalesce(safe_cast(regexp_replace(c.work_phone_c, '[^[:digit:]]', '' ) as int64),null)] as phone_numbers_list
        from `raw_b2c_sfdc.contact` c
        join opps on c.id = opps.contact_c
        where c.is_deleted = false  and lower(c.institution_code_c)='walden'

        ),


    contact_phone_list_final as (

        select
            id,
            array_agg(distinct string ignore nulls) as phone_numbers
        from contact_phone_list, UNNEST(phone_numbers_list) string
        group by id

        ),


    contact_email_list as (

        select
            opps.id,
            [email, alternate_email_c, personal_email_c] as emails_list
        from `raw_b2c_sfdc.contact` c
        join opps on c.id = opps.contact_c
        where c.is_deleted=false and lower(c.institution_code_c)='walden'

        ),


    contact_email_list_final as (

        select
            id,
            array_agg(distinct string ignore nulls) as emails
        from contact_email_list, UNNEST(emails_list) string
        group by id

        ),
/*
    lead_score_tof as (

        select
            sf_id,
            scoring_date,
            score
        from (

                select
                    sf_id,
                    scoring_date,
                    score,
                    row_number() over (partition by sf_id order by scoring_date desc) as rownum
                from `daas-ml-prod.wu_model_cdm.lead_top_of_funnel_score_history`
                order by sf_id, scoring_date
            )
        where rownum =1

        ),


    lead_score_dlf as (

        select sf_id, scoring_date, score
        from (
            select
                sf_id,
                scoring_date,
                score,
                row_number() over (partition by sf_id order by scoring_date desc) as rownum
            from `daas-ml-prod.wu_model_cdm.lead_dynamic_lead_score_history`
            order by sf_id, scoring_date

            )
        where rownum =1

        ),

    */

    Final as (

        select
            c.first_name,
            c.last_name,
            contact_phone_list_final.phone_numbers as phone,
            contact_email_list_final.emails as email,
            c.mailing_city as city,
            c.mailing_state as state,
            cast(c.mailing_postal_code as string) as zip,
            extract(year from birthdate) as dob_year,
            extract(month from birthdate) as dob_month,
            extract(day from birthdate) as dob_day,
            c.mailing_country as country,
            case
                when all_stage_disposition.stage = 'Closed Lost' then 'ClosedLost'
                when all_stage_disposition.stage = 'Student' then 'Student'
                /* when c.is_walden_alum_c then 'Alumni' */
                when   all_stage_disposition.stage = 'Applicant' and all_stage_disposition.disposition = 'New' then 'InitiateCheckout'
                when all_stage_disposition.stage = 'Applicant' and all_stage_disposition.disposition = 'In Process' then 'InitiateCheckout'
                when   all_stage_disposition.stage = 'Applicant' and all_stage_disposition.disposition = 'Uncontacted' then 'InitiateCheckout'
                when all_stage_disposition.stage = 'Applicant' and all_stage_disposition.disposition = 'New - No Outreach' then 'InitiateCheckout'
                when all_stage_disposition.stage = 'Applicant' and all_stage_disposition.disposition = 'Complete - EA Ready for Review' then 'SubmitApplication'
                when all_stage_disposition.stage = 'Applicant' and all_stage_disposition.disposition = 'Admissions Review in Progress' then 'Schedule'
                -- when all_stage_disposition.stage = 'Pre-Enroll' and all_stage_disposition.disposition = 'Reserved' then 'CompleteRegistration'
                when all_stage_disposition.stage = 'Pre-enroll' and all_stage_disposition.disposition = 'Reserved' then 'Subscribe'
                end as event_name,
            all_stage_disposition.channel_c as event_source,
            p.name as program_name,

            /* event_id should be unique. It is used for matching on facebook side */
            case
                when all_stage_disposition.fb_leadgen_id_c is not null then null -- pass null event_id for fb AD generated ones
                when all_stage_disposition.stage = 'Closed Lost' then concat(all_stage_disposition.opportunity_id, 'CL')
                when all_stage_disposition.stage = 'Student' then concat(all_stage_disposition.opportunity_id, 'S')
                when all_stage_disposition.stage = 'Applicant' then CASE WHEN all_stage_disposition.disposition = 'New' then COALESCE(all_stage_disposition.app_start_event_id_c, concat(all_stage_disposition.opportunity_id, 'AN'))
                                                                        when all_stage_disposition.disposition = 'In Process' then COALESCE(all_stage_disposition.app_start_event_id_c, concat(all_stage_disposition.opportunity_id, 'AI'))
                                                                        WHEN all_stage_disposition.disposition = 'Uncontacted' then COALESCE(all_stage_disposition.app_start_event_id_c, concat(all_stage_disposition.opportunity_id, 'AU'))
                                                                        when all_stage_disposition.disposition = 'New - No Outreach' then COALESCE(all_stage_disposition.app_start_event_id_c, concat(all_stage_disposition.opportunity_id, 'ANN'))
                                                                        when all_stage_disposition.disposition = 'Complete - EA Ready for Review' then COALESCE(all_stage_disposition.app_submit_event_id_c , concat(all_stage_disposition.opportunity_id, 'AR'))
                                                                        when all_stage_disposition.disposition = 'Admissions Review in Progress' then COALESCE(all_stage_disposition.app_submit_event_id_c , concat(all_stage_disposition.opportunity_id, 'AP'))
                                                                        END
                when all_stage_disposition.stage = 'Pre-enroll' and all_stage_disposition.disposition = 'Reserved' then concat(all_stage_disposition.opportunity_id, 'PR')
            end as event_id,

            case when all_stage_disposition.stage = 'Closed Lost' or  all_stage_disposition.stage = 'Student'
                or (all_stage_disposition.stage = 'Applicant' and all_stage_disposition.disposition in ('New' , 'In Process','Uncontacted' ,'New - No Outreach'))
                or (all_stage_disposition.stage = 'Applicant' and all_stage_disposition.disposition = 'Complete - EA Ready for Review')
                or (all_stage_disposition.stage = 'Applicant' and all_stage_disposition.disposition = 'Admissions Review in Progress')
                or (all_stage_disposition.stage = 'Pre-enroll' and all_stage_disposition.disposition = 'Reserved')
                    then unix_seconds(all_stage_disposition.ofh_created_date)
                else unix_seconds(all_stage_disposition.created_date)
                end as event_time,

            0 as lead_score_tof,
            0 as lead_score_dynamic,
            case when all_stage_disposition.stage = 'Student' then 'Student'
                when all_stage_disposition.stage = 'Closed Lost' then 'Closed Lost'
                /* when c.is_walden_alum_c then 'Alumni' */
                else concat(all_stage_disposition.stage, '-', all_stage_disposition.disposition)
            end crm_stage_disposition,

            c.is_walden_alum_c as is_walden_alum,
            all_stage_disposition.client_user_agent_c as client_user_agent,
            case when all_stage_disposition.fb_leadgen_id_c is not null then 'system_generated'
            when all_stage_disposition.client_user_agent_c is not null then 'Website' else 'Other' end as action_source,
            all_stage_disposition.event_source_url_c as event_source_url,

            current_timestamp() as etl_created_date,
        /* ------------for manually inserting (Testing) ------------------------            '22f51867-f40b-4d95-b4f2-3fdb3ac9930c' as etl_ins_audit_key ,  -- */

            AUDITKEY as etl_ins_audit_key,
            'cloud_function: fn_wal_to_facebook_lead_events' as etl_resource_name,
            all_stage_disposition.fb_leadgen_id_c,
            all_stage_disposition.opportunity_id

        from all_stage_disposition
        join `raw_b2c_sfdc.contact` c on all_stage_disposition.contact_c = c.id and c.is_deleted = false

        join `raw_b2c_sfdc.product_2` p on all_stage_disposition.Program_of_Interest_c = p.id and c.is_deleted = false

     /*   left join lead_score_tof ToF
            on all_stage_disposition.opportunity_id = ToF.sf_id

        left join lead_score_dlf dls
            on all_stage_disposition.opportunity_id = dls.sf_id
*/
        left join contact_phone_list_final
            on all_stage_disposition.opportunity_id = contact_phone_list_final.id

        left join contact_email_list_final
            on all_stage_disposition.opportunity_id = contact_email_list_final.id

        /* where left(string(ifnull(all_stage_disposition.ofh_created_date,all_stage_disposition.created_date),"America/New_York"), 10) = cast(fromdate as string) */
        where timestamp(datetime(ifnull(all_stage_disposition.ofh_created_date,all_stage_disposition.created_date),"America/New_York")) >= fromdate

    union all

	/* only events created currentdate - 2 and have below events in addition to lead at the same time */

        select
            c.first_name,
            c.last_name,
            contact_phone_list_final.phone_numbers as phone,
            contact_email_list_final.emails as email,
            c.mailing_city as city,
            c.mailing_state as state,
            cast(c.mailing_postal_code as string) as zip,
            extract(year from birthdate) as dob_year,
            extract(month from birthdate) as dob_month,
            extract(day from birthdate) as dob_day,
            c.mailing_country as country,
            'Lead' as event_name,
            all_stage_disposition.channel_c as event_source,
            p.name as program_name,

            /* event_id should be unique. It is used for matching on facebook side */
            case when all_stage_disposition.fb_leadgen_id_c is not null then null -- pass null event_id for fb AD generated ones
		    else COALESCE(all_stage_disposition.transaction_id_c, concat(all_stage_disposition.opportunity_id,'L')) end as event_id,
            unix_seconds(all_stage_disposition.created_date) as event_time,
            0 as lead_score_tof,
            0 as lead_score_dynamic,
            case when all_stage_disposition.Begin_stage = 'Student' then 'Student'
            when all_stage_disposition.Begin_stage = 'Closed Lost' then 'Closed Lost'
            /* when c.is_walden_alum_c then 'Alumni' */
            else concat(all_stage_disposition.Begin_stage, '-', all_stage_disposition.Begin_disposition)
            end crm_stage_disposition,
            c.is_walden_alum_c as is_walden_alum,
            all_stage_disposition.client_user_agent_c as client_user_agent,
            case when all_stage_disposition.fb_leadgen_id_c is not null then 'system_generated'
            when all_stage_disposition.client_user_agent_c is not null then 'Website' else 'Other' end as action_source,
            all_stage_disposition.event_source_url_c as event_source_url,

            current_timestamp() as etl_created_date,

    /* ----------------------------------Test Audit -----------------------------            '22f51867-f40b-4d95-b4f2-3fdb3ac9930c' as etl_ins_audit_key ,  --  */

            AUDITKEY as etl_ins_audit_key,
            'cloud_function: fn_wal_to_facebook_lead_events' as etl_resource_name,
            all_stage_disposition.fb_leadgen_id_c,
            all_stage_disposition.opportunity_id

        from Begin_stage_disposition_combined  all_stage_disposition
        join `raw_b2c_sfdc.contact` c
            on all_stage_disposition.contact_c = c.id

        join `raw_b2c_sfdc.product_2` p
            on all_stage_disposition.Program_of_Interest_c = p.id

     /*   left join lead_score_tof ToF
            on all_stage_disposition.opportunity_id = ToF.sf_id

        left join lead_score_dlf dls
            on all_stage_disposition.opportunity_id = dls.sf_id
*/
        left join contact_phone_list_final
            on all_stage_disposition.opportunity_id = contact_phone_list_final.id

        left join contact_email_list_final
            on all_stage_disposition.opportunity_id = contact_email_list_final.id

        /*where left(string(all_stage_disposition.created_date,"America/New_York"), 10) = cast(fromdate as string) */
        where timestamp(datetime(all_stage_disposition.created_date,"America/New_York")) >= fromdate

       )


        select f.* except (rn)
        from (
                select * from (
                    select row_number()  over (partition by opportunity_id, event_name order by event_time) as rn ,
                            *
                    from Final
                    where event_name is not null
                    /* where left(event_id,18) = '0062G00000mBz69QAC' --'0062G00000mBz69QAC%' */
                )
        where rn = 1  /* Using rn to remove Applicant-New and Applicant-Inprocess Duplicate */
        )f

       /************ left join to ensure only records which were not sent successfully previously are sent this time**********/

        left join `trans_bi_offline_conversion.wal_to_facebook_lead_events` wfl
            on ifnull(left(wfl.event_id,18), wfl.fb_leadgen_id_c) = ifnull(left(f.event_id,18),f.fb_leadgen_id_c) /* send one oppid-(eventname/fb_leadgenid) only once. */
        and wfl.event_name = f.event_name
        and wfl.api_response like "{'events_received': 1%" and wfl.api_response like "%'messages': []%" /* ignore only successfully posted eventids */
        where ifnull(wfl.event_id,wfl.fb_leadgen_id_c) is null; /* only record/send the first occurance of the event for an opportunity */


        insert into
        `audit_cdw_log.audit_load_details` (audit_load_key,
            job_name,
            job_start_dt,
            job_end_dt,
            job_completed_ind,
            job_type,
            load_method)
        values
        (AUDITKEY,'LEADS_TO_FACEBOOK',v_startdate, current_timestamp(),'COMPLETED','send_lead_events_to_facebook_conversion_api','sp_wal_to_facebook_lead_events');


        set RESULT = 'SUCCESS';

        EXCEPTION when ERROR then


        insert into
        `audit_cdw_log.audit_load_details` (audit_load_key,
            job_name,
            job_start_dt,
            job_end_dt,
            job_completed_ind,
            job_type,
            load_method)
        values
        (AUDITKEY,'LEADS_TO_FACEBOOK',(select job_start_dt from `audit_cdw_log.audit_load_details`
        where audit_load_key = AUDITKEY and job_name = "LEADS_TO_FACEBOOK"
        and job_type = "send_lead_events_to_facebook_conversion_api" and load_method = "sp_wal_to_facebook_lead_events" limit 1), current_timestamp(),'FAILED','send_lead_events_to_facebook_conversion_api','sp_wal_to_facebook_lead_events');


        set RESULT = @@error.message;


end
