begin

    declare institution string default 'WLDN';
    declare institution_id int64 default 5;
    declare dml_mode string default 'delete-insert';
    declare target_dataset string default 'rpt_crm_mart';
    declare target_tablename string default 't_atge_webinar_data';
    declare source_tablename string default 'webinar_data';
    declare load_source string default 'trans_crm_mart.sp_atge_webinar_data';
    declare additional_attributes ARRAY<struct<keyword string, value string>>;
    declare last_refresh_time timestamp;
    declare tgt_table_count int64;

    /* common across */
    declare job_start_dt timestamp default current_timestamp();
    declare job_end_dt timestamp default current_timestamp();
    declare job_completed_ind string default null;
    declare job_type string default 'DS';
    declare load_method string default 'scheduled query';
    declare out_sql string;
    BEGIN
    SET additional_attributes= [("audit_load_key", v_audit_key),
              ("load_method",load_method),
              ("load_source",load_source),
              ("job_type", job_type)];
    /* end common across */

---Start of Table---

create temp table  webinar_data_link as(
with Webinar_ID_Link2 as(
SELECT
  DISTINCT
  a.institution,
  a.institution_id,
  a.registrant_firstname AS First_Name,
  a.registrant_lastname AS Last_Name,
  a.registrant_email AS Email,
  case when a.registrant_email like 'system%' or a.registrant_email like 'test%' then 1 else 0 end as system_email_flag,
  a.registrant_homephone AS Phone,
  a.description AS Webinar_Name,
  EXTRACT(date FROM (DATETIME(a.goodafter, 'US/Eastern'))) AS Webinar_Date,
  DATETIME(a.goodafter, 'US/Eastern') AS Webinar_DateTime,
  DATETIME(a.registrant_timestamp, 'US/Eastern') AS Registered_DtTime,
  a.eventid AS EventID,
  CASE WHEN a.registrant_sourceeventid IS NOT NULL THEN 1 ELSE 0 END AS Registered,
  CASE WHEN a.attendee_eventid IS NOT NULL THEN 1 ELSE 0 END AS Attended,
  a.category,
  a.application,
  a.BannerID,
  a.contactid,
  a.partnerref as Partner_Ref_Location_Code,
  c.Brand_profile_c AS BrandprofileSFID,
  c.Id AS Oppsfid, ---Primary Opp Info
  c.created_date,
  c.Stage_Name AS Current_Stage,
  c.Disposition_c AS Current_Disposition,
  s.current_stage_disposition_timestamp as Current_StageDispDate,
  c.start_date_c,
  utility.udf_convert_UTC_to_EST(c.Last_EA_Two_Way_Contact_C) as last_ea_two_way_contact_c,
  utility.udf_convert_UTC_to_EST(c.Last_EA_Outreach_Attempt_C) as last_ea_outreach_attempt_c,
  c.primary_flag_c as PrimaryFlag,
  s.es_curr_name,
  s.es_curr_manager_name,
  s.curr_program_name,
  s.curr_college_code,
  s.curr_level,
  s.is_tempo_flag,
  s.state,
  s.Country_Name,
  s.international_flag,
  s.Channel,
  s.created_date_est as CreatedDate,
  datetime(s.first_ea_contact,'US/Eastern') as First_TwoWayContact_Opp,
  o.Min_Inq_Date,
  o.Max_Inq_Date,
  r.Latest_Reconciled_Start,
  case when r.Latest_Reconciled_Start is not NULL then 1 else 0 end as Reconciled,
  g.First_Graduation_Date,
  g.Alumni,
  CASE WHEN a.registrant_email LIKE '@waldenu.edu' THEN 1 ELSE 0 END AS Registered_Student_Email,
  CASE WHEN a.registrant_email LIKE '@mail.waldenu.edu' OR a.registrant_email LIKE '@adtalem.com' THEN 1 ELSE 0 END AS Employee,

  ---Event details added
  prc_atten.liveminutes as Live_Minutes,
  prc_atten.liveviewed as LiveViewed,
  case when prc_atten.liveviewed>=1 then extract(date FROM (datetime(a.goodafter, 'US/Eastern')))
  when prc_atten.liveviewed=0 and prc_atten.archiveviewed>=1 then extract(date FROM (datetime(prc_atten.firstarchiveactivity, 'US/Eastern')))
  else extract(date FROM (datetime(a.goodafter, 'US/Eastern'))) end as viewed_date,
  case when prc_atten.liveviewed>=1 then 'Y' else 'N' end as Attended_Live, --********
  case when prc_atten.liveviewed>=1 or prc_atten.archiveviewed>=1 then 'Y' else 'N' end as Attended_Live_and_or_Archive,  --********
  prc_atten.archiveminutes as ArchivedMinutes,
  prc_atten.archiveviewed as ArchivedViewed,
  datetime(prc_atten.firstarchiveactivity, 'US/Eastern') as FirstArchiveActivity,
  datetime(prc_atten.lastarchiveactivity, 'US/Eastern') as LastArchiveActivity,
  prc_atten.liveminutes + prc_atten.archiveminutes as Total_Minutes,

  --a.Vertical,a.Category,


  b.current_student_status as Current_Student_Status_Cd,
  b.current_start_date as Current_Student_Start_Date,
  cast(sd.overall_rank as NUMERIC) as OverallRank

FROM `stg_webinar_on24.uvw_rpt_event_details` a ----Current PRIMARY Opp DATA

left join `rpt_academics.v_student_current_status` b
on a.bannerid=b.credential_id

LEFT JOIN `raw_b2c_sfdc.opportunity` c
ON a.ContactID=c.contact_id AND a.contactid IS NOT NULL
--AND c.primary_flag_c=TRUE
and institution_c='a0ko0000002BSH4AAO'
and (c.closed_lost_reason_2_c<>'Duplicate' or c.closed_lost_reason_2_c is null)
--***** JOIN ON  SNAPSHOT causing errors – need TO check*****
LEFT JOIN `rpt_crm_mart.t_wldn_opp_snapshot` s
ON c.id=s.opp_sfid AND c.id IS NOT NULL

---First  AND Most Recent Opportunities created
LEFT JOIN
(select distinct a.contactid,
	min (extract(date FROM (datetime(c.Created_date, 'US/Eastern')))) as Min_Inq_Date,
	max(extract(date FROM (datetime(c.Created_date, 'US/Eastern'))))  as Max_Inq_Date
from `stg_webinar_on24.uvw_rpt_event_details` a
left join `rpt_crm_mart.t_wldn_opp_snapshot` c on a.ContactID=c.contact_sfid and a.contactid is not null
where a.contactid is not null
group by  a.contactid) o
on a.contactid=o.contactid and a.contactid is not null

------Reconciled Flag
LEFT JOIN (
SELECT DISTINCT a.Bannerid,
    MAX(c.start_date) AS Latest_Reconciled_Start
FROM `stg_webinar_on24.uvw_rpt_event_details` a
LEFT JOIN `rpt_academics.t_wldn_reconciled_list_cube` c
ON a.bannerid=c.student_id AND a.bannerid IS NOT NULL
WHERE a.bannerid IS NOT NULL
GROUP BY a.Bannerid) r
ON a.Bannerid=r.Bannerid

-----Alumni Flag
LEFT JOIN (
SELECT DISTINCT a.contactid, a.Bannerid, c.graduation_flag1 AS Alumni,
    MIN(c.outcome_graduation_date1) AS First_Graduation_Date
FROM `stg_webinar_on24.uvw_rpt_event_details` a
LEFT JOIN `rpt_academics.t_wldn_alumni` c
ON a.bannerid=c.id AND a.bannerid IS NOT NULL AND c.graduation_flag1=1 AND c.cert_flag1=0
WHERE a.bannerid IS NOT NULL
GROUP BY a.contactid, a.Bannerid, c.graduation_flag1) g
ON  a.Bannerid=g.Bannerid

-----uvw_prc_attendees details
left join stg_webinar_on24.uvw_prc_attendees prc_atten on a.registrant_eventuserid=prc_atten.eventuserid

----- taking the RANK
left join raw_wldn_manualfiles.manual_opp_stage_disp_ranking sd
on c.Stage_Name=sd.stage_name and c.Disposition_c=sd.Disposition and sd.institution_id=5
)
select wb.*except(institution,institution_id),
case when webinar_name like '%Chamberlain%' then 'CU' else institution end AS institution,
'WLDN_SF' as source_system_name,
case when webinar_name like '%Chamberlain%' then 2 else institution_id end as institution_id,
wb.viewed_date+7 as Date_Day7,
wb.viewed_date+14 as Date_Day14,
wb.viewed_date+30 as Date_Day30,
wb.viewed_date+90 as Date_Day90,
from Webinar_ID_Link2 wb
where system_email_flag=0
);

create temp table Webinar_AllOpps as (
select distinct
a.First_Name,
a.Last_Name,
a.Email,
a.Phone,
a.Webinar_Name,
a.Webinar_Date,
a.viewed_date,
a.Webinar_DateTime,
a.EventID,
--a.Vertical,a.Category,
a.Date_Day7,a.Date_Day14,a.Date_Day30,a.Date_Day90,
a.contactid,
a.BannerID,
a.BrandprofileSFID,
Current_Student_Status_Cd,
Current_Student_Start_Date,
a.Oppsfid,
a.PrimaryFlag,
a.CreatedDate,
a.Current_Stage,
a.Current_Disposition,
a.OverallRank,
a.First_TwoWayContact_Opp,
a.last_ea_two_way_contact_c,
case when a.last_ea_two_way_contact_c is not null or twct.seq=1 then 1 else 0 end as Contacted_Flag
from webinar_data_link a
/**First Two Way Contact on Opp**/
left join rpt_crm_mart.v_two_way_contact_tasks twct on a.Oppsfid=twct.oppsfid and twct.seq=1
/**Latestt Two Way Contact on Opp**/
--left join stg_l1_salesforce.opportunity opp on a.oppsfid=opp.id
where a.oppsfid is not null
);

create temp table Webinar_HighestRankedOpp as (
select *except(rid) from (select *,row_number() over(partition by BrandprofileSFID,bannerid order by OverallRank desc,  PrimaryFlag desc, CreatedDate desc) as rid
 from Webinar_AllOpps)
 where rid=1
);

create temp table Webinar_PrimaryOpp as (
select * from Webinar_AllOpps
where PrimaryFlag=true
);

create temp table Webinar_Prim_HighestOpp as (
select distinct a.First_Name,a.Last_Name,a.Email,a.Phone,
a.Webinar_Name,a.Webinar_Date,a.viewed_date,a.Webinar_DateTime,a.EventID,
--a.Vertical,a.Category,
a.Date_Day7,a.Date_Day14,a.Date_Day30,a.Date_Day90,
a.ContactID,a.BannerID,a.BrandprofileSFID,
a.Current_Student_Status_Cd,a.Current_Student_Start_Date,
/**Use Primary Opportunity for Current Status

- UNLESS Banner Status=Active Student AND Primary Opp=Closed Lost
then use Opportunity with higest current stage**/
case when a.Current_Student_status_CD='AS' and a.Current_Stage='Closed Lost' then c.Oppsfid else a.Oppsfid end as Oppsfid,
case when a.Current_Student_status_CD='AS' and a.Current_Stage='Closed Lost' then c.PrimaryFlag else a.PrimaryFlag end as PrimaryFlag,
case when a.Current_Student_status_CD='AS' and a.Current_Stage='Closed Lost' then c.OverallRank else a.OverallRank end as OverallRank,
case when a.Current_Student_status_CD='AS' and a.Current_Stage='Closed Lost' then c.Current_Stage else a.Current_Stage end as Current_Stage,
case when a.Current_Student_status_CD='AS' and a.Current_Stage='Closed Lost' then c.Current_Disposition else a.Current_Disposition end as Current_Disposition,
case when a.Current_Student_status_CD='AS' and a.Current_Stage='Closed Lost' then c.CreatedDate else a.CreatedDate end as CreatedDate,

case when a.Current_Student_status_CD='AS' and a.Current_Stage='Closed Lost' then c.First_TwoWayContact_Opp else a.First_TwoWayContact_Opp end as First_TwoWayContact_Opp,
case when a.Current_Student_status_CD='AS' and a.Current_Stage='Closed Lost' then c.Last_EA_Two_Way_Contact_C else a.Last_EA_Two_Way_Contact_C end as Last_EA_Two_Way_Contact_C,
case when a.Current_Student_status_CD='AS' and a.Current_Stage='Closed Lost' then c.Contacted_Flag else a.Contacted_Flag end as Contacted_Flag
from  Webinar_PrimaryOpp a
left join Webinar_HighestRankedOpp c on a.BrandprofileSFID=c.BrandprofileSFID and a.BannerId=c.BannerID
);

create temp table Webinar_PrimaryOpp_Data as (
select distinct a.First_Name,a.Last_Name,a.Email,a.Phone,
a.Webinar_Name,a.Webinar_Date,a.viewed_date,a.Webinar_DateTime,a.EventID,--a.Vertical,a.Category,
a.Date_Day7,a.Date_Day14,a.Date_Day30,a.Date_Day90,
a.ContactID,a.BannerID,a.BrandprofileSFID,
a.Current_Student_Status_Cd,a.Current_Student_Start_Date,
a.Oppsfid,a.PrimaryFlag,a.OverallRank,a.Current_Stage,a.Current_Disposition,
case when a.CreatedDate<=a.viewed_date and a.CreatedDate is not null
then 1 else 0 end as Opp_Before_Webinar,
case when a.CreatedDate>a.viewed_date and a.CreatedDate is not null
then 1 else 0 end as Opp_CreatedAfter_Webinar,
EXTRACT(Date from b.current_stage_disposition_timestamp) as Current_StageDispDate,
a.First_TwoWayContact_Opp,a.Last_EA_Two_Way_Contact_C,a.Contacted_Flag,
b.selected_program_start_date,b.inq_date,
b.date_app,b.date_appcomplete,
b.date_admitted,b.date_rs,b.enr_date,date_closed_lost,
b.curr_program_name as program_name,b.curr_college_code as college_name,b.curr_level as level,b.is_tempo_flag,b.channel,b.state,b.country_name,b.international_flag,
b.es_curr_name as ea_name_curr,b.es_curr_manager_name as ea_manager_curr,b.es_curr_site as enrollment_site_curr
from Webinar_Prim_HighestOpp a
left join `rpt_crm_mart.t_wldn_opp_snapshot` b on a.Oppsfid=b.Opp_sfid and a.oppsfid is not null
);

create temp table Opp_History_update as (
with Opp_History as (
select distinct  a.Oppsfid,a.PrimaryFlag,a.CreatedDate,
a.ContactID,a.BannerID,a.BrandprofileSFID,a.Webinar_Date,--a.Vertical,a.Category,
a.Date_Day7,a.Date_Day14,a.Date_Day30,a.Date_Day90,
b.stage_name,b.disposition,
b.status_date,b.status_time,
--COALESCE(c.overall_rank,c2.stagerank)as Rank,
cast(c.overall_rank as NUMERIC) as Rank,
case when Webinar_Date>=status_date then 1 else 0 end as Status_before_Webinar,
case when Date_Day7>=status_date then 1
when Date_Day7>= current_date() then 0 else null end as Status_before_Day7,
case when Date_Day14>=status_date then 1
when Date_Day14>= current_date() then 0 else null end as Status_before_Day14,
case when Date_Day30>=status_date then 1
when Date_Day30>= current_date() then 0 else null end as Status_before_Day30,
case when Date_Day90>=status_date then 1
when Date_Day90>= current_date() then 0 else null end as Status_before_Day90,
from Webinar_AllOpps a
left join rpt_crm_mart.t_opp_status_history b on a.Oppsfid=b.opp_sfid
left join raw_wldn_manualfiles.manual_opp_stage_disp_ranking c
on b.stage_name=c.stage_name and b.disposition=c.Disposition and c.institution_id=5
--left join raw_wldn_manualfiles.manual_opp_stage_disp_ranking c2
--on b.stage_name=c2.stage_name and c2.institution_id=5
)
select * from Opp_History
);

create temp table History_prior_Webinar as (
select * from Opp_History_update
where Status_before_Webinar=1
);

create temp table Ind_Latest_Status_before_Webinar as (
with Opps_Status_before_Webinar as (
select *except(rid) from (select *,row_number() over(partition by oppsfid,Webinar_Date order by status_date desc, status_time desc) as rid
 from History_prior_Webinar)
 where rid=1
)
select *except(rid) from (select *,row_number() over(partition by BrandprofileSFID,BannerID,Webinar_Date order by Rank desc, status_date desc, PrimaryFlag desc) as rid
 from Opps_Status_before_Webinar)
 where rid=1
);

create temp table Ind_StageDay7 as (
with History_prior_Day7 as (
select *except(rid) from (select *,row_number() over(partition by Oppsfid,Webinar_Date order by status_date desc, status_time desc, Rank desc) as rid
from Opp_History_update
where Status_before_Day7=1)
where rid=1
)
select * from (select *,row_number() over(partition by BrandprofileSFID,BannerID,Webinar_Date order by Rank desc, status_date desc, PrimaryFlag desc) as rid
 from History_prior_Day7)
 where rid=1
);

create temp table Ind_StageDay14 as (
with History_prior_Day14 as (
select *except(rid) from (select *,row_number() over(partition by Oppsfid,Webinar_Date order by status_date desc, status_time desc, Rank desc) as rid
from Opp_History_update
where Status_before_Day14=1)
where rid=1
)

select * from (select *,row_number() over(partition by BrandprofileSFID,BannerID,Webinar_Date order by Rank desc, status_date desc, PrimaryFlag desc) as rid
 from History_prior_Day14)
 where rid=1
);

create temp table Ind_StageDay30 as (
with History_prior_Day30 as (
select *except(rid) from (select *,row_number() over(partition by Oppsfid,Webinar_Date order by status_date desc, status_time desc, Rank desc) as rid
from Opp_History_update
where Status_before_Day30=1)
where rid=1
)
select * from (select *,row_number() over(partition by BrandprofileSFID,BannerID,Webinar_Date order by Rank desc, status_date desc, PrimaryFlag desc) as rid
 from History_prior_Day30)
 where rid=1
);

create temp table Ind_StageDay90 as (
with History_prior_Day90 as (
select *except(rid) from (select *,row_number() over(partition by Oppsfid,Webinar_Date order by status_date desc, status_time desc, Rank desc) as rid
from Opp_History_update
where Status_before_Day90=1)
where rid=1
)
select * from (select *,row_number() over(partition by BrandprofileSFID,BannerID,Webinar_Date order by Rank desc, status_date desc, PrimaryFlag desc) as rid
 from History_prior_Day90)
 where rid=1
);

create temp table webinar_data as
with src as(
select distinct i.* except(system_email_flag),
a.Opp_Before_Webinar,a.Opp_CreatedAfter_Webinar,
b.Oppsfid as Previous_Oppsfid,b.CreatedDate as Prev_Opp_Create_Dt,
case when b.stage_name='Paused' then 'Pause' else b.stage_name end as Previous_Stage,
b.Disposition as Previous_Disposition,
b.Rank as Previous_Rank,b.status_date as Previous_Status_Date,

case when i.oppsfid=b.oppsfid and i.oppsfid is not null and b.oppsfid is not null then 1 else 0 end as Same_Oppsfid,

a.Contacted_Flag,
case when a.First_TwoWayContact_Opp is not NULL and DATE(a.First_TwoWayContact_Opp)<=i.Webinar_Date then 1 else 0 end as FirstContacted_PriorWebinar,
case when a.First_TwoWayContact_Opp is not NULL and DATE(a.First_TwoWayContact_Opp)>i.Webinar_Date then 1 else 0 end as FirstContacted_AfterWebinar,

a.selected_program_start_date,a.inq_date,a.enrollment_site_curr,

case when i.Latest_Reconciled_Start is not NULL and i.Latest_Reconciled_Start>=i.Webinar_Date then 1 else 0 end as ReconciledStart_AfterWebinar,

/**Stage/Disp As of Day 7 After Webinar*/

case when c.stage_name='Paused' then 'Pause' else c.stage_name end as Stage_Day7,
c.Disposition as Disposition_Day7,
c.Rank as Rank_Day7,
/*c.status_date as Status_Date_Day7,*/

/**Stage/Disp As of Day 14 After Webinar*/
case when d.stage_name='Paused' then 'Pause' else d.stage_name end as Stage_Day14,
d.Disposition as Disposition_Day14,
d.Rank as Rank_Day14,
/*d.status_date as Status_Date_Day14,*/

/**Stage/Disp As of Day 30 After Webinar*/
case when e.stage_name='Paused' then 'Pause' else e.stage_name end as Stage_Day30,
e.Disposition as Disposition_Day30,
e.Rank as Rank_Day30,

/**Stage/Disp As of Day 90 After Webinar*/
case when f.stage_name='Paused' then 'Pause' else f.stage_name end as Stage_Day90,
f.Disposition as Disposition_Day90,
f.Rank as Rank_Day90,


case when i.First_Graduation_Date is not null and i.First_Graduation_Date<=i.Webinar_Date then 1 else 0 end as Alumni_atWebinar,

case when i.email like '%@mail.waldenu.edu' then 1 else 0 end as Walden_Employee,
current_date() as Date_Date
,row_number() over(partition by i.email,i.eventid order by i.PrimaryFlag desc) as rid
 from webinar_data_link i
/*Primary Opp - Current Stage/Disp*/
left join Webinar_PrimaryOpp_Data a
on a.BrandprofileSFID=i.BrandprofileSFID and a.Webinar_Date=i.Webinar_Date
/*Stage/Disp as of Webinar Date */
left join Ind_Latest_Status_before_Webinar b on i.BrandprofileSFID=b.BrandprofileSFID and i.Webinar_Date=b.Webinar_Date
/*Stage/Disp as of 7,14,28,60 days after webinar**/
left join Ind_StageDay7 c on i.BrandprofileSFID=c.BrandprofileSFID and i.Webinar_Date=c.Webinar_Date
left join Ind_StageDay14 d on i.BrandprofileSFID=d.BrandprofileSFID and i.Webinar_Date=d.Webinar_Date
left join Ind_StageDay30 e on i.BrandprofileSFID=e.BrandprofileSFID and i.Webinar_Date=e.Webinar_Date
left join Ind_StageDay90 f on i.BrandprofileSFID=f.BrandprofileSFID and i.Webinar_Date=f.Webinar_Date
--where institution_id=5
)

---End of Table---
SELECT
    src.*except(rid),
    job_start_dt as etl_created_date,
    job_start_dt as etl_updated_date,
    load_source as etl_resource_name,
    v_audit_key as etl_ins_audit_key,
    v_audit_key as etl_upd_audit_key,
    farm_fingerprint(format('%T', concat(src.oppsfid))) AS etl_pk_hash,
    farm_fingerprint(format('%T', src )) as etl_chg_hash,
    FROM src
	where rid=1
	;
-- merge process
CALL utility.sp_process_elt (institution, dml_mode , target_dataset, target_tablename, null, source_tablename, additional_attributes, out_sql );
SET job_end_dt = current_timestamp();
SET job_completed_ind = 'Y';
-- export success audit log record
CALL `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);
SET result = 'SUCCESS';
EXCEPTION WHEN error THEN
SET job_end_dt = cast (NULL as TIMESTAMP);
SET job_completed_ind = 'N';
CALL `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);
-- insert into error_log table
INSERT INTO
`audit_cdw_log.error_log` (error_load_key, process_name, table_name, error_details, etl_create_date, etl_resource_name, etl_ins_audit_key)
VALUES
(v_audit_key,'DS_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;
SET result =  @@error.message;
raise using message = @@error.message;
END;
END