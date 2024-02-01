create or replace view rpt_crm_mart.v_wldn_sto_speedoutreach as
With Outreach As
(
Select distinct
ASF.active_engaged_timestamp,
ASF.active_interest_verified_timestamp,
ASF.applicant_admissions_review_in_progress_timestamp,
ASF.applicant_admitted_timestamp,
ASF.applicant_in_process_timestamp,
ASF.application_submitted_flag,
ASF.applicant_new_timestamp,
ASF.closed_lost_timestamp, --ClosedTimestamp
 datetime(o.Created_Date,"America/New_York") as CreateDateTimestamp,
ASF.current_stage_disposition_timestamp,
ASF.discussed_start_date,
--ASF.EffectiveDt,
ASF.inactive_flag as inactiveflag,
ASF.intended_start_date as IntendedStartDateRFI,
ASF.begin_stage_disposition_timestamp,
cast(ASF.created_date_est as timestamp) as created_date, --ASF.Createddatedimkey,
safe_cast (ASF.intended_start_date as date) AS IntendedStartDate,
ASF.open_uncontacted_timestamp,
ASF.opp_sfid as oppsfid,
ASF.preenroll_registered_timestamp,
ASF.preenroll_reserved_timestamp,
ASF.qualified_committed_timestamp,
ASF.qualified_documents_pending_timestamp,
ASF.is_deleted, --ASF.DeletedInSf,
ASF.international_flag ASF_InternationalFlag,
ASF.applicant_complete_ready_for_review_timestamp,
ASF.student_active_flag,
ASF.withdrawn_flag,
cast(ASF.first_contacted_date as timestamp) as FirstContactedDate,

C.Activity_Id_c as ActivityId,
C.Channel_c as ChannelEDW,
C.Name AS ContactName,
C.Supplier_Id_c as SupplierId,
C.Status,
A.name as SupplierName,

'Walden University(WAL)'as Institution_Name,
5 as institution_id, --3 as Institution_DimKey,

BP.banner_id_c as BannerId,
BP.id as BrandProfileSFId,
BP.first_name_c as Firstname,
BP.last_name_c as Lastname,
BP.full_name_c as Name,
BP.shipping_country_text_c as ShippingCountryText,
BP.shipping_state_text_c as ShippingStateText,
BP.billing_country_text_c as BillingCountryText,
BP.billing_state_text_c as BillingStateText,

regexp_replace(P.full_banner_program_code_c, '_1', '') as BPCLeg,
P.degree_level_c as DegreeLevel,
CAST((Case p.type_c when 'Doctorate' Then 'PHD'
When 'Master' then 'MS'
When 'Masters' Then 'MS'
When 'Bachelor' Then 'BS'
When 'Bachelors' Then 'BS'
When 'Individual Course' Then 'NON'
When 'Non-Degree' Then 'NON'
When 'Certificate' Then  'CERT'
Else 'UDE' END) As string) As LevelDescription,
Case p.type_c when 'Doctorate' Then 1
When 'Master' then 2
When 'Masters' Then 2
When 'Bachelor' Then 3
When 'Bachelors' Then 3
When 'Individual Course' Then 4
When 'Non-Degree' Then 4
When 'Certificate' Then 5
Else 99 END As LevelCd,
P.product_code as ProductName,
P.family as ProductFamily,
P.program_code_c as ProgramGroup,
P.School_College_c as SchoolCollege,
p.college_description_c as CollegeDescription,
P.concentration_description_c as ConcentrationDescription,
P.Description as ProductDescription,
case when UPPER(CAST(P.Is_Active AS STRING)) = 'FALSE' then 1 when UPPER(CAST(P.Is_Active AS STRING)) = 'TRUE' then 0 end as Isdeleted,
(CASE WHEN p.type_c = 'Individual Course' then 1
  WHEN (UPPER(CAST(P.Is_Active AS STRING))  = 'TRUE' and P.Name like 'NDEG%' and p.type_c  not like '%Indiv%') Then 1 ELSE 0 END) as NonDegreeFlag ,

ifnull (PS.start_date_c , ifnull(cast(ASF.discussed_start_date as date), ifnull ((SAFE_CAST(ASF.intended_start_date as date)),  '1900-01-01' ))) AS ProductStartDate,
PS.start_date_c as StartDate,
CASE WHEN (cast(SUBSTR(PS.term_c,5,2) as INT64) = 20
        OR cast(SUBSTR(PS.Term_c,5,2) as INT64) = 40
        OR cast(SUBSTR(PS.Term_c,5,2) as INT64)  = 60) THEN 'Y'
                     ELSE 'N' END AS SemesterFlag,
CASE WHEN (cast(SUBSTR(PS.term_c,5,2) as INT64) = 10
        OR cast(SUBSTR(PS.Term_c,5,2) as INT64) = 30
        OR cast(SUBSTR(PS.Term_c,5,2) as INT64) = 50
		OR cast(SUBSTR(PS.Term_c,5,2) as INT64) = 70) THEN 'Y'
                     ELSE 'N' END AS QuarterFlag,

IFNULL(STO.Owner_Id,EA.id) as EASFID, --need to check this EASFID

null AS FieldRepName,

o.opp_stage_rank_c as OverallRank,
o.stage_name as StageName,
o.disposition_c as Disposition,

ifnull(o.closed_lost_reason_1_c,'Unknown') as ClosedLostReason1,
ifnull(o.closed_lost_reason_2_c,'Unknown') as ClosedLostReason2,
ifnull(o.closed_lost_reason_3_c,'Unknown') as ClosedLostReason3,

CR.name as Region,

STO.CREATEDDATE AS Old_TASK_CREATED_DATE,
ifnull(DATETIME_ADD(STO.createddate, INTERVAL -call_duration_in_seconds SECOND ),STO.Createddate) as TASK_CREATED_DATE,
Case When ASF.disposition like 'Pre-Opp%' or ASF.disposition like 'Applicant%'   --ASF.disposition like 'Applicant%' is not present in snapshot
			Then ifnull(cast(ASF.applicant_new_timestamp as timestamp) , cast(ASF.created_date_est as timestamp))
			Else cast(ASF.created_date_est as timestamp) End SLA_Start,
Case	When ifnull(DATETIME_ADD(STO.createddate, INTERVAL -call_duration_in_seconds SECOND ),STO.Createddate) < created_date_est
		then created_date_est
		else ifnull(DATETIME_ADD(STO.createddate, INTERVAL -call_duration_in_seconds SECOND ),STO.Createddate)
END As TASK_CREATED_DATE_EXC,

ASF.is_tempo_flag,
null as EnrollmentAdvisorDIMKey,
STO.TaskSFId,

STO.transactionid_c,
STO.CREATEDDATE AS TASK_CREATEDDATE,

o.military_affiliation_c,
o.active_military_c,
o.military_veteran_c,
sto.interaction_id,
sto.accept_timestamp,
o.lead_source,
asf.timezone,
bp.time_zone_id_c,
CASE WHEN s.country_ISO_Code_c in ('AA', 'AE', 'AP', 'PR', 'VI', 'GU', 'AS', 'FR','MP', 'UM','PW','FM','MH','CM','CZ','RQ') Then 1
WHEN coalesce(s.Name,bp.Billing_Country_Text_c ) Is Not Null and coalesce(s.Name,bp.Billing_Country_Text_c) Not In ('us', 'u.s.','United States','usa','u.s.a.') Then 1
When coalesce(s.Name,bp.Billing_Country_Text_c) Is Null AND bp.Billing_State_c is not null And (s.Country_ISO_Code_c !='US' OR s.Country_ISO_Code_c is null) Then 1
ELSE 0 END As InternationalFlag

from rpt_crm_mart.t_wldn_opp_snapshot asf
left join raw_b2c_sfdc.opportunity o on o.id = asf.opp_sfid
left join raw_b2c_sfdc.campaign c on c.id = o.campaign_id
left join raw_b2c_sfdc.account a on c.SFDC_Supplier_ID_c = a.id
left join raw_b2c_sfdc.brand_profile_c bp on bp.id = o.brand_profile_c
left join raw_b2c_sfdc.product_2 p on p.id = o.Original_Program_of_Interest_c
left join rpt_crm_mart.v_wldn_loe_enrollment_advisors ea on ea.banner_id_c	 = asf.banner_id
left join (select row_number() over(partition by program_c order by term_c desc ) as rownum ,
             PS.term_c,active_c, ps.start_date_c ,id
              from raw_b2c_sfdc.start_date_c ps
                where cast(ps.active_c as string) = 'true'
            ) ps
ON CAST(o.start_date_c AS DATE) = CAST(ps.start_date_c AS DATE)
and o.original_program_of_interest_c = ps.id
and ps.rownum = 1
left join raw_b2c_sfdc.country_c cr
on cr.id = bp.billing_country_c
left join raw_b2c_sfdc.state_c s
on s.country_iso_code_c = cr.iso_code_c
left join (select t1.id as TaskSFId, what_id ,t1.Owner_Id ,transactionid_c ,cast(datetime(t1.created_date,"America/New_York") as date)
			,ifnull(a.interaction_id,cc.contact_id) as interaction_id, ifnull(cast(a.accept_timestamp  as timestamp),cast(datetime(cc.accept_timestamp,"America/New_York") as timestamp) ) as accept_timestamp
			,case when t1.what_id like '006%' then t1.what_id else null end as opp_sfid , datetime(t1.created_date,"America/New_York") as CREATEDDATE
			,call_duration_in_seconds ,row_number() over(partition by what_id order by datetime(t1.created_date,"America/New_York") ,T1.Last_Modified_Date ) as rownum

			from raw_b2c_sfdc.task t1
			left join `daas-cdw-prod.stg_cc_8x8.allinteractions` a
			on  t1.transactionid_c   = cast ( a.interaction_id as string )
			and cast(substr(create_timestamp,1,10) as date) = cast(datetime(t1.created_date,"America/New_York") as date)
      left join `raw_cc_cxone.contacts_completed` cc
			on  t1.call_object   = cast (cc.contact_id as string )
			and `rpt_bi_cdm.udf_convert_UTC_to_EST`(cast(cc.contact_start as timestamp))


			-- and cast(timestamp(cc.contact_start,"America/New_York") as date)

							= cast(datetime(t1.created_date,"America/New_York") as date)
			where t1.what_id is not null
	  		AND (
		    (t1.Type_c IS NULL AND t1.Call_Type= 'Outbound')
	 		OR (t1.Type_c = 'Chat')
	 		OR (t1.Type_c ='Phone Call' AND t1.Call_Type= 'Inbound' AND IFNULL(t1.Status_c,'DUMMY') NOT IN ('Scheduled','Voicemail Received','--None--'))
	 		OR (t1.Type_c ='Phone Call' AND t1.Call_Type= 'Outbound' AND IFNULL(t1.Status_c,'DUMMY') NOT IN ('Scheduled','Voicemail Received'))
	 		OR (t1.Type_c ='Skype Call' AND IFNULL(t1.Status_c,'DUMMY') NOT IN ('Scheduled'))
 		)
			) STO
on case when STO.what_id like '006%' then STO.what_id else null end = o.id
and sto.rownum = 1
where ASF.is_deleted = FALSE   ###need to check this
and DATE(ASF.created_date_est) > '2019-12-31'
and ASF.institution_id =5
and bp.institution_c in ( 'a0ko0000002BSH4AAO' )

)

Select
interaction_id,
accept_timestamp,
Outreach.active_engaged_timestamp,
Outreach.active_interest_verified_timestamp,
Outreach.applicant_admissions_review_in_progress_timestamp,
Outreach.applicant_admitted_timestamp,
Outreach.applicant_in_process_timestamp,
Outreach.application_submitted_flag,
Outreach.applicant_new_timestamp,
Outreach.closed_lost_timestamp, --ClosedTimestamp
--datetime(o.Created_Date,"America/New_York") as CreateDateTimestamp, this column is not being pulled
Outreach.current_stage_disposition_timestamp,
Outreach.discussed_start_date,
--Outreach.EffectiveDt,
Outreach.inactiveflag, --
Outreach.IntendedStartDateRFI as IntendedStartDateRFI,
Outreach.begin_stage_disposition_timestamp,
Outreach.created_date, --Outreach.Createddatedimkey,
safe_cast (Outreach.IntendedStartDate as date) AS IntendedStartDate, --need to check this
Outreach.open_uncontacted_timestamp,
Outreach.oppsfid,
Outreach.preenroll_registered_timestamp,
Outreach.preenroll_reserved_timestamp,
Outreach.qualified_committed_timestamp,
Outreach.qualified_documents_pending_timestamp,
Outreach.is_deleted as DeletedInSf , --Outreach.DeletedInSf,
Outreach.ASF_InternationalFlag Outreach_InternationalFlag,
Outreach.applicant_complete_ready_for_review_timestamp,
Outreach.student_active_flag,
Outreach.withdrawn_flag,
cast(Outreach.FirstContactedDate as timestamp) as FirstContactedDate,

Outreach.ActivityId,
Outreach.ChannelEDW,
Outreach.ContactName,
Outreach.SupplierId,
Outreach.Status,
Outreach.SupplierName,

Outreach.Institution_Name,
Outreach.institution_id,

Outreach.BannerID,
Outreach.BrandProfileSFId,
Outreach.FirstName,
Outreach.LastName,
Outreach.Name,
Outreach.ShippingCountryText,
Outreach.ShippingStateText,
Outreach.BillingCountryText,
Outreach.BillingStateText,

Outreach.BPCLeg,
Outreach.DegreeLevel,
Outreach.LevelDescription,
Outreach.LevelCd,
Outreach.ProductName,
Outreach.ProductFamily,
Outreach.ProgramGroup,
Outreach.SchoolCollege,
Outreach.CollegeDescription,
Outreach.ConcentrationDescription,
Outreach.ProductDescription,
Outreach.Isdeleted,
Outreach.NonDegreeFlag ,

Outreach.ProductStartDate,
Outreach.StartDate,
Outreach.SemesterFlag,
Outreach.QuarterFlag,

m.name as Usermanager,
pro.Name as Profile,
ifnull(EA.Department, EA.department_c) as Department,
coalesce(EA.Location_c, EA.Office_Location_c) as c__site,
EA.Division as c__Vertical,
EA.name as ENROLLMENTADVISOR,

case when UPPER(CAST(EA.international_c AS STRING)) = 'FALSE' then 0 when UPPER(CAST(EA.international_c AS STRING)) = 'TRUE' then 1 END as  EA_INTERNATIONALFLAG,

m.name as ManagerName,
cast(Outreach.FieldRepName as string) as FieldRepName,

Outreach.OverallRank,
Outreach.StageName,
Outreach.Disposition,

Outreach.ClosedLostReason1,
Outreach.ClosedLostReason2,
Outreach.ClosedLostReason3,

Outreach.Region,

Outreach.Old_TASK_CREATED_DATE,
Outreach.TASK_CREATED_DATE,

Outreach.SLA_Start,
cast(Case When Outreach.TASK_CREATED_DATE_EXC < cast(Outreach.FirstContactedDate as datetime)
	 Then Outreach.TASK_CREATED_DATE_EXC
	Else ifnull(cast(Outreach.FirstContactedDate as datetime), Outreach.TASK_CREATED_DATE_EXC)
	End as timestamp) As SLA_End,

timestamp_diff( case when ifnull(cast(accept_timestamp as string),cast(Outreach.task_created_date_exc  as string))
       < cast(firstcontacteddate as string)
              then cast(ifnull(cast(accept_timestamp as string),cast(Outreach.task_created_date_exc as string)) as timestamp)
            else cast(ifnull(cast(firstcontacteddate as string),
            ifnull(cast(accept_timestamp as string),cast(Outreach.task_created_date_exc as string))) as timestamp)
            end,cast(sla_start as timestamp),second) sto_seconds,

Outreach.TASK_CREATED_DATE_EXC,

Upper(format_date('%a', date(Outreach.SLA_Start))) As Day_Of_Week,

Outreach.CreateDateTimestamp ,
TIMESTAMP(cast(Outreach.CreateDateTimestamp as datetime), 'America/New_York') AS CreatedDateTimestamp_local ,
TIMESTAMP_ADD(TIMESTAMP_trunc(timestamp(cast(Outreach.CreateDateTimestamp as datetime), time_zone_id_c ) , day) , interval 8 hour) AS CreatedDateTimestampStartTime ,
TIMESTAMP_ADD(TIMESTAMP_trunc(timestamp(cast(Outreach.CreateDateTimestamp as datetime), time_zone_id_c ) , day) , interval 16 hour) AS CreatedDateTimestampEndTime ,
timezone ,
time_zone_id_c ,
Case
When InternationalFlag = 0 and
TIMESTAMP(cast(Outreach.CreateDateTimestamp as datetime), time_zone_id_c) between
TIMESTAMP_ADD(TIMESTAMP_trunc(timestamp(cast(Outreach.CreateDateTimestamp as datetime), time_zone_id_c ) , day) , interval 8 hour) and
TIMESTAMP_ADD(TIMESTAMP_trunc(timestamp(cast(Outreach.CreateDateTimestamp as datetime), time_zone_id_c ) , day) , interval 20 hour)   Then 'Y'

When InternationalFlag = 1 and
TIMESTAMP(cast(Outreach.CreateDateTimestamp as datetime), time_zone_id_c) between
TIMESTAMP_ADD(TIMESTAMP_trunc(timestamp(cast(Outreach.CreateDateTimestamp as datetime),  time_zone_id_c ) , day) , interval 7 hour) and
TIMESTAMP_ADD(TIMESTAMP_trunc(timestamp(cast(Outreach.CreateDateTimestamp as datetime), time_zone_id_c ) , day) , interval 22 hour)   Then 'Y'

When time_zone_id_c is null then 'Missing TimeZone'

Else 'N' End as Business_Hrs_Flg,

cast(Outreach.is_tempo_flag as INT64) as IsTempo,
Outreach.EnrollmentAdvisorDIMKey,
Outreach.TaskSFId,
d.name as Director,
ea.Title,
Outreach.military_affiliation_c,
Outreach.active_military_c,
Outreach.military_veteran_c,
Outreach.lead_source

from Outreach
left join rpt_crm_mart.v_wldn_loe_enrollment_advisors ea
on Outreach.EASFID = ea.id
left join raw_b2c_sfdc.user m
on m.id = ea.manager_id
left join raw_b2c_sfdc.user d
on d.id = ea.director_c
left join raw_b2c_sfdc.profile pro
on pro.id = ea.profile_id
