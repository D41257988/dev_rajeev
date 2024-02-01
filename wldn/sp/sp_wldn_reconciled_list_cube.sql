CREATE OR REPLACE PROCEDURE `trans_academics.sp_wldn_reconciled_list_cube`(IN v_audit_key STRING, OUT result STRING)
begin

declare institution string default 'WLDN';
declare institution_id int64 default 5;
declare source_system_name string default 'WLDN_SF';
declare dml_mode string default 'delete-insert';
declare target_dataset string default 'rpt_academics';
declare target_tablename string default 't_wldn_reconciled_list_cube';
declare source_tablename string default 'reconciled_cube_output';
declare load_source string default 'trans_academics.sp_wldn_reconciled_list_cube';
declare additional_attributes ARRAY<struct<keyword string, value string>>;
declare last_refresh_time timestamp;
declare tgt_table_count int64;

/* common across */
declare job_start_dt timestamp default current_timestamp();
declare job_end_dt timestamp default current_timestamp();
declare job_completed_ind string default null;
declare job_type string default 'EDW';
declare load_method string default 'scheduled query';
declare out_sql string;
declare CurrentYrStart date;
declare PreviousYrStart date;

begin

SET additional_attributes= [("audit_load_key", v_audit_key),
          ("load_method",load_method),
          ("load_source",load_source),
          ("job_type", job_type)];
/* end common across */

create or replace temp table Reconciled_List as
(
with reconciled_list as(
select distinct a.applicant_id as CredentialID
   ,a.institution_cd
  ,cast(left(student_start_date,10) as date) as start_dt
  ,d.cal_yr_num as year
  ,d.cal_qtr_num as Quarter
  ,d.cal_mth_num as month
  ,case when a.opp_sf_id like '%NULL%' then NULL else a.opp_sf_id end as opp_sf_id
  --,o.program_of_interest_c as product_sf_id
  ,international_flag
  ,safe_cast(a.term_cd as int64) as term_cd
from rpt_academics.t_wldn_reconcile_list a
left join `mdm.dim_date` d on cast(left(student_start_date,10) as date) = d.cal_dt
--left join `raw_b2c_sfdc.opportunity` o on a.opp_sf_id = o.id
where a.applicant_id like '%A%'
--and o.institution_c = 'a0ko0000002BSH4AAO'
and  student_start_date <> '00:00.0'
-- and extract(year from SAFE.PARSE_DATE('%d-%b-%y',student_start_date)) > 2018
),
/*Adding opportunity id for the NULL values by joing on banner id and start date*/
opp_table as
(SELECT distinct opp.banner_id_c,opp.id,sd.start_date_c as start_date,opp.current_program_full_banner_code_c,opp.is_deleted,opp.system_modstamp,opp.created_date,
ROW_NUMBER ()	OVER (PARTITION BY opp.banner_id_c,opp.current_program_full_banner_code_c,sd.start_date_c ORDER BY opp.system_modstamp desc) AS rid
FROM `raw_b2c_sfdc.opportunity` opp
left join `raw_b2c_sfdc.start_date_c` sd on opp.selected_program_start_date_c=sd.id
where opp.is_deleted=false and opp.institution_c='a0ko0000002BSH4AAO'
),


opp_final as(
  select * from opp_table where rid=1
),
reconciled_ls as(
select distinct rcl.* except(opp_sf_id),COALESCE(rcl.opp_sf_id,opp.id) as opp_sf_id,
from reconciled_list rcl
left join opp_final opp on rcl.CredentialID=opp.banner_id_c and rcl.start_dt=opp.start_date)
select *,o.program_of_interest_c as product_sf_id from reconciled_ls rcl
left join `raw_b2c_sfdc.opportunity` o on rcl.opp_sf_id = o.id
);

create  or replace temp table ES_EM_asofStart as
(
with ES_EM_asofStart as(
select
distinct a.CredentialID
,a.Start_dt
,b.AdmRepName AS EA_Name
,b.Manager AS EA_Manager
,b.EA_Location
,b.Student_Start_Dt
,b.etl_created_date
from Reconciled_List a
left join /*Pull minimum ETL process date on or after the start date to get ES assigned at time of start date*/
	(
		select
			distinct a.CredentialID
			,a.Start_dt
			,min(cast(b.etl_created_date as date)) as Min_ETL_Dt
		from Reconciled_List a
		left join `rpt_academics.t_wldn_reserve_list` b
			on a.CredentialID = b.Applicant_Id
			and cast(a.Start_Dt as date)= cast(b.Student_Start_Dt as date)
		where cast(b.etl_created_date as date) >= cast(b.Student_Start_Dt as date)
		group by a.CredentialID,a.Start_Dt
	) c
on a.CredentialID = c.CredentialID
and cast(a.Start_dt as date) = cast(c.Start_dt as date)
left join `rpt_academics.t_wldn_reserve_list` b
 on a.CredentialID = b.Applicant_Id
 and a.Start_dt = cast(b.Student_Start_Dt as date)
 and cast(b.etl_created_date as date) = c.Min_ETL_Dt),

/* Deduping records to have one record per student_id per start_date and by ordering Unknown in ea_name and ea_manager to last */
ES_EM_asofStart_deduped as(
  select *,
  ROW_NUMBER() OVER (PARTITION BY CredentialID,Start_dt
  ORDER BY (case when ea_name not like "%Unknown%" then 1 else 2 end),(case when ea_manager not like "%Unknown%" then 1 else 2 end)) AS rid
  from ES_EM_asofStart
)select * except(rid) from ES_EM_asofStart_deduped where rid=1
);

## Alumni List
create or replace temp table Alumni_List as
(
select distinct
id as credential_id,
OUTCOME_GRADUATION_DATE1 as OUTCOME_GRADUATION_DATE,
program1,
program1_name as program_desc,
graduation_flag1 as graduation_flag
from `rpt_academics.t_wldn_alumni`
where
graduation_flag1= 1
and enrollment_flag1 =1
and cert_flag1 = 0
and OUTCOME_GRADUATION_DATE1 < CURRENT_DATE()
);

##### Reconciled_Alumni
create or replace temp table Reconciled_Alumni  as
(
select
  distinct a.CredentialID
  ,a.start_dt
  ,b.graduation_flag
  ,b.OUTCOME_GRADUATION_DATE
  ,b.Program_desc
  ,case when b.graduation_flag=  1 then 1 else 0 end as Alumni_Flag
from Reconciled_List a
left join Alumni_List b
  on a.CredentialID = b.credential_id
  and (a.start_dt) > b.OUTCOME_GRADUATION_DATE
);


/**Most Recent Program by Graduation Date**/
create or replace temp table Reconciled_Alumni_LatestGradDt AS
(
with temp as
(
	select *, rank() over(partition by CredentialID, start_dt order by CredentialID, start_dt, OUTCOME_GRADUATION_DATE desc) as rankk
	from Reconciled_Alumni
  )
select * from temp where Rankk = 1
);


create or replace temp table  Walden_Recon_Data as (
select
  distinct a.CredentialID as Student_ID
  ,'WLDN' as Institution
  ,(a.Start_Dt) as Start_Date
  ,a.Year
  ,a.Quarter
  ,a.Month
  ,a.term_cd as Term_cd
  ,case when a.international_flag =""  or a.international_flag like "%NULL%" then  if(b2.international_flag_c is true,  'Y' , 'N')
		when a.international_flag like '%0%' then 'N'
		when a.international_flag like '%y%' or a.international_flag like '%1%' then 'Y'
    else a.international_flag end as International_Flag

  ,e.product_nbr
  ,e.program_name as program_name
  ,e.concentration_description as Specialization
  ,e.Level_description as Level_Desc
  ,e.college_code as College_name
  ,e.SQ_flag
  ,e.program_group as banner_pgm_cd

  ,case when c.Channel_c is null then 'Unknown' else c.Channel_c end as Channel
  ,case when h.activity_id_c is null then 'Unknown' else h.activity_id_c end as ActivityID
  ,extract(date from utility.udf_convert_UTC_to_EST(c.created_date)) as Inq_Date
  ,f.Last_Name
  ,f.First_Name
  ,f.full_name_fmil
  ,AI.nation_desc as Country
  ,AI.nation as Country_Abbr
  ,AI.state_province  as state
  ,AI.street_line1
  ,AI.street_line2
  ,AI.street_line3
  ,AI.street_line4
  ,AI.city
  ,AI.postal_code
  ,UE.internet_address as Walden_Email
  ,PE.internet_address as Personal_Email
  ,c.id as opportunity_id
  ,c.brand_profile_c
  --------------------------------------------------------------------------------1. Added Contact Id field from opportunity (Nicole 4/3)
  ,c.contact_c as contact_id

  ,FALSE as CBL_Flag
  ,0 as Istempo
  ,h.Name as CampaignName
  ,j.Alumni_Flag
  ,j.OUTCOME_GRADUATION_DATE as Graduation_Date
  ,j.program_desc as Graduated_Pgm
  ,b2.hispanic_latino_c as HispanicLatino
  ,b2.race_c as Race
  ,b2.home_phone_c
  ,f.birth_date
  ,f.primary_ethnicity_desc
  ,f.gender
  ,f.hispanic_latino_ethnicity_ind
  ,case when c.military_affiliation_c  = true then 1 else 0 end as Military_flag
  ,u.EA_Name
  ,u.EA_Manager
  ,u.EA_Location
from
 Reconciled_List a
 left join `rpt_academics.t_wldn_product_map` e on a.product_sf_id = e.product_sfid and e.institution_id = 5
 left join `rpt_academics.t_person` f on a.CredentialID = f.credential_id
Left join `rpt_academics.v_address_international` AI
on AI.entity_uid = f.person_uid
left join `rpt_academics.v_internet_address_current` UE on f.person_uid = UE.entity_uid and UE.internet_address_type  = 'UNIV' and UE.institution_id =5
left join `rpt_academics.v_internet_address_current` PE on f.person_uid = PE.entity_uid and PE.internet_address_type  = 'PERS' and PE.institution_id =5
/*Contact info based off of Banner*/
left join `raw_b2c_sfdc.opportunity` c on a.opp_sf_id= c.id
  ---and cast(a.Start_dt as date) = cast(c.created_date as date) ------------------- START DATE JOIN?

/*left join biadm.Walden_Conv_Master_Hybrid c2 on c.groupnewid=c2.groupnewid  */
/*Opp Level Data from Hybrid Conv Master - Maintained by Lei - removed 6/2022 since will not be transitioned to gcp*/

left join `raw_b2c_sfdc.campaign` h
  on c.campaign_id = h.id
  and h.institution_c = 'a0ko0000002BSH4AAO'
  and h.activity_id_c is not null

left join `Reconciled_Alumni_LatestGradDt` j
on a.CredentialID = j.CredentialID
and a.start_dt=j.Start_dt

left join `raw_b2c_sfdc.brand_profile_c` b2
on c.brand_profile_c =b2.id

left join  ES_EM_asofStart u
on a.CredentialID = u.CredentialID
and a.start_dt =  u.start_dt

);

create or replace  temp table temp1 AS
(
select distinct start_date,count(*) as countt
from Walden_Recon_Data
group by start_date
);

################ Recreate this with respect to GCP;

/*Create Perm Dataset to use to merge with tempo data*/
--data Data.Walden_Recon_Data_Latest;
--set
--Data.Walden_Recon_Data_2010_2017
--Walden_Recon_Data;
--run;


create or replace temp table Walden_Recon_Data_Latest as
(
	select * from Walden_Recon_Data
	-- UNION ALL
	-- Select * from History_data  ------------------------------ Yet to pull data in GCP
);

-------------------------------------------------------------------------------2. Added Contact Id to t_wldn_tempo_recon_data from salesforce contact table to union accuratly  (Nicole 4/3)
create or replace temp table tempo_recon_data AS
(Select
trd.student_id
,trd.institution
,trd.start_date
,trd.year
,trd.quarter
,trd.month
,trd.term_cd
,trd.international_flag
,trd.product_nbr
,trd.program_name
,trd.specialization
,trd.level_desc
,trd.college_name
,trd.sq_flag
,trd.banner_pgm_cd
,trd.channel
,trd.activityid
,trd.inq_date
,trd.last_name
,trd.first_name
,trd.full_name
,trd.country
,trd.country_abbr
,trd.state
,trd.street_line1
,trd.street_line2
,trd.street_line3
,trd.street_line4
,trd.city_c
,trd.postal_code
,trd.university_email
,trd.personal_email
,trd.opportunity_id
,trd.brand_profile_c
,ss.contact_sfid as contact_id
,trd.cbl_flag
,trd.istempo
,trd.campaignname
,trd.alumni_flag
,trd.graduation_date
,trd.graduated_pgm
,trd.hispaniclatino
,trd.race
,trd.home_phone_c
,trd.birth_date
,trd.primary_ethnicity
,trd.gender
,trd.hispanic_latino_ethnicity_ind
,trd.military_flag
,trd.ea_name
,trd.ea_manager
,trd.ea_location
from `rpt_academics.t_wldn_tempo_recon_data` trd
left join rpt_crm_mart.t_wldn_opp_snapshot ss on trd.opportunity_id=ss.opp_sfid
);

create or replace temp table Reconciled_Cube_Latest AS
(
	select * from Walden_Recon_Data_Latest
	UNION ALL
  select * from tempo_recon_data

);


create or replace temp table  partnership_accounts as (
select distinct
  a1.id as partnersfid
  ,a1.name as partnername
  ,coalesce (a8.name,a7.name,a6.name,a5.name,a4.name,a3.name,a2.name,a1.name) as partner_account_l1
  ,a1.parent_id
  ,a2.name as parent_name1
  ,a3.name as parent_name2
  ,a4.name as parent_name3
  ,a5.name as parent_name4
  ,a6.name as parent_name5
  ,a7.name as parent_name6
  ,a8.name as parent_name7
-- into #partneraccounts
 from     `raw_b2c_sfdc.account` a1
left join `raw_b2c_sfdc.account` a2 on a1.parent_id=a2.id
left join `raw_b2c_sfdc.account` a3 on a2.parent_id=a3.id
left join `raw_b2c_sfdc.account` a4 on a3.parent_id=a4.id
left join `raw_b2c_sfdc.account` a5 on a4.parent_id=a5.id
left join `raw_b2c_sfdc.account` a6 on a5.parent_id=a6.id
left join `raw_b2c_sfdc.account` a7 on a6.parent_id=a7.id
left join `raw_b2c_sfdc.account` a8 on a7.parent_id=a8.id
where a1.id is not null and a1.institution_brand_c='a0ko0000002BSH4AAO' and a1.is_deleted = false /*and a1.name not like 'Account%'*/ ---------------------------------------3.Removed filter on Name (Nicole 4/3)
);

create or replace temp table students as
(
select distinct
  r.student_id
  --------------------------------------------------------------------------------------------4.Added Contact Id field (Nicole 4/3)
  ,r.contact_id
  ,r.opportunity_id
  ,r.start_date
  ,r.year
  ,r.istempo
  ,r.channel as nonhybrid_channel
  ,brand_profile_c
/*new*/
  ,b.partnername as partner_nonhybrid
  ,b.partner_account_l1 as partner_account_l1_nonhybrid
/*original*/
/*c2.partnername as partner_nonhybrid_o,c2.partner_account_l1 as partner_account_l1_nonhybrid_o*/
from Reconciled_Cube_Latest r
left join `raw_b2c_sfdc.campaign` c on r.ActivityID=c.activity_id_c and c.is_deleted=false and c.institution_c = 'a0ko0000002BSH4AAO'
left join partnership_accounts b
  on c.partner_account_c = b.partnersfid /*using linked partner/top tier parent from above*/
/*left join biadm.walden_campaign_partner_detail c2 on r.activityid=c2.activityid*/
where extract(year from r.start_date)>=2017
);

create or replace temp table employers_bd as
(
select * from (
select distinct
  r.student_id
  ,r.brand_profile_c
  ,b.employer_name_c
  ,b.are_you_currently_employed_here_c
  ,b.bd_partner_c
  ,b.bd_account_c
  ,b.from_date_c
  ,b.to_date_c
  ,b.created_date
  ,1 as bd_employer
  ,d.partnername as bd_partner_account
  ,d.partner_account_l1 as parent_account_employer
  , row_number() over(partition by r.brand_profile_c,student_id order by created_date desc, from_date_c desc) as rn
from students r
left join `raw_b2c_sfdc.employment_c` b
  on r.brand_profile_c = b.brand_profile_c
left join partnership_accounts d
  on b.bd_account_c = d.partnersfid /*using linked partner/top tier parent from above*/
/*left join biadm.walden_campaign_partner_detail c on b.bdaccount=c.partnersfid*/
where b.are_you_currently_employed_here_c is True
and b.bd_partner_c is true
) where rn = 1
);

create or replace temp table sf_case_partnershipname as
(
  select * from (
select distinct
   a.banner_id_c as bannerid
   --------------------------------------------------------------------------------5.Added Contact Id field (Nicole 4/3)
  ,a.contact_id
  ,a.subtype_c
  ,a.bd_partner_account_lookup_c
  ,b1.partnername as bd_partner_account
  ,b1.partner_account_l1 as parent_account_discount
  ,a.start_date_c
  ,a.created_date
  ,row_number() over(partition by banner_id_c, start_date_c order by a.created_date desc) as rn
from `raw_b2c_sfdc.case` a
left join partnership_accounts b1  on a.bd_partner_account_lookup_c=b1.partnersfid
where a.subtype_c='BD Partnership Tuition Reduction' and a.is_deleted = false and a.institution_brand_c = 'a0ko0000002BSH4AAO'

---------------------------------------------------------------------------------6.Modified: Filters removed (Nicole 4/3)
/*and b1.partnername is not null
and a.banner_id_c is not null*/
and extract(year from a.start_date_c)>=2021
) where rn = 1
);

 create or replace temp table partner_discounts_sf as (
select distinct
  r.student_id
  ,r.start_date
  --------------------------------------------------------------------------------7.Added Contact Id field (Nicole 4/3)
  ,r.contact_id
  ,r.brand_profile_c
  ,case when b.contact_id is not null or b2.contact_id is not null then 1 else 0 end as bd_discount
  ,case when b.contact_id is not null then b.bd_partner_account else b2.bd_partner_account end as partnership_discount
  ,case when b.contact_id is not null then b.parent_account_discount else b2.parent_account_discount end as parent_account_discount
  ,case when b.contact_id is not null then b.bd_partner_account_lookup_c else b2.bd_partner_account_lookup_c end as bd_account_disc
from students r

------------------------------------ ------------------------------------------8.Updated Joins using Contact ID  (Nicole 4/3)

/*join on start date as first priority*/
left join sf_case_partnershipname b
  on r.contact_id=b.contact_id
  and r.start_date = cast(b.start_date_c as date)
/*join on or after start date as catchall*/
left join sf_case_partnershipname b2
  on r.contact_id = b2.contact_id
  and r.start_date >= cast(b2.start_date_c as date)
where r.start_date >='2021-07-01'
and (
    b.contact_id is not null
      or
    b2.contact_id is not null
    )

----Original version: join on Banner id
/*
left join sf_case_partnershipname b
  on r.student_id=b.bannerid
  and r.start_date = cast(b.start_date_c as date)
left join sf_case_partnershipname b2
  on r.student_id = b2.bannerid
  and r.start_date >= cast(b2.start_date_c as date)
where r.start_date >='2021-08-01'    -----parse_date("%d%h%y", '01AUG21')
and (
    b.bannerid is not null
      or
    b2.bannerid is not null
    )*/
);

 create or replace temp table partner_discounts_gurfeed as (
  select * from (
select distinct
  r.student_id
    --------------------------------------------------------------------------------9.Added Contact Id field (Nicole 4/3)
  ,r.contact_id
  ,r.start_date
  ,r.brand_profile_c
  ,d.offergrp_iii
  ,d.exemption_code
  ,d.description
  ,1 as bd_discount
  ,substr(exemption_code,6,3) as exemp_cd
  ,d.acctg_date
/*hardcode major partnership names to match naming conventions on updated look up table to get accurate parent account*/
  ,case
    when d.partnership_name='Hospital Corporations of America, Inc. (HCA)' then 'Hospital Corporation of America'
    when d.partnership_name='Hospital Corporations of America' then 'Hospital Corporation of America'
    when d.partnership_name='National Association of Nigerian Nurses in North America (NANNNA)' then 'National Association of Nigerian Nurses in North America'
    when d.partnership_name='National Association for the Education of Young Children (NAEYC)' then 'National Association for the Education of Young Children'
    when d.partnership_name='Catholic Health Initiatives (CHI)' then 'Catholic Health Initiatives'
    when d.partnership_name='Black Nurses Rock' then 'Black Nurses  - National'
    when d.partnership_name='Black Nurses Rock - National' then 'Black Nurses  - National'
    when d.partnership_name='Caribbean Union of Teachers' then 'Caribbean Union of Teachers (CUT)'
    when d.partnership_name='Kindred Healthcare' then 'Kindred Healthcare Operating Inc.'
    when d.partnership_name='AdventHealth (Adventist Health System)' then 'AdventHealth (Florida Adventist Health System)'
    when d.partnership_name='University of Pittsburgh' then 'UPMC'
    when d.partnership_name='Department of Health and Human Services (HHS)' then 'U.S. Department of Health and Human Services'
    when d.partnership_name='Meridian Health' then 'Hackensack Meridian Health'
    when d.partnership_name='Adventist Health CA, HI, OR, WA' then 'Adventist Health(west coast, Cali,Oregon,Hawaii)'
    when d.partnership_name='Adventist Healthcare MD,NJ' then 'Adventist Healthcare (Maryland)'
    when d.partnership_name='Cigna Corporation (Worldwide)' then 'Cigna Corporation'
    when d.partnership_name='Anne Arundel Medical Center (MD)' then 'Anne Arundel Medical Center (AAMC)'
    when d.partnership_name='Holy Cross Health, Inc.' then 'Holy Cross Health'
  else d.partnership_name end as partnership_discount
  ,row_number() over(partition by student_id, start_date order by acctg_date desc) as rn
from students r
left join `rpt_academics.t_wldn_gurfeed_discount` d
/*updated 11/2021- use gurfeed data for starts through 7/2021
use sf cases for starts 8/2021+**/
on r.student_id = d.gurfeed_id
and r.student_id is not null
and  d.acctg_date >= r.start_date
where d.offergrp_iii like 'Partner%'
) where rn = 1
);

create or replace temp table partner_discounts_gurfeed_2 as
(
  select * from
  (
	select distinct
	  a.*
	  ,case
      when b1.partner_account_l1 <>'' then b1.partner_account_l1
      else b2.partner_account_l1
	  end as parent_account_discount
	  ,row_number() over(partition by student_id, start_date) as rn1
	from partner_discounts_gurfeed a
	left join partnership_accounts b1
	on a.partnership_discount = b1.partnername
	and b1.partnername <> ''
	and b1.parent_id <> ''
	/*use records with linked parent account first*/
	left join partnership_accounts b2
	on a.partnership_discount = b2.partnername
	and b2.partnername <> ''
	)
	where rn1 = 1
);

create or replace temp table partner_discounts as
(   ------partner_discounts_2
  select * from
  (
	select distinct
	  r.student_id
	  ,r.start_date
     --------------------------------------------------------------------------------10.Added Contact Id field (Nicole 4/3)
    ,r.contact_id
	  ,r.brand_profile_c
	  /*updated to take give sf case discount priority over gurfeed data to be able to pull in bd account id and have consistent naming conventions across sources*/
	  ,case
		when s.bd_discount=1 then s.bd_discount
		else g.bd_discount
	  end as bd_discount
	  ,case
		when s.bd_discount=1 then s.partnership_discount
		else g.partnership_discount
	  end as partnership_discount
	  ,case
		when s.bd_discount=1 then s.parent_account_discount
		else g.parent_account_discount
	  end as parent_account_discount
	  ,s.bd_account_disc
	  ,g.bd_discount as bd_discount_g
	  ,s.bd_discount as bd_discount_sf
	  /**dedupe to ensure only one partner/parent name per bannerid/start date ***/
	  ,row_number() over(partition by r.student_id, r.start_date) as rn
	from students r
	left join partner_discounts_gurfeed_2 g
	on r.student_id=g.student_id
	and r.start_date=g.start_date
	left join partner_discounts_sf s
    on
    /*r.student_id=s.student_id */
------------------------------------- ------------------------------------------11. Modified Join from student Id to contact ID (Nicole 4/3)
    r.contact_id=s.contact_id

    and r.start_date=s.start_date
    and r.start_date >= '2021-07-01' -----parse_date("%d%h%y", '01aug21')
	where g.bd_discount=1
    or s.bd_discount=1
	)
	where rn = 1
);

create or replace temp table bd_opps as (
select * from
	(
	select distinct
	  r.student_id
	  ,r.start_date
	  ,r.brand_profile_c
	  ,r.nonhybrid_channel
	  ,r.partner_nonhybrid
	  ,r.partner_account_l1_nonhybrid
	  ,a.inq_date
	  ,a.opp_sfid
	  ,a.channel
	  ,a.activity_id
	  ,b.partner_account_c
	  ,1 as bd_channel
	  ,d.partnername
	  ,d.partner_account_l1
	  /**sort and de-dupe by latest inq date**/
	  ,row_number() over(partition by r.brand_profile_c, r.start_date order by inq_date desc) as rn
	from students r
	left join `rpt_crm_mart.t_wldn_opp_snapshot` a
	on r.brand_profile_c = a.brand_profile_sfid
	and r.start_date>=a.inq_date
	left join `raw_b2c_sfdc.campaign` b on a.activity_id=b.activity_id_c
	left join partnership_accounts d
	on b.partner_account_c=d.partnersfid
	and b.partner_account_c is not null
	where a.channel like 'Business%'
	)
	where rn = 1
);

create or replace temp table bd_opps_partner as
(
select distinct
  *
  ,case
    when nonhybrid_channel like 'Business%' and partner_nonhybrid not in ('','Inbound Call') then partner_nonhybrid
    when nonhybrid_channel not like 'Business%' /*and Hybrid_Channel not like 'Business%' */
then partnername
    else partnername end as partner_channel
  ,case
    when nonhybrid_channel like 'Business%' and partner_nonhybrid not in ('','Inbound Call') then partner_account_l1_nonhybrid
    when nonhybrid_channel not like 'Business%' then partner_account_l1
    else partner_account_l1 end as partner_account_l1_channel


from bd_opps
);

create or replace temp table alldata as
(
select distinct
  a.*
  ,e.bd_partner_account as partner_employer
  ,e.parent_account_employer as partner_account_l1_employer
  ,e.bd_partner_c
  ,case
    when e.bd_employer=1 then e.bd_employer
    else 0 end as bd_employer
  ,d.partnership_discount
  ,d.parent_account_discount
  ,case
    when d.bd_discount=1 then d.bd_discount
    else 0 end as bd_discount
    ,c.channel
    ,c.partner_channel
    ,c.partner_account_l1_channel
    ,case
      when c.bd_channel=1 then c.bd_channel
      else 0 end as bd_channel
    ,case
      when e.bd_employer=1 or d.bd_discount=1 or c.bd_channel=1 then 1
      else 0 end as bd_partner_enrollment_flag
      ,e.bd_account_c as emp_partnerid
      ,d.bd_account_disc as disc_partnerid
      ,c.partner_account_c as chan_partnerid
from students a
left join employers_bd e on a.brand_profile_c = e.brand_profile_c
------------------------------------- -----------------------------------------------------12. Modified Join from student Id to contact ID (Nicole 4/3)
left join partner_discounts d on /*a.student_id=d.student_id*/ a.contact_id=d.contact_id and a.start_date=d.start_date
-- left join partner_discounts_2 d on a.student_id=d.student_id and a.start_date=d.start_date   ---we can remove as we have used above line in place of this.
left join bd_opps_partner c on a.brand_profile_c=c.brand_profile_c and a.start_date=c.start_date
);

create or replace temp table BD_Enrollments_Data as
(
	select distinct
	  *
	  ,concat(bd_employer,bd_channel,bd_discount) as bd_source_total
	/*new logic 5/2022 - use employer partner first to limit data goveranance issues
	(partner naming conventions and lack of partner sfid for discount data from gurfeed)
	1.employer, 2. sf discount, 3. gurfeed discount, 4.channel*/
	  ,case
		when bd_employer=1 then partner_employer
		when bd_discount=1 and partnership_discount<>'' and  parent_account_discount<>'' and bd_employer=0 then partnership_discount
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='') and bd_employer=0 and bd_channel=1 and partner_channel<>'' then partner_channel
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='') and bd_employer=0 and bd_channel=1 and partner_channel='' then partnership_discount
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='') and bd_employer=0 and bd_channel=0 then partnership_discount
		when bd_discount=0 and bd_employer=0 and bd_channel=1 then partner_channel
		else ''
	  end as partnership_name

	  ,case
		when bd_employer=1 then partner_account_l1_employer
		when bd_discount=1 and partnership_discount<>'' and  parent_account_discount<>'' and bd_employer=0 then parent_account_discount
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='') and bd_employer=0 and bd_channel=1 and partner_channel<>'' then partner_account_l1_channel
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='') and bd_employer=0 and bd_channel=1 and partner_channel='' then parent_account_discount
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='') and bd_employer=0 and bd_channel=0 then parent_account_discount
		when bd_discount=0 and bd_employer=0 and bd_channel=1 then partner_account_l1_channel
		else ''
	  end as parent_account

	  , case
		when bd_employer=1 then 'Employer'
		when bd_discount=1 and partnership_discount<>'' and  parent_account_discount<>''
		and bd_employer=0 then 'Discount'
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='')
		and bd_employer=0 and bd_channel=1 and partner_channel<>'' then 'Channel'
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='')
		and bd_employer=0 and bd_channel=1 and partner_channel='' then 'Discount'
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='')
		and bd_employer=0 and bd_channel=0 then 'Discount'
		when bd_discount=0 and bd_employer=0 and bd_channel=1 then 'Channel'
		else '' end as partnership_name_source

	  ,case
		when bd_employer=1 then emp_partnerid
		when bd_discount=1 and partnership_discount<>'' and  parent_account_discount<>''
		and bd_employer=0 then disc_partnerid
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='')
		and bd_employer=0 and bd_channel=1 and partner_channel<>'' then chan_partnerid
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='')
		and bd_employer=0 and bd_channel=1 and partner_channel='' then disc_partnerid
		when bd_discount=1 and (partnership_discount='' or  parent_account_discount='')
		and bd_employer=0 and bd_channel=0 then disc_partnerid
		when bd_discount=0 and bd_employer=0 and bd_channel=1 then chan_partnerid
		else '' end as partnersfid

	from alldata
	where bd_partner_enrollment_flag=1
);

create or replace temp table reconciled_cube_output as
(
with src as
(
select
	distinct a.*
	,b.BD_Partner_Enrollment_Flag
	,b.PartnerSFID
	,b.Partnership_Name
	,b.Parent_Account
	,b.Partnership_Name_Source
	,b.BD_Discount
	,b.BD_Channel
	,b.BD_Employer
from Reconciled_Cube_Latest a
left join BD_Enrollments_Data b
on a.Student_ID=b.Student_ID
and a.brand_profile_c = b.brand_profile_c
and a.Start_Date=b.Start_Date
)
select src.*,
 5 as institution_id,
	job_start_dt as etl_created_date,
	job_start_dt as etl_updated_date,
	load_source as etl_resource_name,
	v_audit_key as etl_ins_audit_key,
	v_audit_key as etl_upd_audit_key,
	farm_fingerprint(format('%T', (''))) AS etl_pk_hash,
	farm_fingerprint(format('%T', src )) as etl_chg_hash,
from
src
);


-- merge process
 call utility.sp_process_elt (institution, dml_mode , target_dataset, target_tablename, null, source_tablename, additional_attributes, out_sql );

    set job_end_dt = current_timestamp();
    set job_completed_ind = 'Y';

    -- export success audit log record
    call `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);

    -- update audit refresh upon process successful completion
    --multiple driver tables need multiple inserts here, one for each driver table, In this case we only have sgbstdn
    --call `audit_cdw_log.sp_export_audit_table_refresh` (v_audit_key, 'general_student_stage',target_tablename,institution_id, job_start_dt, current_timestamp(), load_source );


    set result = 'SUCCESS';


EXCEPTION WHEN error THEN

SET job_end_dt = cast (NULL as TIMESTAMP);
SET job_completed_ind = 'N';

CALL `audit_cdw_log.sp_export_audit_cdw_log`(v_audit_key,target_tablename, job_start_dt, job_end_dt, job_completed_ind, job_type, load_method, load_source);


-- insert into error_log table
insert into
`audit_cdw_log.error_log` (error_load_key, process_name, table_name, error_details, etl_create_date, etl_resource_name, etl_ins_audit_key)
values
(v_audit_key,'EDW_LOAD',target_tablename, @@error.message, current_timestamp() ,load_source, v_audit_key) ;


set result = @@error.message;
raise using message = @@error.message;

END;

END
