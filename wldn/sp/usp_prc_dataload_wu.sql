BEGIN

delete from tds_analytics_partner_portal.organization_wu_master where true;

INSERT INTO tds_analytics_partner_portal.organization_wu_master
(b2b_account_id,b2b_account_name,walden_account_id,walden_account_name,b2b_parent_id,walden_parent_id,
b2b_global_unique_id,walden_global_unique_id,institution,state,record_insert_date)

select distinct b.b2b_account_id,b.b2b_account_name,a.id,a.name,b.b2b_parent_id,a.parent_id,b.b2b_global_unique_id,
a.global_unique_id_c,'Walden',a.billing_state,current_datetime()
from raw_b2c_sfdc.account a
     join
     tds_analytics_partner_portal.organization_b2b_master b
     on a.global_unique_id_c=b.b2b_global_unique_id and a.is_deleted is false;
-- and (lower(a.type)='affiliate' or a.type is null);

--**********************************************************************************************************
delete from tds_analytics_partner_portal.hospital_wu_master where true;

INSERT INTO tds_analytics_partner_portal.hospital_wu_master(b2b_account_id,b2b_account_name,walden_account_id,walden_account_name,b2b_parent_id,walden_parent_id,
b2b_global_unique_id,walden_global_unique_id,institution,state,hospital,record_insert_date)

select distinct org.b2b_account_id,org.b2b_account_name,org.walden_account_id,org.walden_account_name,org.b2b_parent_id,
org.walden_parent_id,org.b2b_global_unique_id,org.walden_global_unique_id,org.institution,org.state,ac.name,current_datetime()

from raw_b2c_sfdc.account ac
     join
     tds_analytics_partner_portal.organization_wu_master org
     on ac.id=org.walden_account_id
      and ac.is_deleted is false and ac.name !='';

--*************************************************************************************************************

delete from tds_analytics_partner_portal.hospital_wu_employer where true;

INSERT INTO tds_analytics_partner_portal.hospital_wu_employer(b2b_account_id,b2b_account_name,walden_account_id,walden_account_name,b2b_parent_id,walden_parent_id,
b2b_global_unique_id,walden_global_unique_id,institution,state,hospital,brand_profile_c,opportunity_c,eid,record_insert_date)

select distinct hosp.b2b_account_id,hosp.b2b_account_name,hosp.walden_account_id,hosp.walden_account_name,hosp.b2b_parent_id,
hosp.walden_parent_id,hosp.b2b_global_unique_id,hosp.walden_global_unique_id,hosp.institution,hosp.state,hosp.hospital,
emp.brand_profile_c,emp.opportunity_c,emp.name,current_datetime()

FROM tds_analytics_partner_portal.hospital_wu_master hosp
JOIN
raw_b2c_sfdc.employment_c emp
ON
(hosp.walden_account_id =emp.bd_account_c
--OR hosp.walden_parent_id=emp.bd_account_c
OR hosp.walden_account_id=emp.parent_account_name_c
OR hosp.walden_parent_id=emp.parent_account_name_c
OR hosp.walden_account_name=emp.employer_name_c)
AND emp.opportunity_c is not null and emp.brand_profile_c is not null AND emp.is_deleted is false;

--*************************************************************************************
drop TABLE IF EXISTS tds_analytics_partner_portal.opportunity_wu_master;
create table tds_analytics_partner_portal.opportunity_wu_master as
select distinct id,contact_c,brand_profile_c,start_date_c,stage_name,disposition_c,primary_flag_c,banner_id_c,program_start_date_formula_c,application_submitted_c,Anticipated_Graduation_Date_c,anticipated_graduation_date_text_c,current_program_banner_code_c,most_recent_inquiry_date_c,app_started_date_time_c,program_of_interest_c,(select banner_program_code_c from raw_b2c_sfdc.product_2 where id=program_of_interest_c and institution_code_c='1' and trim(lower(status_c)) ='active') as banner_program_code_c,(select banner_degree_level_c from raw_b2c_sfdc.product_2 where id=program_of_interest_c and institution_code_c='1' and trim(lower(status_c)) ='active') as banner_degree_level_c,fiscal_year,
(select walden_alumni_c from raw_b2c_sfdc.contact where id=contact_c ) as is_alumni,(select walden_graduation_date_c from raw_b2c_sfdc.contact where id=contact_c) as graduation_year,'NULL' as application_active ,'NULL' as session_active

--(select true from rpt_academics.t_wldn_alumni where id=banner_id_c  and outcome_grad_year1>=2020 and graduation_flag1=1 and enrollment_flag1=1 ) as is_alumni,(select outcome_grad_year1 from rpt_academics.t_wldn_alumni where id=banner_id_c) as graduation_year

from raw_b2c_sfdc.opportunity o
where id in
((Select  opportunity_c from raw_b2c_sfdc.employment_c where (bd_account_c in (select walden_account_id from tds_analytics_partner_portal.hospital_wu_master) or parent_account_name_c in (select walden_account_id from tds_analytics_partner_portal.hospital_wu_master) or employer_name_c in (select walden_account_name from tds_analytics_partner_portal.hospital_wu_master)
or bd_account_c in (select walden_parent_id from tds_analytics_partner_portal.hospital_wu_master)or parent_account_name_c in (select walden_parent_id from tds_analytics_partner_portal.hospital_wu_master)
)

or  brand_profile_c in
(Select brand_profile_c from raw_b2c_sfdc.employment_c where (BD_Account_c in (
select walden_account_id from tds_analytics_partner_portal.hospital_wu_master) or Parent_Account_Name_c in (
select walden_account_id from tds_analytics_partner_portal.hospital_wu_master) or employer_name_c in (select walden_account_name from tds_analytics_partner_portal.hospital_wu_master)
or BD_Account_c in (select walden_parent_id from tds_analytics_partner_portal.hospital_wu_master)or Parent_Account_Name_c in (select walden_parent_id from tds_analytics_partner_portal.hospital_wu_master)
)))) and is_deleted is false;

----*******************************

---group by contact_c,banner_degree_level_c
---applying update statement for application_active = Y
update tds_analytics_partner_portal.opportunity_wu_master set application_active = 'Y' where id in (
select iep.id from tds_analytics_partner_portal.opportunity_wu_master iep
join (
select min(app_started_date_time_c) as app_started_date_time_c,contact_c,banner_degree_level_c  from tds_analytics_partner_portal.opportunity_wu_master iep where stage_name in ('Applicant')
and CAST(FORMAT_DATE('%Y',app_started_date_time_c) AS INT64) >= 2020
group by contact_c,banner_degree_level_c
)
iep_temp on iep_temp.app_started_date_time_c = iep.app_started_date_time_c and iep_temp.banner_degree_level_c = iep.banner_degree_level_c and iep_temp.contact_c = iep.contact_c
where stage_name in ('Applicant')
);

---applying update statement for application_date_c = null
update tds_analytics_partner_portal.opportunity_wu_master set app_started_date_time_c = null where id in (
select id from tds_analytics_partner_portal.opportunity_wu_master where stage_name in ('Applicant')
and CAST(FORMAT_DATE('%Y',app_started_date_time_c) AS INT64) >= 2020 and application_active is null) ;

-----
---students update statement for session_active = Y
update tds_analytics_partner_portal.opportunity_wu_master set session_active = 'Y' where id in (
select distinct iep.id from tds_analytics_partner_portal.opportunity_wu_master iep
join
(
select max(program_start_date_formula_c) as program_start_date_formula_c, contact_c,banner_degree_level_c  from tds_analytics_partner_portal.opportunity_wu_master where stage_name in  ('Pre-enroll','Paused','Pause','Student')
and CAST(FORMAT_DATE('%Y',program_start_date_formula_c) AS INT64) >= 2020
group by  contact_c,banner_degree_level_c
)
iep_temp on iep_temp.program_start_date_formula_c = iep.program_start_date_formula_c and iep_temp.banner_degree_level_c = iep.banner_degree_level_c and iep_temp.contact_c = iep.contact_c
where iep.stage_name in  ('Pre-enroll','Paused','Pause','Student')  ) ;

---students update statement for session_start_date_c = null
update tds_analytics_partner_portal.opportunity_wu_master set program_start_date_formula_c = null where id in (
select id from tds_analytics_partner_portal.opportunity_wu_master where stage_name in ('Pre-enroll','Paused','Pause','Student')
and CAST(FORMAT_DATE('%Y',program_start_date_formula_c) AS INT64) >= 2020 and session_active is null) ;

--***********************************************************************

delete from tds_analytics_partner_portal.program_wu_master where true;

INSERT INTO tds_analytics_partner_portal.program_wu_master(b2b_account_id,b2b_account_name,
walden_account_id,walden_account_name,b2b_parent_id,walden_parent_id,b2b_global_unique_id,walden_global_unique_id,institution,state,hospital,opportunity_id,contact_c,brand_profile_c,
emp_brand_profile_c,emp_opportunity_c,eid,start_date_c,stage_name,stage_type,stage_alumni,disposition_c,primary_flag_c,banner_id_c,program_start_date_formula_c,application_submitted_c,anticipated_graduation_date_c,anticipated_graduation_date_text_c,current_program_banner_code_c,most_recent_inquiry_date_c,app_started_date_time_c,program_of_interest_c,banner_program_code_c,program_group,location,fiscal_year,application_start_year,program_start_year,graduation_year,record_insert_date
)

select distinct hosp.b2b_account_id,hosp.b2b_account_name,
hosp.walden_account_id,hosp.walden_account_name,hosp.b2b_parent_id,hosp.walden_parent_id,hosp.b2b_global_unique_id,hosp.walden_global_unique_id,hosp.institution,hosp.state,hosp.hospital,op.id,op.contact_c,op.brand_profile_c,hosp.brand_profile_c,hosp.opportunity_c,hosp.eid,op.start_date_c,op.stage_name,
case when trim(op.stage_name) in ('Pre-Opportunity','Open','Active','Qualified') and op.primary_flag_c is true then 'Interested'
 When trim(op.stage_name) in ('Applicant') and op.primary_flag_c is true and app_started_date_time_c is not null then 'Applying'
 When trim(op.stage_name) in ('Pre-enroll','Paused','Pause','Student') and program_start_date_formula_c is not null then 'Students'
else op.stage_name
 end,
 case when op.is_alumni is true and op.graduation_year is not null then 'Alumni'
 else NULL
 end,
op.disposition_c,op.primary_flag_c,op.banner_id_c,op.program_start_date_formula_c,op.application_submitted_c,op.anticipated_graduation_date_c,op.anticipated_graduation_date_text_c,op.current_program_banner_code_c,op.most_recent_inquiry_date_c,op.app_started_date_time_c,op.program_of_interest_c,op.banner_program_code_c,
Case
     When trim(upper(op.banner_degree_level_c)) like 'BS' THEN 'Non-Clinical UG'
     When trim(upper(op.banner_degree_level_c)) like 'BSN' THEN 'RNBSN'
     When trim(upper(op.banner_degree_level_c)) like 'BSW' THEN 'BSW'
     When trim(upper(op.banner_degree_level_c)) like 'CERT' THEN 'Certificates'
     When trim(upper(op.banner_degree_level_c)) like 'DBA' THEN 'Non-Clinical Doctoral'
     When trim(upper(op.banner_degree_level_c)) like 'DHA' THEN 'Non-Clinical Doctoral'
     When trim(upper(op.banner_degree_level_c)) like 'DIT' THEN 'Non-Clinical Doctoral'
     When trim(upper(op.banner_degree_level_c)) like 'DNP' THEN 'DNP'
     When trim(upper(op.banner_degree_level_c)) like 'DOCT' THEN 'Non-Clinical Doctoral'
     When trim(upper(op.banner_degree_level_c)) like 'DPA' THEN 'Non-Clinical Doctoral'
     When trim(upper(op.banner_degree_level_c)) like 'DPSY' THEN 'Non-Clinical Doctoral'
     When trim(upper(op.banner_degree_level_c)) like 'DRPH' THEN 'Non-Clinical Doctoral'
     When trim(upper(op.banner_degree_level_c)) like 'DSW' THEN 'Non-Clinical Doctoral'
     When trim(upper(op.banner_degree_level_c)) like 'EDD' THEN 'Non-Clinical Doctoral'
     When trim(upper(op.banner_degree_level_c)) like 'EDS' THEN 'Non-Clinical Doctoral'
     When trim(upper(op.banner_degree_level_c)) like 'MAT' THEN 'Non-Clinical Masters'
     When trim(upper(op.banner_degree_level_c)) like 'MBA' THEN 'Non-Clinical Masters'
     When trim(upper(op.banner_degree_level_c)) like 'MBMSP' THEN 'Non-Clinical Masters'
     When trim(upper(op.banner_degree_level_c)) like 'MBPA' THEN 'Non-Clinical Masters'
     When trim(upper(op.banner_degree_level_c)) like 'MBPH' THEN 'Non-Clinical Masters'
     When trim(upper(op.banner_degree_level_c)) like 'MHA' THEN 'Non-Clinical Masters'
     When trim(upper(op.banner_degree_level_c)) like 'MISM' THEN 'Non-Clinical Masters'
     When trim(upper(op.banner_degree_level_c)) like 'MPA' THEN 'Non-Clinical Masters'
     When trim(upper(op.banner_degree_level_c)) like 'MPH' THEN 'MPH'
     When trim(upper(op.banner_degree_level_c)) like 'MPMSP' THEN 'Non-Clinical Masters'
     When trim(upper(op.banner_degree_level_c)) like 'MPP' THEN 'Non-Clinical Masters'
     When trim(upper(op.banner_degree_level_c)) like 'MPPH' THEN 'Non-Clinical Masters'
     When trim(upper(op.banner_degree_level_c)) like 'MS' THEN 'Non-Clinical Masters'
     When trim(upper(op.banner_degree_level_c)) like 'MSN' THEN 'MSN'
     When trim(upper(op.banner_degree_level_c)) like 'MSW' THEN 'MSW'
     When trim(upper(op.banner_degree_level_c)) like 'PHD' THEN 'Non-Clinical Doctoral'
Else 'OTHERS'
End,hosp.state,op.fiscal_year,
   CAST(FORMAT_DATE('%Y',op.app_started_date_time_c) AS INT64),
   CAST(FORMAT_DATE('%Y',op.program_start_date_formula_c) AS INT64),
   CAST(op.anticipated_graduation_date_text_c AS INT64),
current_datetime()

from
    tds_analytics_partner_portal.hospital_wu_employer hosp
JOIN
    tds_analytics_partner_portal.opportunity_wu_master op
ON (hosp.brand_profile_c=op.brand_profile_c or hosp.opportunity_c=op.id)
and trim(upper(op.banner_degree_level_c)) != '0' and trim(upper(op.banner_degree_level_c)) != 'ENDRS' and trim(upper(op.banner_degree_level_c)) != 'NDEG' and trim(upper(op.banner_degree_level_c)) != '' and trim(upper(op.banner_degree_level_c)) != 'LLM' and trim(upper(op.banner_degree_level_c)) != 'PGA' and trim(upper(op.banner_degree_level_c)) != 'PGC' and trim(upper(op.banner_degree_level_c)) != '00000' and op.banner_degree_level_c is not null and upper(op.banner_program_code_c) not like '%MS_INMT%' and trim(upper(op.banner_degree_level_c)) !='#N/A';

END