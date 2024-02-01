create or replace view rpt_crm_mart.v_wldn_apps_one_element_missing as
with temp_opp as
(select opp.id,st.start_date_c,opp.stage_name,opp.institution_c
from `raw_b2c_sfdc.opportunity` opp
left join   `raw_b2c_sfdc.start_date_c`  st
on opp.selected_program_start_date_c = st.id and st.is_deleted=false and opp.institution_c='a0ko0000002BSH4AAO'
where st.start_date_c is not null
and opp.is_deleted=false),---1450877


All_Apps as
(
  select distinct a.related_opportunity_c
from raw_b2c_sfdc.application_element_c a
left join temp_opp b on a.related_opportunity_c=b.id
where b.institution_c='a0ko0000002BSH4AAO'
and b.start_date_c>=CURRENT_DATE()-14
and b.start_date_c<=CURRENT_DATE()+35
and b.stage_name='Applicant'
and a.is_deleted=false),

Total_NonTran_NotSubmitted as
(select distinct a.related_opportunity_c, count(*) as Total_NonTrans_Not_Submitted
from raw_b2c_sfdc.application_element_c a
left join temp_opp b on a.related_opportunity_c=b.id
where  b.institution_c='a0ko0000002BSH4AAO'
and b.start_date_c>=CURRENT_DATE()-14
and b.start_date_c<=CURRENT_DATE()+35
and b.stage_name='Applicant'
and a.status_c='Not Submitted'
and a.requirement_name_c not like '%Transcript%'
and a.is_deleted=false
group by a.related_opportunity_c),

Total_NonTran_NonNurs_NotSubmitted as
(select distinct a.related_opportunity_c, count(*) as Total_NonTrans_NonNurs_Not_Submitted
from raw_b2c_sfdc.application_element_c a
left join temp_opp b on a.related_opportunity_c=b.id
where  b.institution_c='a0ko0000002BSH4AAO'
and b.start_date_c>=CURRENT_DATE()-14
and b.start_date_c<=CURRENT_DATE()+35
and b.stage_name='Applicant'
and a.status_c='Not Submitted'
and a.requirement_name_c not like '%Transcript%'
and a.requirement_name_c not in ('Clinical Site Identification Form','Walden University School of Nursing Technical Standards Policy')
and a.is_deleted=false
group by a.related_opportunity_c),

Transcript_data as
(select distinct a.related_opportunity_c,a.requirement_name_c,a.status_c,
case when  a.status_c in ('Submitted','Verified') then 1 else 0 end as Trans_Submitted,
case when  a.status_c in ('Not Submitted') then 1 else 0 end as Trans_NotSubmitted,
case when  a.status_c in ('Not Required') then 1 else 0 end as Trans_NotRequired
from raw_b2c_sfdc.application_element_c a
left join temp_opp b on a.related_opportunity_c=b.id
where  b.institution_c='a0ko0000002BSH4AAO'
and b.start_date_c>=CURRENT_DATE()-14
and b.start_date_c<=CURRENT_DATE()+35
and b.stage_name='Applicant'
and a.requirement_name_c like '%Transcript%'
and a.is_deleted=false),

Transcript_Cnt as
(select distinct related_opportunity_c,
sum(Trans_Submitted) as Trans_Submitted,
sum(Trans_NotSubmitted) as Trans_NotSubmitted,
sum(Trans_NotRequired) as Trans_NotRequired
from Transcript_data
group by related_opportunity_c),


/**Pull status of 2 Nursing Forms for applicants-Only need one for Admission*/
NursForm_data as
(select distinct a.related_opportunity_c,
--a.requirement_name_c,
a.status_c,
case when  a.status_c in ('Submitted','Verified') then 1 else 0 end as NursForm_Submitted,
case when  a.status_c in ('Not Submitted') then 1 else 0 end as NursForm_NotSubmitted,
case when  a.status_c in ('Not Required') then 1 else 0 end as NursForm_NotRequired
from raw_b2c_sfdc.application_element_c a --------------(table missing in b2c sfdc)
left join temp_opp b on a.related_opportunity_c=b.id
where  b.institution_c='a0ko0000002BSH4AAO'
and b.start_date_c>=CURRENT_DATE()-14
and b.start_date_c<=CURRENT_DATE()+35
and b.stage_name='Applicant'
and a.requirement_name_c in ('Clinical Site Identification Form','Walden University School of Nursing Technical Standards Policy')
and a.is_deleted=false),

NursForm_Cnt as
(select distinct related_opportunity_c,
sum(NursForm_Submitted) as NursForm_Submitted,
sum(NursForm_NotSubmitted) as NursForm_NotSubmitted,
sum(NursForm_NotRequired) as NursForm_NotRequired
from NursForm_data
group by related_opportunity_c),


/**Merge all Applicants with count of Not Submitted Requirements and status of Transcripts**/
Total_Elements_Cnt as
(select distinct a.*,e.Total_NonTrans_NonNurs_Not_Submitted,c.Total_NonTrans_Not_Submitted,b.Trans_Submitted,
b.Trans_NotSubmitted,b.Trans_NotRequired,
d.NursForm_Submitted,d.NursForm_NotSubmitted,d.NursForm_NotRequired
from All_Apps a
left join Transcript_Cnt b on a.related_opportunity_c=b.related_opportunity_c
left join Total_NonTran_NotSubmitted c on a.related_opportunity_c=c.related_opportunity_c
left join NursForm_Cnt d on a.related_opportunity_c=d.related_opportunity_c
left join Total_NonTran_NonNurs_NotSubmitted e on a.related_opportunity_c=e.related_opportunity_c),


/*Determine who is missing only 1 requirement */
Input as
(select *,
case
  when Total_NonTrans_NonNurs_Not_Submitted>=2 then '>1 Missing'
  when Total_NonTrans_Not_Submitted is null and Trans_Submitted>=1 then '0 Missing'
  when Total_NonTrans_Not_Submitted is null and Trans_Submitted is null then '0 Missing'
  when Total_NonTrans_Not_Submitted=1 and Trans_Submitted>=1 and NursForm_Submitted=1 and NursForm_NotSubmitted=1 then '0 Missing'

  when Total_NonTrans_Not_Submitted=1 and Trans_Submitted>=1 and NursForm_Submitted is null then '1 Non Transcript Missing'
  when Total_NonTrans_Not_Submitted=1 and Trans_Submitted>=1 and NursForm_Submitted<>NursForm_NotSubmitted then '1 Non Transcript Missing'
  when Total_NonTrans_Not_Submitted=1 and Trans_Submitted is null then '1 Non Transcript Missing'
  when Total_NonTrans_Not_Submitted=2 and Trans_Submitted>=1 and NursForm_NotSubmitted=2 then '1 Non Transcript Missing' /*Trans submitted - 2 Nursin Forms not submitted-only need 1*/
  when Total_NonTrans_Not_Submitted=2 and Trans_Submitted>=1 and NursForm_Submitted=1 and NursForm_NotSubmitted=1 then '1 Non Transcript Missing'

  when Total_NonTrans_Not_Submitted is null and Trans_Submitted=0 then '1 Transcript Missing'
  when Total_NonTrans_Not_Submitted=1 and Trans_Submitted=0 and NursForm_Submitted=1 and NursForm_NotSubmitted=1  then '1 Transcript Missing'

  when Total_NonTrans_Not_Submitted=1 and Trans_Submitted=0 and NursForm_Submitted<> NursForm_NotSubmitted  then '>1 Missing'
  when Total_NonTrans_Not_Submitted=1 and Trans_Submitted=0 and NursForm_Submitted is null  then '>1 Missing'
  when Total_NonTrans_Not_Submitted=2 and Trans_Submitted=0 then '>1 Missing'
  when Total_NonTrans_Not_Submitted=2 and Trans_Submitted>=1 and NursForm_Submitted is null then '>1 Missing'
  when Total_NonTrans_Not_Submitted=2 and Trans_Submitted>=1 and NursForm_Submitted=0 and NursForm_NotSubmitted=1 then '>1 Missing'
 else 'Other' end as app_group
from Total_Elements_Cnt
where
(Total_NonTrans_NonNurs_Not_Submitted is null or Total_NonTrans_NonNurs_Not_Submitted<2) and
(Total_NonTrans_Not_Submitted is null or Total_NonTrans_Not_Submitted<3)),

output as
(select distinct i.*,
a.related_opportunity_c as opp_sfid,b.brand_profile_c as brand_profile_sfid,d.banner_id_c as banner_id,
b.stage_name as Current_Stage,b.disposition_c as Current_Disposition,
a.last_name_c as Last_Name,a.first_name_c as First_Name,
b.Name as Opportunity_Name,
r.customer_friendly_poi_name as Program_Name,
r.level_description as Degree_Level,
st.start_date_c as Program_Start_Date,
b.ea_assigned_c,
b.last_ea_outreach_attempt_c,b.last_ea_two_way_contact_c,
e1.name as EA_Manager, e1.office_location_c,
case when d.preferred_email_c like '%Personal%' then d.personal_email_c
when d.preferred_email_c like '%University%' then d.university_email_c
when d.preferred_email_c like '%Alternate%' or d.preferred_email_c like '%Alternative%' then d.alternate_email_c
else NULL end as preferred_email_c,
a.requirement_name_c as Requirement,a.status_c as Status,
case when a.status_c='Not Submitted' then 1 else 0 end as Not_Submitted,
case when a.status_c='Not Submitted' then 'Y' else 'N' end as Not_Submitted_Flag
from Input i
left join raw_b2c_sfdc.application_element_c a on i.related_opportunity_c=a.related_opportunity_c and a.is_deleted=false
left join  raw_b2c_sfdc.opportunity b on a.related_opportunity_c=b.id and b.is_deleted=false and b.institution_c='a0ko0000002BSH4AAO'
left join raw_b2c_sfdc.start_date_c st on st.id=b.selected_program_start_date_c
left join raw_b2c_sfdc.brand_profile_c d on b.brand_profile_c=d.Id and d.is_deleted=false
left join raw_b2c_sfdc.user e on b.owner_id=e.id
left join raw_b2c_sfdc.user e1 on e1.id=e.manager_id
left join rpt_academics.t_wldn_product_map r on b.program_of_interest_c = r.product_sfid
where b.institution_c='a0ko0000002BSH4AAO'
and b.stage_name='Applicant'
and i.app_group in ('1 Non Transcript Missing','1 Transcript Missing')
),


/**List of Applicants with only 1 requirement not Submitted -
only need to have official or unofficial transcripts submitted - not both */
Output_deduped as
(select *,
case
when app_group='1 Non Transcript Missing' and Requirement like '%Transcript%'
then 'Exclude'
when app_group='1 Transcript Missing' and Trans_NotSubmitted=2
and Requirement like 'Official Transcript%'
 then 'Exclude'
when app_group='1 Non Transcript Missing' and NursForm_NotSubmitted=2
and Requirement='Clinical Site Identification Form'
then 'Exclude'
when app_group='1 Non Transcript Missing' and NursForm_NotSubmitted>=1 and NursForm_Submitted>=1
and Requirement in ('Walden University School of Nursing Technical Standards Policy','Clinical Site Identification Form')
 then 'Exclude'

when app_group='1 Transcript Missing' and NursForm_NotSubmitted=1
and Requirement in ('Walden University School of Nursing Technical Standards Policy','Clinical Site Identification Form')
then 'Exclude'

else 'Include' end as De_Dupe
from output
where  Not_Submitted_Flag='Y'),

FINAL_OUTPUT as
(select distinct
5 as institution_id,'WLDN' as institution,'WLDN_BNR' as source_system_name, related_opportunity_c,app_group,opp_sfid,brand_profile_sfid,banner_id,Current_Stage,
Current_Disposition,Last_Name,First_Name,Opportunity_Name,Program_Name,Degree_Level,
Program_Start_Date,last_ea_outreach_attempt_c,last_ea_two_way_contact_c,
ea_assigned_c,EA_Manager,office_location_c,preferred_email_c,Requirement,Status, CURRENT_DATE() as Data_Date
from Output_deduped  where De_Dupe='Include'
and Current_Disposition<>'Admitted')
select * from FINAL_OUTPUT
