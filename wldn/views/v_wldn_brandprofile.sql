create or replace view rpt_crm_mart.v_wldn_brandprofile as
SELECT sbp.*,
CASE WHEN  SS.ISO_Code_c in ('AA', 'AE', 'AP', 'PR', 'VI', 'GU', 'AS', 'FR','MP', 'UM','PW','FM','MH','CM','CZ','RQ') Then 1
	WHEN cast(Ifnull(SC.Name,SBP.Billing_Country_Text_c) AS string) Is Not Null and cast(Ifnull(SC.Name,SBP.Billing_Country_Text_c) as string) Not In ('us', 'u.s.','United States','usa','u.s.a.') Then 1
	When cast(Ifnull (SC.Name,SBP.Billing_Country_Text_c) as string) Is Null AND SBP.Billing_State_text_c  is not null  And (SS.Country_ISO_Code_c !='US'  OR  SS.Country_ISO_Code_c is null) Then 1
	ELSE 0 END As InternationalFlag
, CASE WHEN SC.ISO_Code_c = 'US' AND SS.Country_ISO_Code_c != 'US' THEN NULL  ELSE SBP.Billing_State_text_c END AS CleansedBillingState
 FROM `raw_b2c_sfdc.brand_profile_c` sbp
 LEFT join `raw_b2c_sfdc.country_c` SC
 on sbp.Billing_country_c = SC.ID
LEFT JOIN `raw_b2c_sfdc.state_c` SS
 on sbp.Billing_State_c = SS.ID
 where sbp.institution_c='a0ko0000002BSH4AAO'
 and sbp.is_deleted = False
