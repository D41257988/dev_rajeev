BEGIN
-- declare constants
declare curr_timestamp timestamp;
declare audit_key string;
DECLARE Enroll_Specialist, Rank_uid string;
SET curr_timestamp = current_timestamp();
set audit_key = upper(replace(generate_uuid(), "-", ""));
SET Enroll_Specialist = (Select id from `raw_b2c_sfdc.user` where name = 'Walden University' and current_user_profile_c = 'Enrollment Specialist' );
SET Rank_uid = '0051N000006wBJUQA2';
INSERT INTO `rpt_student_cdm.prc_l1_opportunity_cl_score_tile_update`
Select opp.id, 'false' as new_opp_button_c
, CASE WHEN cl.scoring_model_name = 'Closed Lost Score' AND cl.score is not null AND cl.tile is not null THEN Rank_uid
    WHEN cl.scoring_model_name = 'Closed Lost Score' AND cl.score is null AND cl.tile is null AND u.is_active = true
    THEN u.id ELSE Enroll_Specialist END AS OwnerId
, CASE WHEN cl.scoring_model_name = 'Closed Lost Score' AND cl.score is not null AND cl.tile is not null THEN opp.owner_id
    WHEN cl.scoring_model_name = 'Closed Lost Score' AND cl.score is null AND cl.tile is null THEN opp.previous_opportunity_ownerid_c
    END as previous_opportunity_ownerid_c
, CASE WHEN cl.scoring_model_name = 'Closed Lost Score' THEN cl.score END as bi_closed_score_c
, CASE WHEN cl.scoring_model_name = 'Closed Lost Score' THEN cl.tile END as bi_closed_tile_c
, false                                  as processed_flag
, curr_timestamp                         as etl_created_date
, curr_timestamp                         as etl_updated_date
, audit_key                              as etl_ins_audit_key
, audit_key                              as etl_upd_audit_key
,'usp_prc_l1_opportunity_cl_score_tile_update'      as etl_resource_name
from `daas-ml-prod.wu_model_cdm.lead_closed_lost_score_latest` cl
JOIN `raw_b2c_sfdc.opportunity` opp
ON opp.id = cl.sf_id and cl.scoring_model_name = 'Closed Lost Score'
LEFT JOIN `raw_b2c_sfdc.user` u
ON u.id = opp.previous_opportunity_ownerid_c
;
END