create or replace view rpt_crm_mart.v_wldn_all_user_manager as
with CTE
as
	(	select  source , id as user_sfid , ManagerId, name , Manager_Name ,Etl_Process_Dt ,lastmodifieddate ,createddate, Department , Division ,isactive , title ,email
			,row_number() over (partition by id, ManagerId ,Division,Department order by lastmodifieddate ) rn
		from
			(


                SELECT
                    distinct 'snapshot' source ,
                        u.id ,
                        u.Manager_Id as ManagerId,
                        u.name ,
                        um.name as Manager_Name ,
                        cast(u.last_modified_date as date) as Etl_Process_Dt ,
                        u.last_modified_date as lastmodifieddate ,
                        u.created_date as createddate,
                        u.Department ,
                        u.Division ,
                        cast(u.is_active as string) as isactive ,
                        u.title ,
                        u.email
                FROM `raw_b2c_sfdc.user` u
                left join raw_b2c_sfdc.user um on u.manager_id = um.id
                where u._fivetran_deleted is False
                
            ) 
    )
Select  A.*,
        IFNULL(CAST(DIMBegin.date_key  AS STRING),'99999999') AS BeginDtDIMKey,
        IFNULL(REPLACE(CAST(LEAD(cast(lastmodifieddate as date)) OVER (PARTITION BY user_sfid ORDER BY lastmodifieddate ASC)-1 AS STRING),'-',''),'99999999') AS EndDtDIMKey
from CTE A
JOIN mdm.dim_date DIMBegin 
ON CAST(DIMBegin.cal_dt AS DATE) = CASE WHEN lastmodifieddate = createddate THEN CAST(createddate AS DATE) ELSE CAST(lastmodifieddate AS DATE) END 
where rn = 1
