create or replace view rpt_crm_mart.v_wldn_ns_target_grid as
select *except(start_date),PARSE_DATE('%m/%d/%Y',start_date) as start_date FROM `raw_wldn_manualfiles.walden_ns_target_grid` ns
where _fivetran_synced = (select max(_fivetran_synced) from `raw_wldn_manualfiles.walden_ns_target_grid` ns1
                          where ns.start_date = ns1.start_date
                          and ns.budget_code = ns1.budget_code
                          and ns.domestic_flag = ns1.domestic_flag
                          and ns.modality=ns1.modality)