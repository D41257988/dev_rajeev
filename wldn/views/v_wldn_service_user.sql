select
   u.*except(_fivetran_synced,department,department_c),
   coalesce(u.department, u.department_c) as owner_department,
   m.name as manager_name,
   m.department as manager_department,
   m.division as manager_divistion,
   m.email as manager_email,
   d.name as director_name,
   d.department as director_department,
   d.division as director_division,
   d.email as director_email

from
 `raw_b2c_sfdc.user` u
 left join `raw_b2c_sfdc.user` m
 on upper(u.manager_id) = upper(m.id)
 left join `raw_b2c_sfdc.user` d
 on upper(u.director_c) = upper(d.id)
where
   ifnull(u.department, u.department_c) in
   (
      'SST-Financial Aid','Academic Advising','SST','Registrar','Bursar','Success Coaches',
'Student Experience','LOEBV Business Operations','Academic Operations','Content Operations','Career Services','Academic Residencies','Library Services',
'Student Affairs','Field Experience','GSFS','Student Support Team', 'Financial Aid' , 'Walden Student Affairs', 'Student Success','Military Services',
'Customer Care','Customer Care - Advising','Customer Care - Financial Services','Customer Care - Tech'
   ) and  u._fivetran_deleted = false
   and m._fivetran_deleted = false
   and d._fivetran_deleted = false
   and u.email not like '%invalid'
   and (lower(u.email) like '%walden%' OR lower(u.email) like '%laureate%' OR lower(u.email) like '%liverpool%' OR lower(u.email) like '%roehampton%')
   and lower(u.email) not like '%invalid'

