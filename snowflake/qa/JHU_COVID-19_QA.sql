with err (fips, date, err_count) AS
(
select count(*)
from COVID19.PUBLIC.JHU_COVID_19
having count(*) > 1
)
SELECT 
'JHU_COVID_19' as table_name
,'Duplicate data' as error_desc
,'select fips, date, count(*) from JHU_COVID_19 where JHU_COVID_19.ISO3166_1 = "US"  and CASE_TYPE = "Confirmed"  and JHU_COVID_19.County is not null  and JHU_COVID_19.fips is not nullgroup by 1, 2 having count(*) > 1' as error_condition
,sum(err_count) as error_count
FROM
err
group by 1,2,3
;
with err (fips, date, err_count) AS
(
select fips, date, count(*)
from COVID19.PUBLIC.JHU_COVID_19
where JHU_COVID_19.ISO3166_1 = 'US'
  and CASE_TYPE = 'Confirmed'
  and JHU_COVID_19.County is not null
  and JHU_COVID_19.fips is not null
group by 1, 2
having count(*) > 1
)
SELECT 
'JHU_COVID_19' as table_name
,'Duplicate data' as error_desc
,'select fips, date, count(*) from JHU_COVID_19 where JHU_COVID_19.ISO3166_1 = "US"  and CASE_TYPE = "Confirmed"  and JHU_COVID_19.County is not null  and JHU_COVID_19.fips is not nullgroup by 1, 2 having count(*) > 1' as error_condition
,sum(err_count) as error_count
FROM
err
group by 1,2,3
;
