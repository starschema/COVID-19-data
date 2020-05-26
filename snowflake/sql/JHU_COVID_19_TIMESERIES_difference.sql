create or replace table JHU_COVID_19_TIMESERIES_0522 as
select COUNTRY_REGION,
       PROVINCE_STATE,
       COUNTY,
       FIPS,
       LAT,
    "LONG",
    ISO3166_1,
    ISO3166_2,
    DATE,
    CASES,
    CASE_TYPE,
    LAST_UPDATE_DATE,
    LAST_REPORTED_FLAG, NVL
(
    cases- lag
(
    cases
) OVER
(
    PARTITION
    BY
    COUNTRY_REGION,
    PROVINCE_STATE,
    COUNTY,
    case_type,
    fips
    ORDER
    BY
    DATE
),0) difference
    from COVID19.public.JHU_COVID_19_TIMESERIES
    where 1=1;


truncate table COVID19.public.JHU_COVID_19_TIMESERIES;

insert into COVID19.public.JHU_COVID_19_TIMESERIES
select *
from JHU_COVID_19_TIMESERIES_0522;
