CREATE TABLE IF NOT EXISTS covid19.public.demographics
(
  iso3166_1 TEXT
  ,iso3166_2 TEXT
  ,fips TEXT
  ,latitude FLOAT
  ,longitude FLOAT
  ,state TEXT
  ,county TEXT
  ,total_population NUMBER
  ,total_male_population NUMBER
  ,total_female_population NUMBER
);

INSERT INTO covid19.public.demographics
WITH 
meta (row_rnk, fips, latitude, longitude, state, county) AS
(  
  SELECT 
    ROW_NUMBER() OVER (PARTITION BY mf.state, mf.county ORDER BY longitude DESC) AS row_rnk
    ,SUBSTRING(mg.census_block_group,1,5) AS fips
    ,mg.latitude
    ,mg.longitude
    ,mf.state
    ,mf.county
  FROM
    safegraph_uscensus_and_neighborhood.public.metadata_cbg_geographic_data mg
    INNER JOIN safegraph_uscensus_and_neighborhood.public.metadata_cbg_fips_codes mf
      ON SUBSTRING(mg.census_block_group,1,5) = LPAD(mf.state_fips,2,'0') || LPAD(mf.county_fips,3,'0')
), 
pop (fips, total_population, total_male_population, total_female_population ) AS
(
  SELECT
    SUBSTRING(cbg,1,5) as fips
    ,SUM(total_population) as total_population
    ,SUM(total_male_population) as total_male_population
    ,SUM(total_female_population) as total_female_population
  FROM
    safegraph_uscensus_and_neighborhood.public.us_population_by_sex
  GROUP BY 
    SUBSTRING(cbg,1,5)
  ),
iso (fips, iso3166_1, iso3166_2) AS
(
  SELECT DISTINCT
    fips
    ,iso3166_1
    ,iso3166_2
  FROM
    covid19.public.jhu_covid_19_country
)

SELECT
  iso.iso3166_1
  ,iso.iso3166_2
  ,meta.fips
  ,meta.LATITUDE
  ,meta.LONGITUDE
  ,meta.state
  ,meta.county
  ,pop.total_population
  ,pop.total_male_population
  ,pop.total_female_population
FROM  
  iso 
  INNER JOIN meta
    ON meta.fips = iso.fips
  INNER JOIN pop
    ON pop.fips = meta.fips
    and meta.row_rnk = 1
;
