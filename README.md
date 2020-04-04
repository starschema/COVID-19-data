# 2019 Novel Coronavirus (2019-nCoV) and COVID-19 Unpivoted Data

[![DOI](https://zenodo.org/badge/245742949.svg)](https://zenodo.org/badge/latestdoi/245742949) ![Test execution and deploy to DEV](https://github.com/starschema/COVID-19-data/workflows/Test%20execution%20and%20deploy%20to%20DEV/badge.svg) ![BSD 3-clause license](https://img.shields.io/badge/license-BSD--3-green)

This data set collates a growing number of critical indicators for assessment, monitoring and forecasting of the global COVID-19 situation. The data set is maintained by [Starschema](https://starschema.com), an international data services consultancy.

## Real-time data, easy to work with

A range of data sets have been published that are useful for monitoring and understanding the spread of COVID-19. Our efforts are intended to **collate, curate and unify** the most valuable data sources for enterprises, individuals and public health experts to assess the situation and make data-driven decisions. This single source easily blends with other data sources so you can analyze the movement of the SARS-CoV-2 pandemic over time, in any context.

## Data sets

Currently, the following data sets are included:

| Name | Source | Table name |
|------|--------|------------|
| US COVID-19 testing and mortality | [The COVID Tracking Project](https://covidtracking.com) | `CT_US_COVID_TESTS` |
| Global data on healthcare providers | OpenStreetMap, via [Healthsites.io](https://healthsites.io) | `HS_BULK_DATA` |
| Global case counts | [JHU CSSE](https://github.com/CSSEGISandData/COVID-19) | `JHU_COVID_19` |
| US healthcare capacity by state, 2018 | [The Henry J. Kaiser Family Foundation](https://www.kff.org/health-costs/issue-brief/state-data-and-policy-actions-to-address-coronavirus/) | `KFF_HCP_CAPACITY` |
| US policy actions by state | [The Henry J. Kaiser Family Foundation](https://www.kff.org/health-costs/issue-brief/state-data-and-policy-actions-to-address-coronavirus/) | `KFF_US_POLICY_ACTIONS` |
| US actions to mitigate spread, by state | [The Henry J. Kaiser Family Foundation](https://www.kff.org/health-costs/issue-brief/state-data-and-policy-actions-to-address-coronavirus/) | `KFF_US_STATE_MITIGATIONS` |
| ICU beds by county, US | [The Henry J. Kaiser Family Foundation](https://www.kff.org/health-costs/issue-brief/state-data-and-policy-actions-to-address-coronavirus/) | `KFF_US_ICU_BEDS` |
| Italy case statistics, summary | [Protezione Civile](https://github.com/pcm-dpc/COVID-19) | `PCM_DPS_COVID19` |
| Italy case statistics, detailed | [Protezione Civile](https://github.com/pcm-dpc/COVID-19) | `PCM_DPS_COVID19_DETAILS` |
| WHO situation reports | [World Health Organization](https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports) | `WHO_SITUATION_REPORTS` |
| US case and mortality counts, by county | [The New York Times](https://github.com/nytimes/covid-19-data) | `NYT_US_COVID19` |
| COVID-19 cases and deaths, Canada, province level | [ViriHealth](https://virihealth.com) | `VH_CAN_DETAILED` |
| Travel restrictions by country | [World Food Programme via HDX](https://data.humdata.org/dataset/covid-19-global-travel-restrictions-and-airline-information) | `HUM_RESTRICTIONS_COUNTRY` |
| Travel restrictions by airline | [World Food Programme via HDX](https://data.humdata.org/dataset/covid-19-global-travel-restrictions-and-airline-information) | `HUM_RESTRICTIONS_AIRLINE` |
| ACAPS public health restriction data | [ACAPS via HDX](https://data.humdata.org/dataset/acaps-covid19-government-measures-dataset) | `HDX_ACAPS` |
| Detailed case counts by province, sex and age band, Belgium | [Sciensano](https://www.sciensano.be/en) | `SCS_BE_DETAILED_PROVINCE_CASE_COUNTS` |
| Detailed hospitalisations by type of hospital care, Belgium | [Sciensano](https://www.sciensano.be/en) | `SCS_BE_DETAILED_HOSPITALISATIONS` |
| Detailed mortality by region, sex and age band, Belgium | [Sciensano](https://www.sciensano.be/en) | `SCS_BE_DETAILED_MORTALITY` |
| Number of tests performed by day, Belgium | [Sciensano](https://www.sciensano.be/en) | `SCS_BE_DETAILED_TESTS` |




## Technical details

### Conventions

By convention, we unify geographies to ISO-3166-1 and ISO-3166-2 alpha-2 identifiers. We use `pycountry`'s country definitions and mappings.

### Outputs

Raw data is available through a range of availabilities.

#### Snowflake Data Exchange

The COVID-19 data set is available on [Snowflake Data Exchange](https://www.snowflake.com/datasets/starschema/). This data set is continuously refreshed.

You can use the `METADATA` table for metadata about each table, on a column level. Where the column is not specified, information pertains to the entire table.

#### S3 raw CSVs

Raw CSV files are available on AWS S3:

| Name | Source | Table name |
|------|--------|------------|
| US COVID-19 testing and mortality | [The COVID Tracking Project](https://covidtracking.com) | [`s3://starschema.covid/CT_US_COVID_TESTS.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/CT_US_COVID_TESTS.csv) |
| Global data on healthcare providers | OpenStreetMap, via [Healthsites.io](https://healthsites.io) | [`s3://starschema.covid/HS_BULK_DATA.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/HS_BULK_DATA.csv) |
| Global case counts | [JHU CSSE](https://github.com/CSSEGISandData/COVID-19) | [`s3://starschema.covid/JHU_COVID-19.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/JHU_COVID-19.csv) |
| US healthcare capacity by state, 2018 | [The Henry J. Kaiser Family Foundation](https://www.kff.org/health-costs/issue-brief/state-data-and-policy-actions-to-address-coronavirus/) | [`s3://starschema.covid/KFF_HCP_capacity.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/KFF_HCP_capacity.csv) |
| US policy actions by state | [The Henry J. Kaiser Family Foundation](https://www.kff.org/health-costs/issue-brief/state-data-and-policy-actions-to-address-coronavirus/) | [`s3://starschema.covid/KFF_US_POLICY_ACTIONS.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/KFF_US_POLICY_ACTIONS.csv) |
| US actions to mitigate spread, by state | [The Henry J. Kaiser Family Foundation](https://www.kff.org/health-costs/issue-brief/state-data-and-policy-actions-to-address-coronavirus/) | [`s3://starschema.covid/KFF_US_STATE_MITIGATIONS.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/KFF_US_STATE_MITIGATIONS.csv) |
| ICU beds by county, US | [The Henry J. Kaiser Family Foundation](https://www.kff.org/health-costs/issue-brief/state-data-and-policy-actions-to-address-coronavirus/) | [`s3://starschema.covid/KFF_US_ICU_BEDS.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/KFF_US_ICU_BEDS.csv) |
| Italy case statistics, summary | [Protezione Civile](https://github.com/pcm-dpc/COVID-19) | [`s3://starschema.covid/PCM_DPS_COVID19.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/PCM_DPS_COVID19.csv) |
| Italy case statistics, detailed | [Protezione Civile](https://github.com/pcm-dpc/COVID-19) | [`s3://starschema.covid/PCM_DPS_COVID19-DETAILS.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/PCM_DPS_COVID19-DETAILS.csv) |
| WHO situation reports | [World Health Organization](https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports) | [`s3://starschema.covid/WHO_SITUATION_REPORTS.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/WHO_SITUATION_REPORTS.csv) |
| US case and mortality counts, by county | [The New York Times](https://github.com/nytimes/covid-19-data) | [`s3://starschema.covid/NYT_US_COVID19.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/NYT_US_COVID19.csv) |
| COVID-19 cases and deaths, Canada, province level | [ViriHealth](https://virihealth.com) | [`s3://starschema.covid/VH_CAN_DETAILED.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/VH_CAN_DETAILED.csv) |
| Travel restrictions by country | [World Food Programme via HDX](https://data.humdata.org/dataset/covid-19-global-travel-restrictions-and-airline-information) | [`s3://starschema.covid/HUM_RESTRICTIONS_COUNTRY`](https://s3-us-west-1.amazonaws.com/starschema.covid/HUM_RESTRICTIONS_COUNTRY.csv) |
| Travel restrictions by airline | [World Food Programme via HDX](https://data.humdata.org/dataset/covid-19-global-travel-restrictions-and-airline-information) | [`s3://starschema.covid/HUM_RESTRICTIONS_AIRLINE`](https://s3-us-west-1.amazonaws.com/starschema.covid/HUM_RESTRICTIONS_AIRLINE.csv) |
| ACAPS public health restriction data | [ACAPS via HDX](https://data.humdata.org/dataset/covid-19-global-travel-restrictions-and-airline-information) | [`s3://starschema.covid/HDX_ACAPS.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/HDX_ACAPS.csv) |
| Detailed case counts by province, sex and age band, Belgium | [Sciensano](https://www.sciensano.be/en) | [`s3://starschema.covid/SCS_BE_DETAILED_PROVINCE_CASE_COUNTS.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/SCS_BE_DETAILED_PROVINCE_CASE_COUNTS.csv) |
| Detailed hospitalisations by type of hospital care, Belgium | [Sciensano](https://www.sciensano.be/en) | [`s3://starschema.covid/SCS_BE_DETAILED_HOSPITALISATIONS.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/SCS_BE_DETAILED_HOSPITALISATIONS.csv) |
| Detailed mortality by region, sex and age band, Belgium | [Sciensano](https://www.sciensano.be/en) | [`s3://starschema.covid/SCS_BE_DETAILED_MORTALITY.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/SCS_BE_DETAILED_MORTALITY.csv) |
| Number of tests performed by day, Belgium | [Sciensano](https://www.sciensano.be/en) | [`s3://starschema.covid/SCS_BE_DETAILED_TESTS.csv`](https://s3-us-west-1.amazonaws.com/starschema.covid/SCS_BE_DETAILED_TESTS.csv) |



#### Tableau Web Data Connector

There is a [Tableau Web Data Connector](https://starschema-extensions.s3.amazonaws.com/covid-tableau-online-wdc/index.html) available for your use in Tableau to integrate the COVID-19 data set into your dashboards and analytical applications. Currently, this supports the JHU CSSE data set and the Italian case counts released by the Dipartimento delle Protezione Civile. The reach of the WDC is currently being expanded, please check back for details.


### Transformations

All applied transformation sets are documented in the `Jupyter` notebooks in the `notebooks/` folder.

## Credits

The original data flow was designed by Allan Walker for Mapbox in Alteryx. 

## Use and disclaimer

**Use of this data source is subject to your implied acceptance of the following terms.**

Data and transformations are provided 'as is', *without any warranty or representation, express or implied, of correctness, usefulness or fitness to purpose*. Starschema Inc. and its contributors disclaim all representations and warranties of any kind with respect to the data or code in this repository to the fullest extent permitted under applicable law.

The 2019 novel coronavirus (2019-nCoV)/COVID-19 outbreak is a rapidly evolving situation. Data may be out of date or incorrect due to reporting constraints. Before making healthcare or other personal decisions, please consult a physician licensed to practice in your jurisdiction and/or the website of the public health authorities in your jurisdiction, such as the [CDC](https://www.cdc.gov/coronavirus/2019-ncov/index.html), [Public Health England](https://www.gov.uk/government/collections/coronavirus-covid-19-list-of-guidance) or [Public Health Canada](https://www.canada.ca/en/public-health/services/diseases/2019-novel-coronavirus-infection.html). Nothing in this repository is to be construed as medical advice.

## Citation

To cite this work:

> Foldi, T. and Csefalvay, K. _2019 Novel Coronavirus (2019-nCoV) and COVID-19 Unpivoted Data._ Available on: `https://github.com/starschema/COVID-19-data`.
