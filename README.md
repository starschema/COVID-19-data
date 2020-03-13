# 2019 Novel Coronavirus (2019-nCoV) and COVID-19 Unpivoted Data

[![DOI](https://zenodo.org/badge/245742949.svg)](https://zenodo.org/badge/latestdoi/245742949) ![Test execution and deploy to DEV](https://github.com/starschema/COVID-19-data/workflows/Test%20execution%20and%20deploy%20to%20DEV/badge.svg) ![BSD 3-clause license](https://img.shields.io/badge/license-BSD--3-green)

The following script takes data from the repository of the 2019 Novel Coronavirus Visual Dashboard operated by the Johns Hopkins University Center for Systems Science and Engineering (JHU CSSE). It will apply necessary cleansing/reformatting to make it use in traditional relational databases and data visualization tools.

More information about the dataset with example Tableau Public Dashboards: https://www.tableau.com/covid-19-coronavirus-data-resources


## Real-time data, easy to work with

Johns Hopkins University has taken the lead in compiling data in real-time and making it available to the public. Now through this JHU Coronavirus Data Stream we’ve made that real-time data easy to access and analyze alongside other data sources as it updates.

As a contribution by [Starschema](https://starschema.com) to global data-driven efforts to combat COVID-19, we are making this data available in a standardized format with an automated refresh. This single source easily blends with other data sources so you can analyze the movement of the disease over time, in any context.

## Technical details

The JHU Coronavirus Data Stream includes reported cases at the province level in China, country/province/state level in the US, Australia and Canada, and at the country level otherwise. Drawing from the JHU CSSE github repository, this cleaned (ISO-8601 date format), unioned & unpivoted dataset is updated hourly. Full information on the data sources available: https://systems.jhu.edu/research/public-health/ncov/.


### Output File

The location of the populated google sheet file is: https://docs.google.com/spreadsheets/d/1avGWWl1J19O_Zm0NGTGy2E-fOG05i4ljRfjl87P7FiA/edit?usp=sharing

The data updated at least once per a day.

### Transformations

All applied transformation sets are documented in the `Jupyter` notebook: https://github.com/starschema/COVID-19-data/blob/master/JH_COVID-19.ipynb

## Credits

The original data flow was designed by Allan Walker for Mapbox in Alteryx. 

## Use and disclaimer

**Use of this data source is subject to your implied acceptance of the following terms.**

Data and transformations are provided 'as is', *without any warranty or representation, express or implied, of correctness, usefulness or fitness to purpose*. Starschema Inc. and its contributors disclaim all representations and warranties of any kind with respect to the data or code in this repository to the fullest extent permitted under applicable law.

The 2019 novel coronavirus (2019-nCoV)/COVID-19 outbreak is a rapidly evolving situation. Data may be out of date or incorrect due to reporting constraints. Before making healthcare or other personal decisions, please consult a physician licensed to practice in your jurisdiction and/or the website of the public health authorities in your jurisdiction, such as the [CDC](https://www.cdc.gov/coronavirus/2019-ncov/index.html), [Public Health England](https://www.gov.uk/government/collections/coronavirus-covid-19-list-of-guidance) or [Public Health Canada](https://www.canada.ca/en/public-health/services/diseases/2019-novel-coronavirus-infection.html). Nothing in this repository is to be construed as medical advice.

## Citation

To cite this work:

> Foldi, T. and Csefalvay, K. _2019 Novel Coronavirus (2019-nCoV) and COVID-19 Unpivoted Data._ Available on: `https://github.com/starschema/COVID-19-data`.
