# 2019 Novel Coronavirus COVID-19 (2019-nCoV) Unpivoted Data

The following script takes data from the repository of the 2019 Novel Coronavirus Visual Dashboard operated by the Johns Hopkins University Center for Systems Science and Engineering (JHU CSSE). It will apply necessary cleansing/reformatting to make it use in traditional relational databases and data visualization tools.

More information about the dataset with example Tableau Public Dashboards: https://www.tableau.com/covid-19-coronavirus-data-resources


## Real-time data, easy to work with

Johns Hopkins University has taken the lead in compiling data in real-time and making it available to the public. Now though this JHU Coronavirus Data Stream we’ve made that real-time data easy to access and analyze alongside other data sources as it updates.

With the help of several current and former Tableau Zen Masters, we are making this data available in a standardized format with an automated refresh. This single source easily blends with other data sources so you can analyze the movement of the disease over time, in any context.

## Technical details

The JHU Coronavirus Data Stream includes reported cases at the province level in China, country/province/state level in the US, Australia and Canada, and at the country level otherwise. Drawing from the JHU CSSE github repository, this cleaned (ISO-8601 date format), unioned & unpivoted dataset is updated hourly. Full information on the data sources available: https://systems.jhu.edu/research/public-health/ncov/.


### Output File

The location of the populated google sheet file is: https://docs.google.com/spreadsheets/d/1avGWWl1J19O_Zm0NGTGy2E-fOG05i4ljRfjl87P7FiA/edit?usp=sharing

The data updated at least once per a day.

### Transformations

All applied transformation sets are documented in the `Jupyter` notebook: https://github.com/starschema/COVID-19-data/blob/master/JH_COVID-19.ipynb

## Credits

The original data flow was designed by Allan Walker for Mapbox in Alteryx. 
