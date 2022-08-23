# FDRS Imputing Missing Value

## Introduction

This repository centralizes the imputation methodology used by the Federation-wide databank and reporting system (FDRS) team to address missing data. [FDRS](https://data.ifrc.org/FDRS/) is an IFRC platform dedicated to providing insights on the Red Cross Red Crescent National Societies. The data is gathered through a yearly data collection from 192 National Societies. 

The FDRS is ambitious and wide reaching. Although the data quality and reporting are improving each year, data is missing for some National Societies. As a result, some data fluctuations may be misleading: trend lines can drop for a given year when there is missing data, and some National Societies are excluded from the total and then appear again in another year.In order to better represent the network and better count everyone, FDRS implement every year data imputation techniques. 

The purpose of this notebook is to apply the method selected by the FDRS team and to interact with the FDRS backoffice with a post method to publish the imputed values. 
The ingested data is  replicated and displayed on the website https://data.ifrc.org/FDRS/ and used in FDRS research such as [Everyone count report](https://data-api.ifrc.org/documents/noiso/Everyone%20Counts%20Report%202022%20EN.pdf). 

The approach chosen was to replace the 2019, 2020 and 2021 missing data as well as to apply two different techniques according to the indicator categories, in the previous years all NSs reported their data then no input technique was employed.   

The imputing applies only to main indicators and does not apply to disaggregated levels to maintain consistency across years. A detailed description of the FDRS indicators is available in the excel [codebook](https://github.com/FDRS-ifrc/fdrs_imputing_missing_values/blob/main/references/codebook.xlsx). Naming convention: Imputed variables have the suffix "_IP". 

## Description of the methodology
For every combination of one National Society and one main indicator of FDRS sections NS Governance & Structure and NS Finance & Partnerships:  
If a value is missing for a year between 2019 and 2020, but there is at least one non-missing value in a later or an earlier year from 2018, replace the missing value:

- Looking at the years before this one, propagate last non-missing observation forward to next observation.
- If all the values for every year between 2019 and 2020 are missing, ignore this National Society for this indicator.

For every combination of one National Society and one main indicator of FDRS section NS Reach:  
If a value is missing for a year between 2019 and 2020, but there is at least one non-missing value in a later or an earlier year from 2018, replace the missing value:

- Looking at the years before this one, returns the mean of the non-missing values among these previous years.
- If all the values for every year between 2019 and 2020 are missing, ignore this National Society for this indicator.


### Examples

| Example: Number of local units, Nolandia National Society 	                                    | 2018    	| 2019    	| 2020    	|
|-----------------------------------------------------------------------------------------------	|---------	|---------	|---------	|
| Local Units                                                                                    	| 150    	| Missing 	| Missing 	|
| Missing value is replaced like this                                                           	| 2018    	| 2019    	| 2020    	|
| Local Units                                                                                    	| 150    	| 150   	| 150    	|

| Example: People reached by WASH activities, Nolandia National Society 	                        | 2018    	| 2019    	| 2020    	|
|-----------------------------------------------------------------------------------------------	|---------	|---------	|---------	|
| People Reached by WASH activities                                                             	| 200,000 	| 500,000 	| Missing 	|
| Missing value is replaced like this                                                           	| 2018    	| 2019    	| 2020    	|
| People Reached by WASH activities                                                             	| 200,000 	| 500,000 	| 350,000 	|

## Setting up

### Setting up this project

To get this repository, the best way is to have `git` and use `git clone`:

```bash
git clone https://github.com/FDRS-ifrc/fdrs_imputing_missing_values.git
```

then to enter the project
```bash
cd fdrs_imputing_missing_values 
```
### Setting up the environment

This project requires Python 3.

Virtual environments are important for creating reproducible analyses. One popular tool for managing Python and virtual environments is [`conda`](https://docs.conda.io/en/latest/miniconda.html). You can set up the environment for this project with `conda` using the commands below.

```bash
conda create -n fdrs_imputing_missing_values python=3.8
conda activate fdrs_imputing_missing_values
pip install -r requirements.txt
```

### Running the notebook
The core of this repository is the notebook 1.0-sw-imputing-notebook. 
This is to apply the method selected by the FDRS team and to interact with the FDRS backoffice with a post method to publish the imputed values. 
The ingested data is replicated and displayed on the website https://data.ifrc.org/FDRS/ and used in FDRS research such as [Everyone count report](https://data-api.ifrc.org/documents/noiso/Everyone%20Counts%20Report%202022%20EN.pdf). 

Therefore this is not a research notebook but a production notebook. It aims to simply expose the FDRS methodology of imputation step by step. A related python script has been created: [imp_script_post.py](https://github.com/FDRS-ifrc/fdrs_imputing_missing_values/blob/main/imp_script_post.py) which allows to launch this methodology once. 

You can use [Jupyter Lab](https://jupyter.org/) to view and edit it:

```bash
jupyter lab
```


## Project Organization
------------
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── imp_script_post.py    <- Full scripted imputation for automation
    ├── src                <- Source code for use in this project.
    │   ├── data           <- Scripts used in the notebook 
    │   │   └── fdrsapi.py         <- Script to download FDRS data from public API
            └── imp_function.py    <- Scripted imputation method 
    │   │
    │   ├── config       <- Configuration files
    │   │   └── public_api_key.txt <- Public  API key
            └── api_key.txt        <- Private API key (hidden)

*Last update: 20.08.2022*  
*Contact : fdrs@ifrc.org or simon.weiss@ifrc.org*  
*FDRS focal point: Simon Weiss, FDRS Data Analyst*    

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>.</small></p>
