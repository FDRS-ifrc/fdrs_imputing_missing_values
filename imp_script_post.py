#Imputing Python script to be run each time imputed totals need to be updated
#Note: staging to be replaced when backoffice and front end updated
### 0. Import modules
import pandas as pd
import numpy as np
import random as rnd
import requests, json
from pandas import json_normalize
import json
### 1. Get Data
from fdrsapi import api_function,baseline
f = open("api_key.txt", "r")
api_key=f.readline()
#Selecting years to download
years=["2017","2018","2019","2020"]
#Selecting indicators to impute
kpi_code=["KPI_DonBlood_Tot","KPI_TrainFA_Tot","KPI_ReachDRER_D_Tot","KPI_ReachDRER_I","KPI_ReachDRR_D_Tot","KPI_ReachDRR_I","KPI_ReachLTSPD_D_Tot","KPI_ReachLTSPD_I","KPI_ReachS_D_Tot","KPI_ReachS_I","KPI_ReachL_D_Tot","KPI_ReachL_I","KPI_ReachM_D_Tot","KPI_ReachM_I","KPI_ReachCTP_D_Tot","KPI_ReachCTP_I","KPI_ReachSI_D_Tot","KPI_ReachSI_I","KPI_ReachH_D_Tot","KPI_ReachH_I","KPI_ReachWASH_I","KPI_ReachWASH_D_Tot","KPI_ReachDRER_CPD","KPI_ReachLTSPD_CPD","KPI_ReachDRR_CPD","KPI_ReachS_CPD","KPI_ReachL_CPD", "KPI_ReachH_CPD","KPI_ReachWASH_CPD","KPI_ReachM_CPD","KPI_ReachCTP_CPD","KPI_ReachSI_CPD","KPI_GB_Tot","KPI_pr_sex","KPI_sg_sex","KPI_PeopleVol_Tot","KPI_PStaff_Tot","KPI_noLocalUnits","KPI_IncomeLC","KPI_IncomeLC_CHF","KPI_expenditureLC","KPI_expenditureLC_CHF"]
kpi_float=kpi_code.copy()
kpi_float.remove("KPI_pr_sex")
kpi_float.remove("KPI_sg_sex")
time_series=api_function(years,kpi_code,kpi_float,api_key)
### 2. Run imputing missing values
from imp_function import imputing_mean,imputing_na
# drop from 0 to 2011
data_clean=time_series.query('KPI_Year >= 2012 & KPI_Year < 2021')
# 1st subset:
data_clean1=data_clean.query('KPI_Year >= 2012 & KPI_Year < 2018').copy()
# 2nd subset:
data_clean2=data_clean.query('KPI_Year >= 2018').copy()
data_clean2_na_columns=data_clean2.copy()
#Imputing:People reached sections 
selected_cols=["KPI_DonBlood_Tot","KPI_TrainFA_Tot","KPI_ReachDRER_D_Tot","KPI_ReachDRER_I","KPI_ReachDRR_D_Tot","KPI_ReachDRR_I","KPI_ReachLTSPD_D_Tot","KPI_ReachLTSPD_I","KPI_ReachS_D_Tot","KPI_ReachS_I","KPI_ReachL_D_Tot","KPI_ReachL_I","KPI_ReachM_D_Tot","KPI_ReachM_I","KPI_ReachCTP_D_Tot","KPI_ReachCTP_I","KPI_ReachSI_D_Tot","KPI_ReachSI_I","KPI_ReachH_D_Tot","KPI_ReachH_I","KPI_ReachWASH_I","KPI_ReachWASH_D_Tot","KPI_ReachDRER_CPD","KPI_ReachLTSPD_CPD","KPI_ReachDRR_CPD","KPI_ReachS_CPD","KPI_ReachL_CPD", "KPI_ReachH_CPD","KPI_ReachWASH_CPD","KPI_ReachM_CPD","KPI_ReachCTP_CPD","KPI_ReachSI_CPD"]
imputing_mean(data_clean2_na_columns,selected_cols,data_clean2)
# Imputing : NS Finance and Parnership & NS Governance and Structure Section
selected_cols_2d=["KPI_GB_Tot","KPI_pr_sex","KPI_sg_sex","KPI_PeopleVol_Tot","KPI_PStaff_Tot","KPI_noLocalUnits","KPI_IncomeLC","KPI_IncomeLC_CHF","KPI_expenditureLC" ,"KPI_expenditureLC_CHF"]
imputing_na(data_clean2_na_columns,selected_cols_2d)
# concatenate the two sub_datasets
fdrs_na_columns = [data_clean1, data_clean2_na_columns]
fdrs_data_fdrs_na_columns = pd.concat(fdrs_na_columns)
### 3. Post method - interact with Imp_Var
#Selecting years to post
years=[2017,2018,2019,2020]
value=[]
for kpi_don_code in list(set(fdrs_data_fdrs_na_columns["KPI_DON_code"])):
    #for year in list(set(fdrs_data_fdrs_na_columns["KPI_Year"])):
    for year in years:
            for kpi in list(kpi_code):
                value= (fdrs_data_fdrs_na_columns[kpi][(fdrs_data_fdrs_na_columns["KPI_Year"]==year)&(fdrs_data_fdrs_na_columns["KPI_DON_code"]==kpi_don_code)]).item()
                if type(value)==float:
                    if not np.isnan(value):
                        value=np.int64(value)
                kpi_post=kpi+"_IP"
                url = (f"https://data-api-staging.ifrc.org/api/ImputedKPI?apiKey=21e401ae-6b35-404b-a72a-b74cce66dee3&kpicode={kpi_post}&year={year}&don_code={kpi_don_code}&value={value}&user=simon.weiss@ifrc.org")
                print(url)
                r = requests.post(url)
                #print(r.text)