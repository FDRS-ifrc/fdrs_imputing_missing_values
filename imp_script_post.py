#Imputing Python script to be run each time imputed totals need to be updated
#Note: staging to be replaced when backoffice and front end updated
### 0. Import modules
import pandas as pd
import numpy as np
import random as rnd
import requests, json
from pandas import json_normalize
import json
from datetime import datetime
print ("1. Getting Data")
from fdrsapi import api_function,baseline, api_function_imputed
f = open("public_api_key.txt", "r")
api_key=f.readline()
#Selecting years to download
years=["2017","2018","2019","2020"]
#Selecting indicators to impute
kpi_code=["KPI_DonBlood_Tot","KPI_TrainFA_Tot","KPI_ReachDRER_D_Tot","KPI_ReachDRER_I","KPI_ReachDRR_D_Tot","KPI_ReachDRR_I","KPI_ReachLTSPD_D_Tot","KPI_ReachLTSPD_I","KPI_ReachS_D_Tot","KPI_ReachS_I","KPI_ReachL_D_Tot","KPI_ReachL_I","KPI_ReachM_D_Tot","KPI_ReachM_I","KPI_ReachCTP_D_Tot","KPI_ReachCTP_I","KPI_ReachSI_D_Tot","KPI_ReachSI_I","KPI_ReachH_D_Tot","KPI_ReachH_I","KPI_ReachWASH_I","KPI_ReachWASH_D_Tot","KPI_ReachDRER_CPD","KPI_ReachLTSPD_CPD","KPI_ReachDRR_CPD","KPI_ReachS_CPD","KPI_ReachL_CPD", "KPI_ReachH_CPD","KPI_ReachWASH_CPD","KPI_ReachM_CPD","KPI_ReachCTP_CPD","KPI_ReachSI_CPD","KPI_GB_Tot","KPI_pr_sex","KPI_sg_sex","KPI_PeopleVol_Tot","KPI_PStaff_Tot","KPI_noLocalUnits","KPI_IncomeLC","KPI_IncomeLC_CHF","KPI_expenditureLC","KPI_expenditureLC_CHF"]
kpi_float=kpi_code.copy()
kpi_float.remove("KPI_pr_sex")
kpi_float.remove("KPI_sg_sex")
time_series=api_function(years,kpi_code,kpi_float,api_key)
print ("2. Running imputing missing values")
from imp_function import imputing_mean,imputing_na
# drop from 0 to 2011
data_clean=time_series.query('KPI_Year >= 2012 & KPI_Year < 2021')
# filter out columns that can be ignored: 64 columns dropped (see columns 1134 to 1070)
# The only reporting year FDRS didn't count with 100% reporting was 2019. Therefore, lets divide the dataset in 2: 
# 2012-2017, 2018-2020. This facilitates the code as KPI_WasSubmitted variable (which is being used to filter the data) 
# was implemented in 2017 and all years before have NaN 
# 1st subset:
data_clean1=data_clean.query('KPI_Year >= 2012 & KPI_Year < 2018').copy()
# 2nd subset:
data_clean2=data_clean.query('KPI_Year >= 2018').copy()
data_clean2_na_columns=data_clean2.copy()
#Imputing:People reached sections 
selected_cols=["KPI_DonBlood_Tot","KPI_TrainFA_Tot","KPI_ReachDRER_D_Tot","KPI_ReachDRER_I","KPI_ReachDRR_D_Tot","KPI_ReachDRR_I","KPI_ReachLTSPD_D_Tot","KPI_ReachLTSPD_I","KPI_ReachS_D_Tot","KPI_ReachS_I","KPI_ReachL_D_Tot","KPI_ReachL_I","KPI_ReachM_D_Tot","KPI_ReachM_I","KPI_ReachCTP_D_Tot","KPI_ReachCTP_I","KPI_ReachSI_D_Tot","KPI_ReachSI_I","KPI_ReachH_D_Tot","KPI_ReachH_I","KPI_ReachWASH_I","KPI_ReachWASH_D_Tot","KPI_ReachDRER_CPD","KPI_ReachLTSPD_CPD","KPI_ReachDRR_CPD","KPI_ReachS_CPD","KPI_ReachL_CPD", "KPI_ReachH_CPD","KPI_ReachWASH_CPD","KPI_ReachM_CPD","KPI_ReachCTP_CPD","KPI_ReachSI_CPD"]
imputing_mean(data_clean2_na_columns,selected_cols,data_clean2)
# Imputing : NS Finance and Parnership & NS Governance and Structure Section
selected_cols_2d=["KPI_GB_Tot","KPI_pr_sex","KPI_sg_sex","KPI_PeopleVol_Tot","KPI_PStaff_Tot","KPI_noLocalUnits","KPI_IncomeLC_CHF" ,"KPI_expenditureLC_CHF"]
# 18.11.21Comment : After reviewing with the Team, "supported1" and "received_support1" have been imputed seperatly in the script FDRS Network Data Transformation
imputing_na(data_clean2_na_columns,selected_cols_2d)
# concatenate the two sub_datasets
fdrs_na_columns = [data_clean1, data_clean2_na_columns]
fdrs_data_fdrs_na_columns = pd.concat(fdrs_na_columns)

#3. Get imputed values t-1 from backoffice to compare with imputed values t
kpi_code=["KPI_PeopleVol_Tot","KPI_PStaff_Tot","KPI_DonBlood_Tot","KPI_TrainFA_Tot","KPI_noLocalUnits","KPI_ReachDRER_CPD","KPI_ReachLTSPD_CPD","KPI_ReachDRR_CPD","KPI_ReachS_CPD","KPI_ReachL_CPD","KPI_ReachH_CPD","KPI_ReachWASH_CPD","KPI_ReachM_CPD","KPI_ReachCTP_CPD","KPI_ReachSI_CPD","KPI_IncomeLC_CHF","KPI_expenditureLC_CHF"]
kpi_code= [s + "_IP" for s in kpi_code]
kpi_float=kpi_code.copy()
time_series_imputed=api_function_imputed(years,kpi_code,kpi_float,api_key)
time_series_imputed
kpi_code=["KPI_PeopleVol_Tot","KPI_PStaff_Tot","KPI_DonBlood_Tot","KPI_TrainFA_Tot","KPI_noLocalUnits","KPI_ReachDRER_CPD","KPI_ReachLTSPD_CPD","KPI_ReachDRR_CPD","KPI_ReachS_CPD","KPI_ReachL_CPD","KPI_ReachH_CPD","KPI_ReachWASH_CPD","KPI_ReachM_CPD","KPI_ReachCTP_CPD","KPI_ReachSI_CPD","KPI_IncomeLC_CHF","KPI_expenditureLC_CHF"]
# Convert to type int to remove decimals from calculations
#fdrs_data_fdrs_na_columns[kpi_code]=fdrs_data_fdrs_na_columns[kpi_code].fillna(0).astype('int')
#fdrs_data_fdrs_na_columns[kpi_code]=fdrs_data_fdrs_na_columns[kpi_code].replace(0,np.nan)
kpi_code=["KPI_PeopleVol_Tot","KPI_PStaff_Tot","KPI_DonBlood_Tot","KPI_TrainFA_Tot","KPI_noLocalUnits","KPI_ReachDRER_CPD","KPI_ReachLTSPD_CPD","KPI_ReachDRR_CPD","KPI_ReachS_CPD","KPI_ReachL_CPD","KPI_ReachH_CPD","KPI_ReachWASH_CPD","KPI_ReachM_CPD","KPI_ReachCTP_CPD","KPI_ReachSI_CPD","KPI_IncomeLC_CHF","KPI_expenditureLC_CHF"]
kpi_code_imp= [s + "_IP" for s in kpi_code]
kpi_code_binary = [s + "_binary" for s in kpi_code]
for kpi in list(kpi_code):
    kpi_code_imp=kpi+"_IP"
    kpi_code_binary=kpi+"_binary"
    fdrs_data_fdrs_na_columns.loc[time_series_imputed[kpi_code_imp] == fdrs_data_fdrs_na_columns[kpi],kpi_code_binary]=0 #0 if same
    fdrs_data_fdrs_na_columns.loc[time_series_imputed[kpi_code_imp] != fdrs_data_fdrs_na_columns[kpi],kpi_code_binary]=1 #1 if different
    
    fdrs_data_fdrs_na_columns.loc[fdrs_data_fdrs_na_columns[kpi].isna(),kpi_code_binary]=2
    #To be uncommented when the data is ready
    fdrs_data_fdrs_na_columns.loc[fdrs_data_fdrs_na_columns[kpi_code_binary]==2,kpi_code_binary]=0

            



#Selecting years to post
years=[2016,2017,2018,2019,2020]
#Selecting indicators to impute
f = open("api_key.txt", "r")
api_key=f.readline()

kpi_code=["KPI_PeopleVol_Tot"]
payload={}
files={}
headers = {}

#fdrs_data_fdrs_na_columns_test.fillna(0,inplace=True)
for kpi in list(kpi_code):
    kpi_binary = kpi + "_binary"
    for year in years:
        for kpi_don_code in list(set(fdrs_data_fdrs_na_columns["KPI_DON_code"])): 
            for bi in (fdrs_data_fdrs_na_columns[kpi_binary][(fdrs_data_fdrs_na_columns["KPI_Year"]==year)&(fdrs_data_fdrs_na_columns["KPI_DON_code"]==kpi_don_code)]):
                binary = bi
                if binary == 1.0: 
                    for label, content in (fdrs_data_fdrs_na_columns[kpi][(fdrs_data_fdrs_na_columns["KPI_Year"]==year)&(fdrs_data_fdrs_na_columns["KPI_DON_code"]==kpi_don_code)]).items():
                        value = content
                        if type(value)==float:
                            if not np.isnan(value):
                                value=np.int64(value)
                                value = value
                        #if value == "nan":
                        if np.isnan(value):
                            value = ""
                        kpi_post=kpi+"_IP"
                        url = (f"https://data-api.ifrc.org/api/ImputedKPI?apiKey={api_key}&kpicode={kpi_post}&year={year}&don_code={kpi_don_code}&value={value}&user=simon.weiss@ifrc.org")
                        r = requests.post(url,headers=headers, data=payload, files=files)
                        #print(url,r.text)
                        #print(r.request.body)
                        #print(r.request.headers)

df1 = pd.DataFrame([datetime.now()], columns=['Lattest Update Date'])
df1.to_excel("lattest_update.xlsx", index=False)
                        