# data analysis and wrangling
import pandas as pd
import numpy as np
import random as rnd
#Define function
#Imputing by means
def imputing_mean(dataset,col):
  for j in col:
    print(j)
    dataset[j]=dataset.groupby("NSO_DON_name")[j].transform(lambda x: x.fillna(x.mean(),limit=3))
  dataset[dataset.KPI_Year ==2018]=data_clean2[data_clean2.KPI_Year ==2018].copy()
#Imputing by previous values
def imputing_na(dataset2,col):
  for j in col:
    print(j)
    dataset2[j]=dataset2.groupby("NSO_DON_name")[j].transform(lambda x: x.fillna(method='ffill',limit=3))
# import FDRS dataset using CSV snapshot 0.allTimeValidation_02112021134644.csv : "EC 21 - November\DRAFT3\0.allTimeValidation_02112021134644.csv"
data = pd.read_csv("https://drive.google.com/uc?export=download&id=17lSBwY62HAJg9h2KmlsOifXeqti2JgLQ")
# Import FDRS dataset using get method (live data)


# drop from 0 to 2011
data_clean=data.query('KPI_Year >= 2012 & KPI_Year < 2021')
# filter out columns that can be ignored: 64 columns dropped (see columns 1134 to 1070)
data_clean=data_clean.drop(['Population','GDP',	'Poverty',	'GNIPC',	'LifeExp',	'ChildMortality',	'Literacy',	'UrbPop',	'MaternalMortality', 'KPI_StartDate', 'KPI_EndDate', 'supported2','supported3','supported4','supported5',
'supported6','supported7','supported8','supported9','supported10','received_support2','received_support3','received_support4','received_support5','received_support6','received_support7','received_support8','received_support9',
'received_support10','KPI_accountingSystem','KPI_deletedDateTime', 'KPI_donatedInKindLC',	'KPI_donatedInKindLC_CHF',	'KPI_FiguresHasInKind',	'KPI_FinanaceStatementHasInKind',	'KPI_financesToNSsLC',	'KPI_financesToNSsLC_CHF',	
'KPI_hasExternalAduits',	'KPI_hasGoodsInKind',	'KPI_hasIFRS',	'KPI_hasServicesInKind',	'KPI_noPeopleCoveredPreparedness',	'KPI_noPeopleReachedAllServices',	'KPI_noPeopleReachedDevelopment',	'KPI_noPeopleReachedDevelopmentDirect',	
'KPI_noPeopleReachedDevelopmentDirectF',	'KPI_noPeopleReachedDevelopmentDirectM',	'KPI_noPeopleReachedDevelopmentIndirect',	'KPI_noPeopleReachedDisaster',	'KPI_noPeopleReachedHealth',	'KPI_noPeopleReachedHealthDirect',	
'KPI_noPeopleReachedHealthDirectF',	'KPI_noPeopleReachedHealthDirectM',	'KPI_noPeopleReachedHealthIndirect',	'KPI_noPeopleReachedServices',	'KPI_noPeopleReachedServicesDirect',	'KPI_noPeopleReachedServicesDirectF',	
'KPI_noPeopleReachedServicesDirectM',	'KPI_noPeopleReachedServicesIndirect',	'KPI_recievedInKindLC',	'KPI_recievedInKindLC_CHF',	'KPI_SPS_ID',	'lc',	'supp'], axis=1)
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
imputing_mean(data_clean2_na_columns,selected_cols)
# Imputing : NS Finance and Parnership & NS Governance and Structure Section
selected_cols_2d=["KPI_GB_Tot","KPI_pr_sex","KPI_sg_sex","KPI_PeopleVol_Tot","KPI_PStaff_Tot","KPI_noLocalUnits","KPI_IncomeLC","KPI_IncomeLC_CHF","KPI_expenditureLC" ,"KPI_expenditureLC_CHF"]
# 18.11.21Comment : After reviewing with the Team, "supported1" and "received_support1" have been imputed seperatly in the script FDRS Network Data Transformation
imputing_na(data_clean2_na_columns,selected_cols_2d)
# concatenate the two sub_datasets
fdrs_na_columns = [data_clean1, data_clean2_na_columns]
fdrs_data_fdrs_na_columns = pd.concat(fdrs_na_columns)
