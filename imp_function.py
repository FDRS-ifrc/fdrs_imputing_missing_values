# data analysis and wrangling
import pandas as pd
import numpy as np
import random as rnd
#Define function
#Imputing by means
def imputing_mean(dataset,col,data_clean2):
  for j in col:
    print(j)
    dataset[j]=dataset.groupby("NSO_DON_name")[j].transform(lambda x: x.fillna(x.mean(),limit=3))
  dataset[dataset.KPI_Year ==2019]=data_clean2[data_clean2.KPI_Year ==2019].copy()
#Imputing by previous values
def imputing_na(dataset2,col):
  for j in col:
    print(j)
    dataset2[j]=dataset2.groupby("NSO_DON_name")[j].transform(lambda x: x.fillna(method='ffill',limit=3))
