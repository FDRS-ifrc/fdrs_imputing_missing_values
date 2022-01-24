import pandas as pd
import numpy as np
import requests, json
from pandas import json_normalize
import json


#function baseline
def baseline(years_selected,key):
    code = requests.get(f"https://data-api.ifrc.org/api/entities/ns/?apiKey={key}").json()
    df_code = json_normalize(data=code)
    df_code=df_code[["KPI_DON_code","NSO_DON_name","NSO_ZON_name","iso_3"]]
    df_baseline=pd.DataFrame()
    df_final=pd.DataFrame()
    for year in years_selected:
        print(year)
        df_code["KPI_Year"]=year
        df_baseline=df_baseline.append(df_code)
        df_final=df_final.append(df_baseline)
    df_baseline["KPI ID"]=df_baseline["KPI_DON_code"]+df_baseline["KPI_Year"]
    df_final["KPI ID"]=df_final["KPI_DON_code"]+df_final["KPI_Year"]
    return df_baseline


#api_function
def api_function(years,kpi_code,kpi_float,api_key):
    df_baseline=baseline(years,api_key)
    df_temp = pd.DataFrame()
    df = pd.DataFrame()
    df_final=pd.DataFrame()
    print("number of KPI downloaded:", len(kpi_code))
    for kpi_codes in kpi_code:
    # df_final.merge(df_baseline[["KPI ID","value"]],on=["KPI ID"],how="outer")
        for year in years:
            results = requests.get(f"https://data-api.ifrc.org/api/KpiValue?kpicode={kpi_codes}&year={year}&apiKey={api_key}").json()
            df_temp = json_normalize(data=results)
            df_temp["KPI_Year"] = year
            df_temp["KPI_Code"]=kpi_codes
            df=df.append(df_temp,ignore_index=True)
            df["KPI ID"]=df["doncode"]+df["KPI_Year"]
    df_final=df_baseline.merge(df[["KPI ID","value","KPI_Code"]],on=["KPI ID"],how="outer")
    df_baseline.set_index("KPI ID",inplace=True)
    df_final_pivot=df_final.pivot(values=["value"],columns=["KPI_Code"],index=["KPI ID"])
    df_final_pivot["KPI ID"]=df_final_pivot.index
    df_final_pivot.reset_index(drop=True)
    df_final_pivot.index.rename('', inplace=True)
    time_series=df_baseline.join(df_final_pivot["value"])
    time_series=pd.DataFrame(time_series.to_records())
    time_series=time_series.drop("nan",axis=1)
    time_series[kpi_float]=time_series[kpi_float].astype(float)
    time_series["KPI_Year"]=time_series["KPI_Year"].astype(np.int64)
    return time_series


