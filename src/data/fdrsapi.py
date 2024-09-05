import requests
from pandas import json_normalize
import pandas as pd

def baseline(years_selected, config):
    """
    Fetches baseline data for a given year from the FDRS API.
    """
    try:
        code = requests.get(f"{config['BASE_URL']}entities/ns/?apiKey={config['API_KEY_PUBLIC']}").json()
        df_code = json_normalize(data=code)[["KPI_DON_code", "NSO_DON_name", "NSO_ZON_name", "iso_3"]]
        
        dfs = [df_code.assign(KPI_Year=float(year)) for year in years_selected]
        df_baseline = pd.concat(dfs, ignore_index=True)
        df_baseline["KPI ID"] = df_baseline["KPI_DON_code"] + df_baseline["KPI_Year"].astype(str)
        
        return df_baseline
    except requests.RequestException as e:
        print(f"Error fetching baseline data: {e}")
        return pd.DataFrame()

def fetch_data(years, kpi_code, config, endpoint):
    df_temp = pd.DataFrame()
    df = pd.DataFrame()

    for kpi_codes in kpi_code:
        for year in years:
            try:
                url = f"{config['BASE_URL']}{endpoint}?kpicode={kpi_codes}&year={year}&apiKey={config['API_KEY_PUBLIC']}&showunpublished=true"
                results = requests.get(url).json()
                df_temp = json_normalize(data=results).assign(KPI_Year=float(year), KPI_Code=kpi_codes)
                df = df._append(df_temp, ignore_index=True)
            except requests.RequestException as e:
                print(f"Error fetching data for {kpi_codes} in {year}: {e}")

    return df

def pivot_data(df, df_baseline):
    df_pivoted = df.pivot_table(index=['doncode', 'KPI_Year'], columns='KPI_Code', values='value', aggfunc='first').reset_index()
    
    # Convert specific columns to float
    for col in df_pivoted.columns:
        if col not in ['doncode']:
            df_pivoted[col] = df_pivoted[col].astype(float, errors='ignore')
    
    df_final = df_baseline.merge(df_pivoted, left_on=["KPI_DON_code", "KPI_Year"], right_on=["doncode", "KPI_Year"], how="outer")
    return df_final.drop("doncode", axis=1)

def api_function(years, kpi_code, config):
    df_data = fetch_data(years, kpi_code, config, 'KpiValue')
    df_baseline = baseline(years, config)
    return pivot_data(df_data, df_baseline)

def api_function_imputed(years, kpi_code, config):
    df_data = fetch_data(years, kpi_code, config, 'KpiImputedValue')
    if len(df_data)==0:
        df_data=fetch_data(["2022"], kpi_code, config, 'KpiImputedValue')
    df_baseline = baseline(years, config)
    return pivot_data(df_data, df_baseline)
