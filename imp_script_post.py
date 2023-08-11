import os
import json
import time
import requests
import pandas as pd
import numpy as np
import sys

# 1. Configuration Loading
def select_environment(config_prod_path, config_staging_path):
    """Select the environment and load the respective configuration."""
    env = ''
    while env not in ['staging', 'prod']:
        env = input("Select environment ('staging' or 'prod'): ").lower()
        if env == 'prod':
            confirm = input("WARNING: You've selected the PRODUCTION environment. Are you sure? (yes/no) ")
            if confirm.lower() != 'yes':
                print("Switching to 'staging' by default for safety.")
                env = 'staging'
    try:
        with open(config_prod_path if env == 'prod' else config_staging_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        raise Exception(f"Configuration file for {env} not found.")

# Load configuration
config_prod_path = '../src/config/config_prod.json'
config_staging_path = '../src/config/config_staging.json'
config = select_environment(config_prod_path, config_staging_path)
print(f"Loaded configuration for {config['ENV']} environment.")
user_email = input('Please provide your FDRS data analyst email: ')

# 2. Data Retrieval
sys.path.append(os.path.join(os.path.abspath(os.path.join('..')), "src", "data"))
from fdrsapi import api_function

# Load KPI Codebook
base_url = config['BASE_URL']
KPI = pd.json_normalize(data=requests.get(f"{base_url}indicator?apiKey={config['API_KEY_PUBLIC']}").json())
codebook = KPI.copy()

kpi_reach = codebook.query('KPI_Note == "NS Reach - CPD"')["KPI_Code"].to_list() + ["KPI_TrainFA_Tot", "KPI_DonBlood_Tot"]
metadata_kpis = codebook.query('KPI_Note == "Metadata"')["KPI_Code"].tolist()
kpi_gov = ["KPI_GB_Tot", "KPI_PeopleVol_Tot", "KPI_PStaff_Tot", "KPI_noLocalUnits"]
kpi_fi = ["KPI_IncomeLC_CHF", "KPI_expenditureLC_CHF"]
kpi_gov_fi_code = kpi_gov + kpi_fi
exclude_kpis = ["KPI_Year", "DON_Code", "KPI_Id"]
metadata_kpis = [kpi for kpi in metadata_kpis if kpi not in exclude_kpis]
kpi_code = kpi_reach + kpi_gov_fi_code + metadata_kpis

# Get data from FDRS API
years = ["2018", "2019", "2020", "2021", "2022"]
time_series = api_function(years, kpi_code, config)

def impute_data(df, kpi_reach_code, kpi_gov_fi_code, selected_year):
    """Impute missing data in the dataframe."""
    data = df.copy()
    
    condition_selected_year = data['KPI_Year'] == float(selected_year)
    condition_not_submitted = data['KPI_WasSubmitted'] != 1.0
    condition_nsgs_not_submitted = data['KPI_NSGS_WasSubmitted'] != 1.0

    for col in kpi_reach_code:
        mask = condition_selected_year & condition_not_submitted & data[col].isna()
        if mask.any():
            data[col] = data.groupby("NSO_DON_name")[col].transform(lambda x: x.fillna(x.mean(), limit=3))

    for col in kpi_gov_fi_code:
        mask = condition_selected_year & condition_not_submitted & data[col].isna()
        if mask.any():
            data[col] = data.groupby("NSO_DON_name")[col].transform(lambda x: x.fillna(method='ffill', limit=3))

    condition_special = (data['KPI_WasSubmitted'] == 1.0) & condition_nsgs_not_submitted
    for col in kpi_gov:
        mask = condition_selected_year & condition_special & data[col].isna()
        if mask.any():
            data[col] = data.groupby("NSO_DON_name")[col].transform(lambda x: x.fillna(method='ffill', limit=3))

    return data


imputed_data_2022 = impute_data(time_series, kpi_reach, kpi_gov_fi_code, 2022)

def prepare_data_for_post(original_df, imputed_df, kpi_list, selected_year):
    """Prepare the data for posting."""
    original_subset = original_df[original_df["KPI_Year"] == selected_year].copy()
    imputed_subset = imputed_df[imputed_df["KPI_Year"] == selected_year].copy()

    for kpi in kpi_list:
        for _, row in imputed_subset.iterrows():
            original_value = original_subset[original_subset['KPI_DON_code'] == row['KPI_DON_code']][kpi].values[0]
            imputed_subset.at[row.name, f"{kpi}_source"] = 'NSI' if original_value == row[kpi] else 'I'

    return imputed_subset

def post_imputed_data(df, kpi_list, selected_year):
    """Post imputed data."""
    total_kpis = len(kpi_list)
    total_dons = len(df["KPI_DON_code"].unique())
    total_requests = total_kpis * total_dons
    sleep_time = 0.5
    print(f"Estimated time for completion: {total_requests * sleep_time / 60:.2f} minutes.")
    
    failed_posts = []
    for kpi in kpi_list:
        kpi_ip = kpi + "_IP"
        for kpi_don_code in df["KPI_DON_code"].unique():
            row = df[(df["KPI_Year"] == selected_year) & (df["KPI_DON_code"] == kpi_don_code)].iloc[0]
            value = int(row[kpi]) if not pd.isna(row[kpi]) else ""
            source = row[f"{kpi}_source"]
            url = f"{config['BASE_URL']}ImputedKPI?apiKey={config['API_KEY_PRIVATE']}&kpicode={kpi_ip}&year={selected_year}&don_code={kpi_don_code}&value={value}&source={source}&user={user_email}"
            
            response = requests.post(url)
            time.sleep(sleep_time)
            if response.status_code != 200:
                failed_posts.append((kpi, kpi_don_code, response.status_code, response.text))

    if failed_posts:
        print("\nSummary of failed POSTs:")
        for kpi, don, status, msg in failed_posts:
            print(f"KPI: {kpi}, DON: {don} failed with {status}: {msg}")

imputed_data_prepared = prepare_data_for_post(time_series, imputed_data_2022, kpi_reach + kpi_gov_fi_code, 2022)
post_imputed_data(imputed_data_prepared, kpi_reach + kpi_gov_fi_code, 2022)
