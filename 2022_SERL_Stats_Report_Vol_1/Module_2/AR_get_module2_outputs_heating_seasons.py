# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 11:44:59 2022

@author: j.few
"""


import numpy as np
import pandas as pd
import os
import warnings
import time

working_path = ''
script_path = working_path + ''
os.chdir(script_path)
import AR_mod2_functions_per_puprn as mod2


mod_1_output_path = ''
output_data_path = working_path + ''
os.chdir(working_path)


# get heating season summaries

# 2019/2020 and 2020/2021 season 
# hh 
puprn_summary_hh_2019 = pd.read_csv(output_data_path  + 'Annual_report_sm_monthly_mean_hh_profiles_2019.csv')
puprn_summary_hh_2020 = pd.read_csv(output_data_path  + 'Annual_report_sm_monthly_mean_hh_profiles_2020.csv')
puprn_summary_hh_2021 = pd.read_csv(output_data_path  + 'Annual_report_sm_monthly_mean_hh_profiles_2021.csv')

puprn_summary_hh_hs_19_20 = mod2.get_heating_season_summary(puprn_summary_hh_2019, 2019, puprn_summary_hh_2020, 2020)
puprn_summary_hh_hs_19_20.to_csv(output_data_path + 'Annual_report_sm_heating_season_mean_hh_profiles_2019_2020.csv', index = False)

puprn_summary_hh_hs_20_21 = mod2.get_heating_season_summary(puprn_summary_hh_2020, 2020, puprn_summary_hh_2021, 2021)
puprn_summary_hh_hs_20_21.to_csv(output_data_path + 'Annual_report_sm_heating_season_mean_hh_profiles_2020_2021.csv', index = False)

# daily 
puprn_summary_daily_2019 = pd.read_csv(output_data_path  + 'Annual_report_sm_monthly_mean_daily_consumption_2019.csv')
puprn_summary_daily_2020 = pd.read_csv(output_data_path  + 'Annual_report_sm_monthly_mean_daily_consumption_2020.csv')
puprn_summary_daily_2021 = pd.read_csv(output_data_path  + 'Annual_report_sm_monthly_mean_daily_consumption_2021.csv')

puprn_summary_daily_hs_19_20 = mod2.get_heating_season_summary(puprn_summary_daily_2019, 2019, puprn_summary_daily_2020, 2020)
puprn_summary_daily_hs_19_20.to_csv(output_data_path + 'Annual_report_sm_heating_season_mean_daily_consumption_2019_2020.csv', index = False)

puprn_summary_daily_hs_20_21 = mod2.get_heating_season_summary(puprn_summary_daily_2020, 2020, puprn_summary_daily_2020, 2021)
puprn_summary_daily_hs_20_21.to_csv(output_data_path + 'Annual_report_sm_heating_season_mean_daily_consumption_2020_2021.csv', index = False)
