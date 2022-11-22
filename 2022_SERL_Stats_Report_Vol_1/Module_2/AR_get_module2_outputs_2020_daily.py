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
daily_data_path = mod_1_output_path
year = 2020
hh_data_path = mod_1_output_path + ''
output_data_path = working_path + ''
os.chdir(working_path)

puprn_summary_daily = mod2.get_daily_means_all_puprn(daily_data_path, year)
puprn_summary_daily.to_csv(output_data_path + 'Annual_report_sm_monthly_mean_daily_consumption_' + str(year) + '.csv', index = False)

puprn_summary_daily_annual = mod2.get_annual_summary(puprn_summary_daily, year)
puprn_summary_daily_annual.to_csv(output_data_path + 'Annual_report_sm_annual_mean_daily_consumption_' + str(year) + '.csv', index = False)

puprn_summary_daily_temp_bands = mod2.get_daily_means_all_puprn(daily_data_path, year, temperature_banding = True)
puprn_summary_daily_temp_bands.to_csv(output_data_path + 'Annual_report_sm_temp_banded_mean_daily_consumption_' + str(year) + '.csv', index = False)
