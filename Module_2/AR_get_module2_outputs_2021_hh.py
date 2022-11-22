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
year = 2021
hh_data_path = mod_1_output_path + ''
output_data_path = working_path + ''
os.chdir(working_path)

puprn_summary_hh = mod2.get_hh_means_all_puprn(hh_data_path)
puprn_summary_hh.to_csv(output_data_path + 'Annual_report_sm_monthly_mean_hh_profiles_' + str(year) + '.csv', index = False)

puprn_summary_hh_annual = mod2.get_annual_summary(puprn_summary_hh, year)
puprn_summary_hh_annual.to_csv(output_data_path + 'Annual_report_sm_annual_mean_hh_profiles_' + str(year) + '.csv', index = False)

puprn_summary_temp_bands = mod2.get_hh_means_all_puprn(hh_data_path, temperature_banding = True)
puprn_summary_temp_bands.to_csv(output_data_path + 'Annual_report_sm_temperature_banded_hh_profiles_' + str(year) + '.csv', index = False)

