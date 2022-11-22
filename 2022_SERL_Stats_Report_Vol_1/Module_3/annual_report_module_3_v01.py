# -*- coding: utf-8 -*-
"""
Created on Mon Feb  7 11:46:15 2022

@author: e.mckenna

Annual report - module 3
Purpose: take outputs from module 2 and produce summary statistics which can be
SDC checked, exported, and form the basis of the Annual Report and statistics
output.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import warnings

#%%
class Module3:
    def __init__(self, serl_data_path, m1_output_path, m2_output_path, save_path):
        self.m2_output_path = m2_output_path #<--- location of module 2 outputs to load
        self.m1_output_path = m1_output_path #<--- location of module 2 outputs to load
        self.save_path = save_path #<--- location to save module 3 outputs
        self.serl_data_path = serl_data_path #<--- location of serl data to load
        self.my_input_data = {}
        self.my_output_df = pd.DataFrame(columns=['fuel',
                                                  'unit',
                                                  'summary_stat',
                                                  'subsample',
                                                  'summary_time',
                                                  'time_period',
                                                  'segmentation_variable_1',
                                                  'segment_1_value',
                                                  'value',
                                                  'n_sample',
                                                  'n_statistic',
                                                  'mean_temp',
                                                  'mean_hdd'])
        
    def load_data(self, year: str):
        # load data for a specified year. Includes module 2 outputs and serl 
        # contextual data
        file_name = 'Annual_report_sm_annual_mean_daily_consumption_' + \
            year + '.csv'
        self.my_input_data['annual_daily_'+year] = pd.read_csv(self.m2_output_path + file_name)
        
        file_name = 'Annual_report_sm_annual_mean_hh_profiles_' + \
            year + '.csv'
        self.my_input_data['annual_hh_'+year] = pd.read_csv(self.m2_output_path + file_name)
        
        file_name = 'Annual_report_sm_monthly_mean_daily_consumption_' + \
            year + '.csv'
        self.my_input_data['monthly_daily_'+year] = pd.read_csv(self.m2_output_path + file_name)
        
        file_name = 'Annual_report_sm_monthly_mean_hh_profiles_' + \
            year + '.csv'
        self.my_input_data['monthly_hh_'+year] = pd.read_csv(self.m2_output_path + file_name)
        
        file_name = 'Annual_report_sm_temp_banded_mean_daily_consumption_' + \
            year + '.csv'
        self.my_input_data['temp_banded_daily_'+year] = pd.read_csv(self.m2_output_path + file_name)
        
        file_name = 'Annual_report_sm_temperature_banded_hh_profiles_' + \
            year + '.csv'
        self.my_input_data['temp_banded_hh_'+year] = pd.read_csv(self.m2_output_path + file_name)

        file_name = 'Elec_' + year + '_list_of_exporter_puprns.csv'
        self.my_input_data['exporters'] = pd.read_csv(self.m1_output_path+file_name, header = None)

    def load_heating_season_data(self, 
                                 start_year: str,
                                 end_year: str):
        file_name = 'Annual_report_sm_heating_season_mean_daily_consumption_' + \
            start_year + '_' + end_year + '.csv'
        self.my_input_data['heating_season_daily_'+start_year+'_'+end_year] = \
            pd.read_csv(self.m2_output_path + file_name)
        
        file_name = 'Annual_report_sm_heating_season_mean_hh_profiles_' + \
            start_year + '_' + end_year + '.csv'
        self.my_input_data['heating_season_hh_'+start_year+'_'+end_year] = \
            pd.read_csv(self.m2_output_path + file_name)
        
        
    def load_contextual(self):
        file_name = 'serl_epc_data_edition04.csv'
        self.my_input_data['epc'] = pd.read_csv(self.serl_data_path+file_name)
        
        file_name = 'serl_survey_data_edition04.csv'
        self.my_input_data['serl_survey'] = pd.read_csv(self.serl_data_path+file_name)
        
        file_name = 'serl_participant_summary_edition04.csv'
        self.my_input_data['participant'] = pd.read_csv(self.serl_data_path+file_name)
        
        file_name = 'serl_smart_meter_rt_summary_edition04.csv'
        self.my_input_data['read_type'] = pd.read_csv(self.serl_data_path+file_name)
        
    def preprocess_total_energy_data(self, year:str, heating_season: bool):
        # Module 2 naively calculates total energy consumption as total of 
        # elec + gas, and gives nan if either is missing
        # only want a nan if there is gas use but no gas meter
        
        serl_survey = self.my_input_data['serl_survey']
        epc = self.my_input_data['epc']
        read_type = self.my_input_data['read_type']
        participant = self.my_input_data['participant']
        
        # find gas heated homes
        gas_set = set()
        # find those with electric central heating
        logic = (serl_survey.A302 == 1)
        gas_set.update(serl_survey.loc[logic, 'PUPRN'])
        
        # also find homes with gas according to epc
        gas_epc_list = ['mains gas - this is for backwards compatibility only and should not be used',
           'mains gas (not community)',
           'mains gas (community)',
           'Gas: mains gas']
        
        logic = epc.mainFuel.isin(gas_epc_list)
        gas_set.update(epc.loc[logic, 'PUPRN'])
        non_gas_set = set(participant.PUPRN).difference(gas_set)
        
        # check that no participants in the non gas set have a gas meter
        logic = read_type.deviceType == 'GPF'
        non_gas_set = non_gas_set.difference(set(read_type.loc[logic, 'PUPRN'].to_list()))
        
        # take summary files and for elec_only_puprn copy the electric column into the total column
        summary_data_list = ['annual_daily_'+year, 'annual_hh_'+year, 'monthly_daily_'+year, 
                             'monthly_hh_'+year, 'temp_banded_daily_'+year,'temp_banded_hh_'+year]
        if heating_season:
            end_year = year
            start_year = str(int(year)-1)
            summary_data_list.extend(['heating_season_daily_'+start_year+'_'+end_year,
                                      'heating_season_hh_'+start_year+'_'+end_year])
        
        for summary_data_i in summary_data_list:
            summary_data = self.my_input_data[summary_data_i]
            if 'Clean_elec_net_Wh_hh_mean' in summary_data.columns:
                elec_column = 'Clean_elec_net_Wh_hh_mean'
                total_column = 'Clean_total_Wh_hh_mean'
            elif 'Clean_elec_net_kWh_d_mean' in summary_data.columns:
                elec_column = 'Clean_elec_net_kWh_d_mean'
                total_column = 'Clean_total_kWh_d_mean'
            
            summary_data.loc[summary_data.PUPRN.isin(non_gas_set), total_column] = summary_data.loc[summary_data.PUPRN.isin(non_gas_set), elec_column]     
        
        
    def preprocess_data(self):
        # Preprocessing steps necessary to support subsequent calculations.
        # Mainly recoding of contextual categorical variables to support 
        # meaningful segmentation and merging categories for SDC 
        serl_survey = self.my_input_data['serl_survey']
        epc = self.my_input_data['epc']
        participant = self.my_input_data['participant']
        exporters = self.my_input_data['exporters']
        # OCCUPANT
        # Number of occupants
        serl_survey['num_occupants'] = np.nan
        logic = (serl_survey.C1_new >= 1) & (serl_survey.C1_new <= 5)
        serl_survey.loc[logic,'num_occupants'] = serl_survey.loc[logic,'C1_new']
        logic = serl_survey.C1_new >= 6
        serl_survey.loc[logic,'num_occupants'] = '>=6'
        logic = serl_survey.C1_new <=0
        serl_survey.loc[logic,'num_occupants'] = 'No data'
        # BUILDING
        # number of bedrooms
        serl_survey['num_bedrooms']= np.nan
        logic = (serl_survey.B6 >= 1) & (serl_survey.B6 <= 4)
        serl_survey.loc[logic,'num_bedrooms'] = serl_survey.loc[logic,'B6']
        logic = (serl_survey.B6 >=5) 
        serl_survey.loc[logic,'num_bedrooms'] = '>=5'
        logic = (serl_survey.B6 <=0)
        serl_survey.loc[logic,'num_bedrooms'] = 'No data'
        # floor area
        epc['floor_area_banded'] = np.nan
        logic = epc.totalFloorArea <=50
        epc.loc[logic,'floor_area_banded'] = '50 or less'
        logic = (epc.totalFloorArea > 50) & (
            epc.totalFloorArea <= 100)
        epc.loc[logic,'floor_area_banded'] = '50 to 100'
        logic = (epc.totalFloorArea > 100) & (
            epc.totalFloorArea <= 150)
        epc.loc[logic,'floor_area_banded'] = '101 to 150'
        logic = (epc.totalFloorArea >150) & (epc.totalFloorArea <=200)
        epc.loc[logic,'floor_area_banded'] = '151 to 200'
        logic = epc.totalFloorArea > 200
        epc.loc[logic,'floor_area_banded'] = 'Over 200'
        # boiler type
        serl_survey['boiler_type'] = np.nan
        logic = (serl_survey.A3_sum == 1) & (
            serl_survey.A302 == 1)
        serl_survey.loc[logic,'boiler_type'] = 'Gas boiler'
        logic = (serl_survey.A3_sum == 1) & (
            serl_survey.A303 == 1)
        serl_survey.loc[logic,'boiler_type'] = 'Electric storage radiators'
        logic = (serl_survey.A3_sum == 1) & (
            serl_survey.A304 == 1)
        serl_survey.loc[logic,'boiler_type'] = 'Electric radiators'
        logic = (serl_survey.A3_sum == 1) & (
            serl_survey.A305 == 1)
        serl_survey.loc[logic,'boiler_type'] = 'Other electric'
        logic = (serl_survey.A3_sum == 1) & (
            serl_survey.A306 == 1)
        serl_survey.loc[logic,'boiler_type'] = 'Oil'
        logic = (serl_survey.A3_sum == 1) & (
            serl_survey.A307 == 1)
        serl_survey.loc[logic,'boiler_type'] = 'Solid fuel'
        logic = (serl_survey.A3_sum == 1) & (
            serl_survey.A308 == 1)
        serl_survey.loc[logic,'boiler_type'] = 'Biomass'
        logic = (serl_survey.A3_sum == 1) & (
            serl_survey.A309 == 1)
        serl_survey.loc[logic,'boiler_type'] = 'District or community'
        logic = (serl_survey.A3_sum == 1) & (
            serl_survey.A310 == 1)
        serl_survey.loc[logic,'boiler_type'] = 'Other'
        logic = (serl_survey.A3_sum > 1) & (
            serl_survey.A302 == 1)
        serl_survey.loc[logic,'boiler_type'] = 'Gas boiler plus other'
        logic = (serl_survey.A3_sum > 1) & (
            serl_survey.A302 == 0)
        serl_survey.loc[logic,'boiler_type'] = 'Other mix'
        logic = (serl_survey.A3_sum == 0) | (
            (serl_survey.A3_sum == 1) & (serl_survey.A301 == 1))
        serl_survey.loc[logic,'boiler_type'] = 'None'
        # create a new dataframe of PUPRN which we infer have PV
        solar_puprn = pd.DataFrame(columns=['PUPRN',
                                            'has_pv'])

        # building type
        serl_survey['building_type'] = np.nan
        serl_survey.loc[serl_survey['B1'] == 1, 'building_type'] = 'Detached'
        serl_survey.loc[serl_survey['B1'] == 2, 'building_type'] = 'Semi-detached'
        serl_survey.loc[serl_survey['B1'] == 3, 'building_type'] = 'Terraced'
        serl_survey.loc[serl_survey['B1'] == 4, 'building_type'] = 'Purpose-built flat'
        serl_survey.loc[serl_survey['B1'] == 5, 'building_type'] = 'Converted flat or shared house'
        serl_survey.loc[serl_survey['B1'] == 6, 'building_type'] = 'Commercial building'
        serl_survey.loc[serl_survey['B1'] == -2, 'building_type'] = 'No answer'
        # tenure
        serl_survey['tenure'] = np.nan
        serl_survey.loc[serl_survey['B4'] == 1, 'tenure'] = 'Own outright or mortgage'
        serl_survey.loc[serl_survey['B4'] == 2, 'tenure'] = 'Part-own part-rent'
        serl_survey.loc[serl_survey['B4'] == 3, 'tenure'] = 'Private rent'
        serl_survey.loc[serl_survey['B4'] == 4, 'tenure'] = 'Social rent'
        serl_survey.loc[serl_survey['B4'] == 5, 'tenure'] = 'Rent free'
        serl_survey.loc[serl_survey['B4'] == -2, 'tenure'] = 'No answer'
        # building age
        serl_survey['building_age'] = np.nan
        serl_survey.loc[serl_survey['B9'] == 1, 'building_age'] = 'Before 1900'
        serl_survey.loc[serl_survey['B9'] == 2, 'building_age'] = '1900 - 1929'
        serl_survey.loc[serl_survey['B9'] == 3, 'building_age'] = '1930 - 1949'
        serl_survey.loc[serl_survey['B9'] == 4, 'building_age'] = '1950 - 1975'
        serl_survey.loc[serl_survey['B9'] == 5, 'building_age'] = '1976 - 1990'
        serl_survey.loc[serl_survey['B9'] == 6, 'building_age'] = '1990 - 2002'
        serl_survey.loc[serl_survey['B9'] == 7, 'building_age'] = '2003 onwards'
        serl_survey.loc[serl_survey['B9'] == -1, 'building_age'] = 'Unknown'
        serl_survey.loc[serl_survey['B9'] == -2, 'building_age'] = 'No answer'
        
        # has ev
        serl_survey['has_ev'] = np.nan
        serl_survey.loc[serl_survey['C5'] == 1, 'has_ev'] = 'Yes'
        serl_survey.loc[serl_survey['C5'] == 2, 'has_ev'] = 'No'
        serl_survey.loc[serl_survey['C5'] == -1, 'has_ev'] = 'Unknown'
        serl_survey.loc[serl_survey['C5'] == -2, 'has_ev'] = 'No answer'

        # from epc data
        set_pv = set()
        # deal with England and Wales / Scotland separately as variables have different values
        # england and wales photosupply variable
        epc_eng_wal = epc.loc[epc.epcVersion == 'England and Wales']
        logic = (epc_eng_wal.photoSupply.astype(float) > 0)
        set_pv.update(epc_eng_wal.loc[logic,'PUPRN'].tolist())
        # scottish photosupply variable
        # this line finds the first digit in the photosupply variable for scottish
        # data and asks if it's greater than 0. This number will either represent peak power
        # or % roof area covered. 
        epc_scot = epc.loc[epc.epcVersion == 'Scotland']
        logic = (epc_scot.photoSupply.str.extract('(\d+)').astype(float) > 0).squeeze()
        set_pv.update(epc_scot.loc[logic,'PUPRN'].tolist())
        # and from presence of active export readings this year - from module 1 outputs
        set_pv.update(exporters.loc[:,0].tolist())
        for this_puprn in participant.PUPRN.unique():
            my_dict = {}
            my_dict['PUPRN'] = this_puprn
            my_dict['has_pv'] = this_puprn in set_pv
            solar_puprn = solar_puprn.append(my_dict, ignore_index=True)
        self.my_input_data['solar_puprn'] = solar_puprn
        
        # merge segments to make sure summary statistics meet SDC requirem]ents
        # create new columns appended with _merge for these variables
        # Building type	Commercial building with no answer
        serl_survey.loc[:,'building_type_merge'] = serl_survey.loc[:,'building_type']
        logic = (serl_survey.building_type_merge == 'No answer') | (
            serl_survey.building_type_merge == 'Commercial building')
        serl_survey.loc[logic,'building_type_merge'] = 'Commercial building or no answer'
        
        # boiler type merge all electric options, merge all non metered and district and merge all other and none
        # do separate merges for electricity and gas consumption stats because if no gas heating then
        # unlikely to have gas data so need to merge many non-gas groups to get n>10
        serl_survey.loc[:,'boiler_type_merge_for_elec_consumption'] = serl_survey.loc[:,'boiler_type']
        logic = (serl_survey.boiler_type_merge_for_elec_consumption == 'Oil') | (
            serl_survey.boiler_type_merge_for_elec_consumption == 'Solid fuel') | (
            serl_survey.boiler_type_merge_for_elec_consumption == 'Biomass')
        serl_survey.loc[logic,'boiler_type_merge_for_elec_consumption'] = 'Oil, solid fuel or biomass'
        logic = (serl_survey.boiler_type_merge_for_elec_consumption == 'Other mix') | (
            serl_survey.boiler_type_merge_for_elec_consumption == 'Other')
        serl_survey.loc[logic,'boiler_type_merge_for_elec_consumption'] = 'Other or other mix'
        
        # for gas consumption, make anything that isn't gas heating one group
        serl_survey.loc[:,'boiler_type_merge_for_gas_consumption'] = serl_survey.loc[:,'boiler_type']
        logic = (serl_survey.boiler_type_merge_for_gas_consumption == 'Gas boiler') | (
            serl_survey.boiler_type_merge_for_gas_consumption == 'Gas boiler plus other')
        serl_survey.loc[~logic,'boiler_type_merge_for_gas_consumption'] = 'Not gas'
        
        
        # ev ownership merge no answer with don't know
        serl_survey.loc[:,'building_age_merge'] = serl_survey.loc[:,'building_age']
        logic = (serl_survey.building_age_merge == 'Unknown') | (serl_survey.building_age_merge == 'No answer')
        serl_survey.loc[logic,'building_age_merge'] = 'No data'
        
        
        # ev ownership merge no answer with don't know
        serl_survey.loc[:,'has_ev_merge'] = serl_survey.loc[:,'has_ev']
        logic = (serl_survey.has_ev_merge == 'Unknown') | (serl_survey.has_ev_merge == 'No answer')
        serl_survey.loc[logic,'has_ev_merge'] = 'No data'
        
        # epc band A with B and F with G
        epc.loc[:, 'currentEnergyRating_merge'] = epc.loc[:, 'currentEnergyRating']
        logic = (epc.currentEnergyRating_merge == 'A') | (epc.currentEnergyRating_merge == 'B')
        epc.loc[logic,'currentEnergyRating_merge'] = 'A and B'
        logic = (epc.currentEnergyRating_merge == 'F') | (epc.currentEnergyRating_merge == 'G')
        epc.loc[logic,'currentEnergyRating_merge'] = 'F and G'

        
        
        # create a list with each item formed of a list containing the segmentation variable
        # and the data to segment on e.g. ['num_bedrooms','serl_survey']
        # this will be iterated over and passed to the diurnal_stats function
        self.segmentation_list = [
            # OCCUPANT
            # Number of occupants
            ['num_occupants','serl_survey'],
            # IMD quintile
            ['IMD_quintile','participant'],
            # BEHAVIOURAL
            # Weekday vs weekend
            # NOTE: cannot do with input data yet
            
            # BUILDING
            # EPC rating
            ['currentEnergyRating_merge', 'epc'],
            # number of bedrooms
            ['num_bedrooms','serl_survey'],
            # Property type
            ['building_type_merge','serl_survey'],
            # Building age
            ['building_age_merge','serl_survey'],
            # Floor area
            ['floor_area_banded','epc'],
            # Tenure
            ['tenure','serl_survey'],
            # Boiler type
            ['boiler_type_merge_for_elec_consumption','serl_survey'],
            ['boiler_type_merge_for_gas_consumption','serl_survey'],
            # PV 
            ['has_pv','solar_puprn'],
            # APPLIANCE/ENERGY USING EQUIPMENT
            # Main heating fuel
            # this is implicit in boiler type, no need for further category
            # EV
            ['has_ev_merge','serl_survey'],
            # CONTEXTUAL
            # Region
            ['Region','participant']
                ]
        # create list of temperature bands to iterate over when producing 
        # temperature banded stats
        self.temperature_band_list = [
            '0_to_5',
            '5_to_10',
            '10_to_15',
            '15_to_20',
            '4.5_to_5.5'
            ]
        
    def produce_stats(self, 
                      year: str,
                      resolution: str, #<--- 'diurnal' or 'monthly' or 'annual'
                      heating_season=False): 
        # STATISTICS FOR THE SPECIFIED COMPLETE YEAR
        # FULL SAMPLE
        if resolution == 'diurnal':
            temp_df = self.summary_stats(self.my_input_data['annual_hh_'+year],
                                         resolution=resolution,
                                        subsample='all',
                                        time_period=year)
        elif resolution == 'monthly':
            temp_df = self.summary_stats(self.my_input_data['monthly_daily_'+year],
                                         resolution=resolution,
                                        subsample='all',
                                        time_period=year)
        elif resolution == 'annual':
            temp_df = self.summary_stats(self.my_input_data['annual_daily_'+year],
                                         resolution=resolution,
                                        subsample='all',
                                        time_period=year)
        self.my_output_df = self.my_output_df.append(temp_df, ignore_index=True)
        # CONTEXTUAL
        segmentation_list = self.segmentation_list
        # iterate over segmentation list and run diurnal_stats function given
        # items
        for item in segmentation_list:   
            if resolution == 'diurnal':
                temp_df = self.summary_stats(self.my_input_data['annual_hh_'+year],
                                             resolution=resolution,
                                            subsample='all',
                                            time_period=year,
                                            segmentation_variable_1=item[0],
                                            data_to_segment_puprn_on=self.my_input_data[item[1]])
            elif resolution == 'monthly':
                temp_df = self.summary_stats(self.my_input_data['monthly_daily_'+year],
                                             resolution=resolution,
                                            subsample='all',
                                            time_period=year,
                                            segmentation_variable_1=item[0],
                                            data_to_segment_puprn_on=self.my_input_data[item[1]])
            elif resolution == 'annual':
                temp_df = self.summary_stats(self.my_input_data['annual_daily_'+year],
                                             resolution=resolution,
                                            subsample='all',
                                            time_period=year,
                                            segmentation_variable_1=item[0],
                                            data_to_segment_puprn_on=self.my_input_data[item[1]])
            self.my_output_df = self.my_output_df.append(temp_df, ignore_index=True)
        # Temperature banded stats
        for this_temperature_band in self.temperature_band_list:
            if resolution == 'diurnal':
                logic = self.my_input_data['temp_banded_hh_'+year].temp_band == this_temperature_band
                temp_df = self.summary_stats(
                    self.my_input_data['temp_banded_hh_'+year].loc[logic,:],
                    resolution=resolution,
                    subsample='all',
                    time_period=year,
                    weekday_weekend=None)
            elif resolution == 'annual':
                logic = self.my_input_data['temp_banded_daily_'+year].temp_band == this_temperature_band
                temp_df = self.summary_stats(
                    self.my_input_data['temp_banded_daily_'+year].loc[logic,:],
                    resolution=resolution,
                    subsample='all',
                    time_period=year,
                    weekday_weekend=None)
            temp_df['segmentation_variable_1'] = 'temperature band'
            temp_df['segment_1_value'] = this_temperature_band
            self.my_output_df = self.my_output_df.append(temp_df, ignore_index=True)
        # weekday / weekend
        if resolution == 'diurnal':
            temp_df = self.summary_stats(self.my_input_data['annual_hh_'+year],
                                         resolution=resolution,
                                        subsample='all',
                                        time_period=year,
                                        weekday_weekend='weekday')
            temp_df['segmentation_variable_1'] = 'weekday_weekend'
            temp_df['segment_1_value'] = 'weekday'
            self.my_output_df = self.my_output_df.append(temp_df, ignore_index=True)
            temp_df = self.summary_stats(self.my_input_data['annual_hh_'+year],
                                         resolution=resolution,
                                        subsample='all',
                                        time_period=year,
                                        weekday_weekend='weekend')
            temp_df['segmentation_variable_1'] = 'weekday_weekend'
            temp_df['segment_1_value'] = 'weekend'
            self.my_output_df = self.my_output_df.append(temp_df, ignore_index=True)
        elif resolution == 'monthly':
            temp_df = self.summary_stats(self.my_input_data['monthly_daily_'+year],
                                         resolution=resolution,
                                        subsample='all',
                                        time_period=year,
                                        weekday_weekend='weekday')
            temp_df['segmentation_variable_1'] = 'weekday_weekend'
            temp_df['segment_1_value'] = 'weekday'
            self.my_output_df = self.my_output_df.append(temp_df, ignore_index=True)
            temp_df = self.summary_stats(self.my_input_data['monthly_daily_'+year],
                                         resolution=resolution,
                                        subsample='all',
                                        time_period=year,
                                        weekday_weekend='weekend')
            temp_df['segmentation_variable_1'] = 'weekday_weekend'
            temp_df['segment_1_value'] = 'weekend'
            self.my_output_df = self.my_output_df.append(temp_df, ignore_index=True)
        elif resolution == 'annual':
            temp_df = self.summary_stats(self.my_input_data['annual_daily_'+year],
                                         resolution=resolution,
                                        subsample='all',
                                        time_period=year,
                                        weekday_weekend='weekday')
            temp_df['segmentation_variable_1'] = 'weekday_weekend'
            temp_df['segment_1_value'] = 'weekday'
            self.my_output_df = self.my_output_df.append(temp_df, ignore_index=True)
            temp_df = self.summary_stats(self.my_input_data['annual_daily_'+year],
                                         resolution=resolution,
                                        subsample='all',
                                        time_period=year,
                                        weekday_weekend='weekend')
            temp_df['segmentation_variable_1'] = 'weekday_weekend'
            temp_df['segment_1_value'] = 'weekend'
            self.my_output_df = self.my_output_df.append(temp_df, ignore_index=True)
        # heating season
        if heating_season:
            previous_year_num = pd.to_numeric(year) - 1
            previous_year_str = f'{previous_year_num}'
            hs_str = 'heating_season_'+previous_year_str+'_'+year
            if resolution == 'diurnal':
                temp_df = self.summary_stats(self.my_input_data['heating_season_hh_'+previous_year_str+'_'+year],
                                             resolution=resolution,
                                             subsample='all',
                                             time_period=hs_str)
                self.my_output_df = self.my_output_df.append(temp_df, ignore_index=True)
            elif resolution == 'monthly':
                # do nothing as no heating season stats for monthly
                pass
                
            elif resolution == 'annual':
                temp_df = self.summary_stats(self.my_input_data['heating_season_daily_'+previous_year_str+'_'+year],
                                             resolution=resolution,
                                             subsample='all',
                                             time_period=hs_str)
                self.my_output_df = self.my_output_df.append(temp_df, ignore_index=True)           
        
        
    def summary_stats(self,
                      input_df,
                      resolution, #<--- 'diurnal', 'monthly', 'annual'
                      subsample,
                      time_period,
                      segmentation_variable_1=None,
                      data_to_segment_puprn_on=None,
                      weekday_weekend='both'):
        # Calculate summary statistics, optionally split on a
        # segmentation variable
        
        output_df = pd.DataFrame(columns=['fuel',
                                        'unit',
                                        'summary_stat',
                                        'subsample',
                                        'summary_time',
                                        'time_period',
                                        'segmentation_variable_1',
                                        'segment_1_value',
                                        'value',
                                        'n_sample', 
                                        'n_statistic',
                                        'mean_temp',
                                        'mean_hdd'])
        
        if (subsample == 'all') and (segmentation_variable_1 is None):
            print(f'Resolution: {resolution}. Full sample, no segmentation.')
            # no segmentation specified Note: this part includes segmentation on weekday/weekend
            if resolution == 'diurnal':
                # produce diurnal statistics 
                temp_df = self.hh_stats_looper(input_df, subsample, time_period, weekday_weekend)

            elif resolution == 'monthly':
                temp_df = self.month_stats_looper(input_df, subsample, time_period, weekday_weekend)
            elif resolution == 'annual':
                temp_df = self.annual_stats(input_df, subsample, time_period, weekday_weekend)
            output_df = output_df.append(temp_df)
            return output_df
        elif (subsample == 'all') and (segmentation_variable_1 is not None):
            print(f'Resolution: {resolution}. Segmentation needed on {segmentation_variable_1}')
            # this iterates through each segment value in this segmentation variable
            # and runs the relevant stats function to calculate summary stats for it
            for this_segment_1_value in data_to_segment_puprn_on[segmentation_variable_1].unique():
                logic = data_to_segment_puprn_on[segmentation_variable_1] == this_segment_1_value
                this_subset_of_puprn = data_to_segment_puprn_on.loc[logic,'PUPRN'].unique()
                logic = input_df.PUPRN.isin(this_subset_of_puprn)
                this_subset_of_input_df = input_df.loc[logic,:]
                print(f'Segment value: {this_segment_1_value}, N={len(this_subset_of_puprn)}')
                if resolution == 'diurnal':
                    temp_df = self.hh_stats_looper(this_subset_of_input_df, subsample,
                                                   time_period,
                                                   weekday_weekend='both')
                elif resolution == 'monthly':
                    temp_df = self.month_stats_looper(this_subset_of_input_df, subsample,
                                                      time_period,
                                                      weekday_weekend='both')
                elif resolution == 'annual':
                    temp_df = self.annual_stats(this_subset_of_input_df, subsample,
                                                   time_period,
                                                   weekday_weekend='both')
                temp_df['segmentation_variable_1'] = segmentation_variable_1
                temp_df['segment_1_value'] = this_segment_1_value
                output_df = output_df.append(temp_df)
            return output_df

    def hh_stats_looper(self,
                        input_df, 
                        subsample,
                        time_period,
                        weekday_weekend): #<--- 'both', 'weekday' or 'weekend', or None for temperature banded data
        # This simply loops over each hh in the day
        # and calculates and returns summary statistics for each fuel type
        
        output_df = pd.DataFrame()
        for this_hh in input_df.Hh_local_time.unique():
            this_summary_time = self.return_summary_time(resolution='diurnal',
                                                     period_number=this_hh,
                                                     time_period=time_period)
            if weekday_weekend is None:#<--- temp banded data does not have weekday_weekend col
                logic = (input_df.Hh_local_time == this_hh)
            else:
                logic = (input_df.Hh_local_time == this_hh) & (
                    input_df.weekday_weekend == weekday_weekend) 
            subset = input_df.loc[logic,:]
            
            # Fuel: Electricity
            this_fuel = 'Clean_elec_net_Wh_hh_mean'
            temp_df = self.fuel_summary_stats(subset,
                                             'diurnal',
                                             subsample,
                                             this_summary_time,
                                             time_period,
                                             this_fuel)
            output_df = output_df.append(temp_df)
            # Fuel: Gas
            this_fuel = 'Clean_gas_Wh_hh_mean'
            temp_df = self.fuel_summary_stats(subset,
                                              'diurnal',
                                             subsample,
                                             this_summary_time,
                                             time_period,
                                             this_fuel)
            output_df = output_df.append(temp_df)
            # Fuel: Total
            this_fuel = 'Clean_total_Wh_hh_mean'
            temp_df = self.fuel_summary_stats(subset,
                                              'diurnal',
                                             subsample,
                                             this_summary_time,
                                             time_period,
                                             this_fuel)
            output_df = output_df.append(temp_df)
        output_df['weekday_weekend'] = weekday_weekend
        return output_df
        
    def month_stats_looper(self,
                        input_df, 
                        subsample,
                        time_period,
                        weekday_weekend): #<--- 'both' 'weekday' or 'weekend'
        # This simply loops over each month in the year
        # and calculates and returns summary statistics for each fuel type
        
        output_df = pd.DataFrame()
        for this_month in input_df.month_local_time.unique():
            this_summary_time = self.return_summary_time(resolution='monthly',
                                                     period_number=this_month,
                                                     time_period=time_period)
            
            if weekday_weekend is None:#<--- temp banded data does not have weekday_weekend col
                logic = (input_df.month_local_time == this_month)
            else:
                logic = (input_df.month_local_time == this_month) & (
                    input_df.weekday_weekend == weekday_weekend)
            subset = input_df.loc[logic,:]
            
            # Fuel: Electricity
            this_fuel = 'Clean_elec_net_kWh_d_mean'
            temp_df = self.fuel_summary_stats(subset,
                                              'monthly',
                                             subsample,
                                             this_summary_time,
                                             time_period,
                                             this_fuel)
            output_df = output_df.append(temp_df)
            # Fuel: Gas
            this_fuel = 'Clean_gas_kWh_d_mean'
            temp_df = self.fuel_summary_stats(subset,
                                              'monthly',
                                             subsample,
                                             this_summary_time,
                                             time_period,
                                             this_fuel)
            output_df = output_df.append(temp_df)
            # Fuel: Total
            this_fuel = 'Clean_total_kWh_d_mean'
            temp_df = self.fuel_summary_stats(subset,
                                              'monthly',
                                             subsample,
                                             this_summary_time,
                                             time_period,
                                             this_fuel)
            output_df = output_df.append(temp_df)
        output_df['weekday_weekend'] = weekday_weekend
        return output_df
    
    def annual_stats(self,
                    input_df, 
                    subsample,
                    time_period,
                    weekday_weekend): #<--- 'both' 'weekday' or 'weekend'
        # Similar to hh and month stats loopers, but this does not require 
        # looping. Calculates and returns summary statistics for each fuel type
        
        output_df = pd.DataFrame()
        this_summary_time = time_period
        if weekday_weekend is None: #<--- temp banded data does not have weekday_weekend col
            subset = input_df
        else:
            logic = input_df.weekday_weekend == weekday_weekend
            subset = input_df.loc[logic,:]
            
        # Fuel: Electricity
        this_fuel = 'Clean_elec_net_kWh_d_mean'
        temp_df = self.fuel_summary_stats(subset,
                                          'annual',
                                         subsample,
                                         this_summary_time,
                                         time_period,
                                         this_fuel)
        output_df = output_df.append(temp_df)
        # Fuel: Gas
        this_fuel = 'Clean_gas_kWh_d_mean'
        temp_df = self.fuel_summary_stats(subset,
                                          'annual',
                                         subsample,
                                         this_summary_time,
                                         time_period,
                                         this_fuel)
        output_df = output_df.append(temp_df)
        # Fuel: Total
        this_fuel = 'Clean_total_kWh_d_mean'
        temp_df = self.fuel_summary_stats(subset,
                                          'annual',
                                         subsample,
                                         this_summary_time,
                                         time_period,
                                         this_fuel)
        output_df = output_df.append(temp_df)
        output_df['weekday_weekend'] = weekday_weekend
        return output_df
    
    def return_summary_time(self,
                            resolution,
                            period_number,
                            time_period):
        # Produces the 'summary_time' variable in module 3 stats outputs
        # first check if it's heating season data
        # if time_period[0:7] == 'heating':
        #     return time_period
        if resolution == 'monthly':
            my_datetime = datetime(pd.to_numeric(time_period),
                                    pd.to_numeric(period_number),
                                    1)
            my_datetime_string = my_datetime.strftime('%b-%y')        
            return my_datetime_string
        if resolution == 'diurnal':
            if time_period[0:7] == 'heating':
               my_datetime = datetime(2000, # doesn't matter what year we use as the string saves only the H:M
                                                1,
                                                1,
                                                0,
                                                0) 
            else:
                my_datetime = datetime(pd.to_numeric(time_period),
                                                1,
                                                1,
                                                0,
                                                0)
            my_timedelta = pd.Timedelta(30*period_number, unit='minutes')
            my_datetime = my_datetime + my_timedelta
            my_datetime_string = my_datetime.strftime('%H:%M')        
            return my_datetime_string
    
    def fuel_summary_stats(self,
                           input_df,
                           resolution,
                           subsample, 
                           this_summary_time, 
                           time_period,
                           this_fuel):
        # Calculates a set of summary stats for a given fuel and set of 
        # input data e.g. data for a specific month, or half-hour
        output_df = pd.DataFrame()
        my_dict = {}
        decimal_places = 3
        if (this_fuel == 'Clean_elec_net_Wh_hh_mean') | (
                this_fuel == 'Clean_elec_net_kWh_d_mean'):
            my_dict['fuel'] = 'Electricity'
            my_dict['mean_temp'] = np.around(input_df.temp_elec_C.mean(), decimal_places)
        elif (this_fuel == 'Clean_gas_Wh_hh_mean') | (
                this_fuel == 'Clean_gas_kWh_d_mean'):
            my_dict['fuel'] = 'Gas'
            my_dict['mean_temp'] = np.around(input_df.temp_gas_C.mean(), decimal_places)
        elif (this_fuel == 'Clean_total_Wh_hh_mean') | (
                this_fuel == 'Clean_total_kWh_d_mean'):
            my_dict['fuel'] = 'Total'
            my_dict['mean_temp'] = np.around(input_df.temp_weighted_C.mean(), decimal_places)
        
        if resolution == 'diurnal':
            my_dict['unit'] = 'Wh'      
        elif (resolution == 'monthly') | (resolution == 'annual'):
            my_dict['unit'] = 'kWh'
            if this_fuel == 'Clean_elec_net_kWh_d_mean':
                my_dict['mean_hdd'] = np.around(input_df.hdd_elec.mean(), decimal_places)
            elif this_fuel == 'Clean_gas_kWh_d_mean':
                my_dict['mean_hdd'] = np.around(input_df.hdd_gas.mean(), decimal_places)
            elif this_fuel == 'Clean_total_kWh_d_mean':
                my_dict['mean_hdd'] = np.around(input_df.hdd_weighted_C.mean(), decimal_places)
        
        my_dict['subsample'] = subsample
        my_dict['summary_time'] = this_summary_time
        my_dict['time_period'] = time_period
        # mean
        my_dict['summary_stat'] = 'mean'
        my_dict['value'] = np.around(input_df[this_fuel].mean(), decimal_places)
        my_dict['n_sample'] = input_df[this_fuel].count()
        my_dict['n_statistic'] = input_df[this_fuel].count()
        my_dict['decimal_places'] = decimal_places
        output_df = output_df.append(my_dict, ignore_index=True)      
        #std dev
        my_dict['summary_stat'] = 'standard deviation'
        my_dict['value'] = np.around(input_df[this_fuel].std(), decimal_places)
        my_dict['decimal_places'] = decimal_places
        output_df = output_df.append(my_dict, ignore_index=True)
        # median
        rounded_q, my_n, decimal_places = self.sdc_quantile(input_df[this_fuel], 0.5)
        my_dict['summary_stat'] = 'median'
        my_dict['value'] = rounded_q
        my_dict['n_statistic'] = my_n
        my_dict['decimal_places'] = decimal_places
        output_df = output_df.append(my_dict, ignore_index=True)
        # 75th percentile
        rounded_q, my_n, decimal_places = self.sdc_quantile(input_df[this_fuel], 0.75)
        my_dict['summary_stat'] = '75th percentile'
        my_dict['value'] = rounded_q
        my_dict['n_statistic'] = my_n
        my_dict['decimal_places'] = decimal_places
        output_df = output_df.append(my_dict, ignore_index=True)
        # 25th percentile
        rounded_q, my_n, decimal_places = self.sdc_quantile(input_df[this_fuel], 0.25)
        my_dict['summary_stat'] = '25th percentile'
        my_dict['value'] = rounded_q
        my_dict['n_statistic'] = my_n
        my_dict['decimal_places'] = decimal_places
        output_df = output_df.append(my_dict, ignore_index=True)
        return output_df
    
    def sdc_quantile(self,
                     input_array,
                     quantile):
        
        # check if the length of the non-nan input array is too small for this function
        if (pd.notnull(input_array)).sum() <= 10:
            rounded_q = np.nan
            my_n = np.nan   
            decimal_places = np.nan
            return rounded_q, my_n, decimal_places
        # find the quantile, then find the 10 closest values and calculate their mean
        decimal_places = 3 # to match the rest of the stats
        my_q = np.nanquantile(input_array, quantile)
        my_n = 10
        closest_10_idx = (input_array - np.nanquantile(input_array, quantile)).abs().nsmallest(my_n).index
        my_q = input_array.loc[closest_10_idx].mean()
        rounded_q = np.around(my_q, decimal_places)
        
        # This code is an alternative way of meeting SDC requirements by rounding
        # NB this can cause some odd behaviour with different segments rounded by different amounts
        # calculate quantile, check n for rounded values, if n too small, 
        # continue to round down further. Check for odd values indicating issue
        # with input array e.g. not enough non-nan data
        # my_n = 0
        # my_q = np.nanquantile(input_array, quantile)
        # decimal_places = 4
        # while my_n <= 10:
        #     decimal_places = decimal_places - 1
        #     rounded_q = np.around(my_q, decimal_places)
        #     if np.isnan(rounded_q):
        #         rounded_q = np.nan
        #         my_n = np.nan
        #         decimal_places = np.nan
        #         return rounded_q, my_n, decimal_places
        #     elif rounded_q == 0:
        #         rounded_q = np.nan
        #         my_n = np.nan
        #         decimal_places = np.nan
        #         return rounded_q, my_n, decimal_places
        #     rounded_values = np.around(input_array, decimal_places)
        #     logic = rounded_values == rounded_q
        #     my_n = logic.sum()
            
        # if rounded_q == 0:
        #     rounded_q = np.nan
        #     my_n = np.nan
        #     decimal_places= np.nan
            
        return rounded_q, my_n, decimal_places
    
    def hist_values(self, year:str):
        # produce histogram data for mean daily consumption for each fuel type
        # for specified year
        output_df = pd.DataFrame()
        # Electricity
        this_fuel = 'Clean_elec_net_kWh_d_mean'
        (bin_values, bin_edges, patches) = plt.hist(
            self.my_input_data['annual_daily_'+year].loc[:,this_fuel],
            bins=30)
        for i in range(len(bin_values)):
            my_dict={}
            my_dict['year'] = year
            my_dict['fuel'] = this_fuel
            my_dict['bin_value'] = bin_values[i]
            my_dict['bin_l_edge'] = bin_edges[i]
            my_dict['bin_r_edge'] = bin_edges[i+1]
            output_df = output_df.append(my_dict, ignore_index=True)
        # Gas
        this_fuel = 'Clean_gas_kWh_d_mean'
        (bin_values, bin_edges, patches) = plt.hist(
            self.my_input_data['annual_daily_'+year].loc[:,this_fuel],
            bins=30)
        for i in range(len(bin_values)):
            my_dict={}
            my_dict['year'] = year
            my_dict['fuel'] = this_fuel
            my_dict['bin_value'] = bin_values[i]
            my_dict['bin_l_edge'] = bin_edges[i]
            my_dict['bin_r_edge'] = bin_edges[i+1]
            output_df = output_df.append(my_dict, ignore_index=True)
        # Total
        this_fuel = 'Clean_total_kWh_d_mean'
        (bin_values, bin_edges, patches) = plt.hist(
            self.my_input_data['annual_daily_'+year].loc[:,this_fuel],
            bins=30)
        for i in range(len(bin_values)):
            my_dict={}
            my_dict['year'] = year
            my_dict['fuel'] = this_fuel
            my_dict['bin_value'] = bin_values[i]
            my_dict['bin_l_edge'] = bin_edges[i]
            my_dict['bin_r_edge'] = bin_edges[i+1]
            output_df = output_df.append(my_dict, ignore_index=True)
        return output_df
    
    def hist_values_compare_2_years(self, year1:str, year2:str):
        # produce histogram data for mean daily consumption for each fuel type
        # for specified year, for all days (not split by weekend/weekday) and generate 
        # generate for puprn where the value is not-nan for both years
        output_df = pd.DataFrame()
        summary_stats_df = pd.DataFrame()
        # get only dwellings which appear in both years
        annual_daily_year1 = self.my_input_data['annual_daily_'+year1].copy()
        annual_daily_year2 = self.my_input_data['annual_daily_'+year2].copy()
        serl_survey = self.my_input_data['serl_survey']
        epc = self.my_input_data['epc']
        
        # filter data - don't want to separate by weekday weekend
        annual_daily_year1 = annual_daily_year1.loc[annual_daily_year1.weekday_weekend == 'both',]
        annual_daily_year2 = annual_daily_year2.loc[annual_daily_year2.weekday_weekend == 'both',]
        
        # only want to generate stats for purpn with non-nan data in both years
        elec_puprn_year1 = set(annual_daily_year1.loc[~pd.isnull(annual_daily_year1.Clean_elec_net_kWh_d_mean), 'PUPRN'])
        elec_puprn_year2 = set(annual_daily_year2.loc[~pd.isnull(annual_daily_year2.Clean_elec_net_kWh_d_mean), 'PUPRN'])
        elec_puprn = elec_puprn_year1.intersection(set(elec_puprn_year2))
        
        gas_puprn_year1 = set(annual_daily_year1.loc[~pd.isnull(annual_daily_year1.Clean_gas_kWh_d_mean), 'PUPRN'])
        gas_puprn_year2 = set(annual_daily_year2.loc[~pd.isnull(annual_daily_year2.Clean_gas_kWh_d_mean), 'PUPRN'])
        gas_puprn = gas_puprn_year1.intersection(set(gas_puprn_year2))
        
        
        # Electricity
        for year, data in [[year1, annual_daily_year1], [year2, annual_daily_year2]]:
            this_fuel = 'Clean_elec_net_kWh_d_mean'
            data = data.loc[data.PUPRN.isin(elec_puprn)]
            elec_bins = np.arange(-5, 50, 5)
            (bin_values, bin_edges, patches) = plt.hist(np.clip( #np.clip gathers everything below the lowest bin and above the highest bin and puts them into the lowest/highest bins
                data.loc[:,this_fuel], elec_bins[0], elec_bins[-1]),
                bins=elec_bins)
            if (bin_values<10).any():
                warnings.warn('Some histogram bin values less than 10! Check ' + year + this_fuel)
            summary_stats = {}
            summary_stats['year'] = year
            summary_stats['fuel'] = 'Net Electricity'
            summary_stats['unit'] = 'kWh'
            summary_stats['subsample'] = 'participants with annual data for ' + year1 + ' and ' + year2
            summary_stats['summary_time'] = year
            summary_stats['time_period'] = year
            summary_stats['segmentation_variable_1'] = np.nan
            summary_stats['segment_1_value'] = np.nan
            summary_stats['decimal_places'] = 3
            summary_stats['weekday_weekend'] = 'both' 
            summary_stats['n_sample'] = len(data)
            summary_stats['mean_temp'] = data.loc[:,'temp_elec_C'].mean()
            summary_stats['mean_hdd'] = data.loc[:,'hdd_elec'].mean()
            summary_stats['mean_floor_area'] = epc.loc[epc.PUPRN.isin(elec_puprn) ,'totalFloorArea'].mean()
            summary_stats['n_mean_floor_area'] = len(epc.loc[epc.PUPRN.isin(elec_puprn),])
            summary_stats['mean_bedrooms'] = serl_survey.loc[(serl_survey.PUPRN.isin(elec_puprn)) & (serl_survey.B6>0), 'B6'].mean()
            summary_stats['n_mean_bedrooms'] = len(serl_survey.loc[(serl_survey.PUPRN.isin(elec_puprn)) & (serl_survey.B6>0),])
            summary_stats['mean_occupants'] = serl_survey.loc[(serl_survey.PUPRN.isin(elec_puprn)) & (serl_survey.C1_new>0), 'C1_new'].mean()
            summary_stats['n_mean_occupants'] = len(serl_survey.loc[(serl_survey.PUPRN.isin(elec_puprn)) & (serl_survey.C1_new>0), ])
        
            
            summary_stats['summary_stat'] = 'mean'
            summary_stats['value'] = np.around(data.loc[:,this_fuel].mean(),3)
            summary_stats['n_statistic'] = len(data)
            summary_stats_df = summary_stats_df.append(summary_stats, ignore_index=True)
            
            summary_stats['summary_stat'] = 'standard deviation'
            summary_stats['value'] = np.around(data.loc[:,this_fuel].std(),3)
            summary_stats_df = summary_stats_df.append(summary_stats, ignore_index=True)
            
            summary_stats['summary_stat'] = 'median'
            n_statistic = 10
            closest_10_idx = (data.loc[:,this_fuel] - np.nanquantile(data.loc[:,this_fuel], 0.5)).abs().nsmallest(n_statistic).index
            value = data.loc[closest_10_idx,this_fuel].mean()
            summary_stats['value'] = np.around(value, 3)
            summary_stats['n_statistic'] = n_statistic
            summary_stats_df = summary_stats_df.append(summary_stats, ignore_index=True)
            
            summary_stats['summary_stat'] = '25th percentile'
            closest_10_idx = (data.loc[:,this_fuel] - np.nanquantile(data.loc[:,this_fuel], 0.25)).abs().nsmallest(n_statistic).index
            value = data.loc[closest_10_idx,this_fuel].mean()
            summary_stats['value'] = np.around(value, 3)
            summary_stats_df = summary_stats_df.append(summary_stats, ignore_index=True)
            
            summary_stats['summary_stat'] = '75th percentile'
            closest_10_idx = (data.loc[:,this_fuel] - np.nanquantile(data.loc[:,this_fuel], 0.75)).abs().nsmallest(n_statistic).index
            value = data.loc[closest_10_idx,this_fuel].mean()
            summary_stats['value'] = np.around(value, 3)
            summary_stats_df = summary_stats_df.append(summary_stats, ignore_index=True)
            
            for i in range(len(bin_values)):
                my_dict={}
                my_dict['year'] = year
                my_dict['fuel'] = 'Net Electricity'
                my_dict['bin_value'] = bin_values[i]
                my_dict['bin_l_edge'] = bin_edges[i]
                my_dict['bin_r_edge'] = bin_edges[i+1]
                output_df = output_df.append(my_dict, ignore_index=True)
                
        
        # Gas
        for year, data in [[year1, annual_daily_year1], [year2, annual_daily_year2]]:
            this_fuel = 'Clean_gas_kWh_d_mean'
            data = data.loc[data.PUPRN.isin(gas_puprn)]
            gas_bins = np.arange(0,130, 10)
            (bin_values, bin_edges, patches) = plt.hist(np.clip( #np.clip gathers everything below the lowest bin and above the highest bin and puts them into the lowest/highest bins
                data.loc[:,this_fuel], gas_bins[0], gas_bins[-1]),
                bins = gas_bins)
            if (bin_values<10).any():
                warnings.warn('Some histogram bin values less than 10! Check ' + year + this_fuel)
            summary_stats = {}
            summary_stats['year'] = year
            summary_stats['fuel'] = 'Gas'
            summary_stats['unit'] = 'kWh'
            summary_stats['subsample'] = 'participants with annual data for ' + year1 + ' and ' + year2
            summary_stats['summary_time'] = year
            summary_stats['time_period'] = year
            summary_stats['segmentation_variable_1'] = np.nan
            summary_stats['segment_1_value'] = np.nan
            summary_stats['decimal_places'] = 3
            summary_stats['weekday_weekend'] = 'both' 
            summary_stats['n_sample'] = len(data)
            summary_stats['mean_temp'] = data.loc[:,'temp_gas_C'].mean()
            summary_stats['mean_hdd'] = data.loc[:,'hdd_gas'].mean()
            summary_stats['mean_floor_area'] = epc.loc[epc.PUPRN.isin(gas_puprn) ,'totalFloorArea'].mean()
            summary_stats['n_mean_floor_area'] = len(epc.loc[epc.PUPRN.isin(gas_puprn),])
            summary_stats['mean_bedrooms'] = serl_survey.loc[(serl_survey.PUPRN.isin(gas_puprn)) & (serl_survey.B6>0), 'B6'].mean()
            summary_stats['n_mean_bedrooms'] = len(serl_survey.loc[(serl_survey.PUPRN.isin(gas_puprn)) & (serl_survey.B6>0),])
            summary_stats['mean_occupants'] = serl_survey.loc[(serl_survey.PUPRN.isin(gas_puprn)) & (serl_survey.C1_new>0), 'C1_new'].mean()
            summary_stats['n_mean_occupants'] = len(serl_survey.loc[(serl_survey.PUPRN.isin(gas_puprn)) & (serl_survey.C1_new>0), ])
        
            
            summary_stats['summary_stat'] = 'mean'
            summary_stats['value'] = np.around(data.loc[:,this_fuel].mean(),3)
            summary_stats['n_statistic'] = len(data)
            summary_stats_df = summary_stats_df.append(summary_stats, ignore_index=True)
            
            summary_stats['summary_stat'] = 'standard deviation'
            summary_stats['value'] = np.around(data.loc[:,this_fuel].std(),3)
            summary_stats_df = summary_stats_df.append(summary_stats, ignore_index=True)
            
            summary_stats['summary_stat'] = 'median'
            n_statistic = 10
            closest_10_idx = (data.loc[:,this_fuel] - np.nanquantile(data.loc[:,this_fuel], 0.5)).abs().nsmallest(n_statistic).index
            value = data.loc[closest_10_idx,this_fuel].mean()
            summary_stats['value'] = np.around(value, 3)
            summary_stats['n_statistic'] = n_statistic
            summary_stats_df = summary_stats_df.append(summary_stats, ignore_index=True)
            
            summary_stats['summary_stat'] = '25th percentile'
            closest_10_idx = (data.loc[:,this_fuel] - np.nanquantile(data.loc[:,this_fuel], 0.25)).abs().nsmallest(n_statistic).index
            value = data.loc[closest_10_idx,this_fuel].mean()
            summary_stats['value'] = np.around(value, 3)
            summary_stats_df = summary_stats_df.append(summary_stats, ignore_index=True)
            
            summary_stats['summary_stat'] = '75th percentile'
            closest_10_idx = (data.loc[:,this_fuel] - np.nanquantile(data.loc[:,this_fuel], 0.75)).abs().nsmallest(n_statistic).index
            value = data.loc[closest_10_idx,this_fuel].mean()
            summary_stats['value'] = np.around(value, 3)
            summary_stats_df = summary_stats_df.append(summary_stats, ignore_index=True)
            for i in range(len(bin_values)):
                my_dict={}
                my_dict['year'] = year
                my_dict['fuel'] = 'Gas'
                my_dict['bin_value'] = bin_values[i]
                my_dict['bin_l_edge'] = bin_edges[i]
                my_dict['bin_r_edge'] = bin_edges[i+1]
                output_df = output_df.append(my_dict, ignore_index=True)
        
        # make clear in output df that the min and max bins are open for electricity and max is open for gas
        output_df.loc[(output_df.fuel == 'Net Electricity') & \
                      (output_df.bin_l_edge == elec_bins[0]), 'bin_l_edge'] = '<' + str(elec_bins[1])
        output_df.loc[(output_df.fuel == 'Net Electricity') & \
                      (output_df.bin_r_edge == elec_bins[-1]), 'bin_r_edge'] = '>' + str(elec_bins[-2])
        output_df.loc[(output_df.fuel == 'Gas') & \
                      (output_df.bin_r_edge == gas_bins[-1]), 'bin_r_edge'] = '>' + str(gas_bins[-2])
        
        # match column order to the rest of the stats produced
        summary_stats_df = summary_stats_df[['fuel', 'unit', 'summary_stat', 'subsample',
           'summary_time', 'time_period', 'segmentation_variable_1',
           'segment_1_value', 'value', 'n_sample', 'n_statistic', 'mean_temp', 'mean_hdd',
           'decimal_places', 'weekday_weekend', 'mean_floor_area', 'n_mean_floor_area',
           'mean_bedrooms', 'n_mean_bedrooms', 'mean_occupants',
           'n_mean_occupants']]
            
        
        return (output_df, summary_stats_df)
    
    def post_process_data(self):
        '''Make plotting easier and remove some unecessary bits of data'''
        logic = (self.my_output_df.fuel == 'Total')
        self.my_output_df = self.my_output_df.loc[~logic,]
        
        # rename electricity net electriicity 
        logic = (self.my_output_df.fuel == 'Electricity')
        self.my_output_df.loc[logic, 'fuel'] = 'Net Electricity'
        
        # adjustment to make plotting all observatory data easier
        self.my_output_df['segmentation_variable_1'] = self.my_output_df['segmentation_variable_1'].fillna('None')
        self.my_output_df['segment_1_value'] = self.my_output_df['segment_1_value'].fillna('None')
        #merge with supplementary info so have floor area, n occupants and n bedrooms for each group
        #later we will want heating system info as well
        supplementary_info_path = ''
        supplementary = pd.read_csv(supplementary_info_path + 'supplementary_AR_info_ed4.csv')
        
        self.my_output_df = pd.merge(self.my_output_df, supplementary, how = 'inner', on = ['segmentation_variable_1', 'segment_1_value'])
        
        # to deal with statistics for gas consumption in non-gas heated homes  
        logic = (self.my_output_df.segmentation_variable_1 == 'boiler_type_merge_for_gas_consumption') & \
                (self.my_output_df.fuel == 'Electricity')
        self.my_output_df = self.my_output_df.loc[~logic,]
        
        logic = (self.my_output_df.segmentation_variable_1 == 'boiler_type_merge_for_elec_consumption') & \
                (self.my_output_df.fuel == 'Gas')
        self.my_output_df = self.my_output_df.loc[~logic,]
        
        # append the summary stats from the histogram with same partcipants in 2020 and 2021
        hist_summary_stats = pd.read_csv(self.save_path + 'summary_stats_same_participants2021_2020.csv')
        self.my_output_df = self.my_output_df.append(hist_summary_stats)

    def export_data(self, year:str):

        self.my_output_df.to_csv(self.save_path+'m3_outputs'+year+'.csv', index = False)
#%%
def run_year(m2_output_path, 
             m1_output_path,
             save_path,
             serl_data_path,
             year: str,
             heating_season = False):
    print(f'Starting run for year: {year}')
    m3 = Module3(m2_output_path=m2_output_path,
                 m1_output_path=m1_output_path,
                 save_path=save_path,
                 serl_data_path=serl_data_path)
    m3.load_contextual()
    m3.load_data(year=year)
    if heating_season:
        previous_year_num = pd.to_numeric(year) - 1
        previous_year_str = f'{previous_year_num}'
        m3.load_heating_season_data(previous_year_str, year)
        m3.load_data(year=previous_year_str)
        hist_values, summary_stats = m3.hist_values_compare_2_years(year, previous_year_str)
        hist_values.to_csv(save_path+'hist_values_same_participants'+year+'_' + previous_year_str+'.csv', index = False)
        summary_stats.to_csv(save_path+'summary_stats_same_participants'+year+'_' + previous_year_str+'.csv', index = False)
    m3.preprocess_total_energy_data(year=year, 
                                    heating_season=heating_season)
    m3.preprocess_data()
    m3.produce_stats(year=year, 
                      resolution='annual', 
                      heating_season=heating_season)
    m3.produce_stats(year=year, 
                      resolution='monthly', 
                      heating_season=heating_season)
    m3.produce_stats(year=year, 
                      resolution='diurnal', 
                      heating_season=heating_season)
    hist_values = m3.hist_values(year)
    hist_values.to_csv(save_path+'hist_values_'+year+'.csv')

    m3.post_process_data()
    m3.export_data(year=year)
    
#%%
def main():
    m2_output_path = ''
    m1_output_path = ''
    save_path = ''
    serl_data_path = ''
    
    year='2019'
    run_year(m2_output_path=m2_output_path,
             m1_output_path=m1_output_path,
             save_path=save_path,
             serl_data_path=serl_data_path,
             year=year,
             heating_season=False)
    year='2020'
    run_year(m2_output_path=m2_output_path,
             m1_output_path=m1_output_path,
             save_path=save_path,
             serl_data_path=serl_data_path,
             year=year,
             heating_season=True)
    year='2021'
    run_year(m2_output_path=m2_output_path,
             m1_output_path=m1_output_path,
             save_path=save_path,
             serl_data_path=serl_data_path,
             year=year,
             heating_season=True)

    
#%%
if __name__ == "__main__":
    main()
        
        
        