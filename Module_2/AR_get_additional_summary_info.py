# -*- coding: utf-8 -*-
"""
Created on Mon Mar 28 10:52:26 2022
This code finds the mean floor area, number of occupants and bedrooms for each
breakdown of puprn included in the aggregated stats. Note that these are for all
puprn within each category, not exclusively the ones with sufficent data to be
included in the energy stats.

@author: j.few, contact jessica.few@ucl.ac.uk
"""
import numpy as np
import pandas as pd
import os
import warnings
import time
import matplotlib.pyplot as plt

ed_4_path = ''

serl_survey = pd.read_csv(ed_4_path+ 'serl_survey_data_edition04.csv')
epc = pd.read_csv(ed_4_path+ 'serl_epc_data_edition04.csv')
participant = pd.read_csv(ed_4_path+ 'serl_participant_summary_edition04.csv')
exporters_path = ''
exporters = pd.read_csv(exporters_path + 'Elec_2021_list_of_exporter_puprns.csv', header = None)
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
#self.my_input_data['solar_puprn'] = solar_puprn

# merge segments to make sure summary statistics meet SDC requirements
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
segmentation_list = [
    # OCCUPANT
    # Number of occupants
    ['num_occupants',serl_survey],
    # IMD quintile
    ['IMD_quintile',participant],
    # BEHAVIOURAL
    # Weekday vs weekend
    # NOTE: cannot do with input data yet
    
    # BUILDING
    # EPC rating
    ['currentEnergyRating_merge', epc],
    # number of bedrooms
    ['num_bedrooms',serl_survey],
    # Property type
    ['building_type_merge',serl_survey],
    # Building age
    ['building_age_merge',serl_survey],
    # Floor area
    ['floor_area_banded',epc],
    # Tenure
    ['tenure',serl_survey],
    # Boiler type
    ['boiler_type_merge_for_elec_consumption',serl_survey],
    ['boiler_type_merge_for_gas_consumption',serl_survey],
    # PV 
    ['has_pv',solar_puprn],
    # APPLIANCE/ENERGY USING EQUIPMENT
    # Main heating fuel
    # this is implicit in boiler type, no need for further category
    # EV
    ['has_ev_merge',serl_survey],
    # CONTEXTUAL
    # Region
    ['Region',participant]]

# loop over the segementation lists to produce summary stats of mean floor area
# mean number of bedrooms, mean occupant number for each segment in the whole 
# SERL observatory, i.e. not calculated separately for the specific participants
# included in each energy statistic

seg_var_list = []
seg_val_list = []
mean_fa_list = []
n_fa_list = []
mean_bdrm_list = []
n_bdrm_list = []
mean_occ_list = []
n_occ_list = []


# first find stats for whole sample, then loop over the contextual variables

seg_var_list.append('None')
seg_val_list.append('None')
mean_fa_list.append(epc.totalFloorArea.mean())
n_fa_list.append(len(epc))
mean_bdrm_list.append(serl_survey.loc[serl_survey.B6>0, 'B6'].mean())
n_bdrm_list.append((serl_survey.B6>0).sum())
mean_occ_list.append(serl_survey.loc[serl_survey.C1_new>0, 'C1_new'].mean())
n_occ_list.append((serl_survey.C1_new>0).sum())

# temperature bands are for the whole sample as well

temperature_band_list = [
            '0_to_5',
            '5_to_10',
            '10_to_15',
            '15_to_20',
            '4.5_to_5.5'
            ]

seg_var = 'temperature band'
for temperature_band in temperature_band_list:
    seg_var_list.append(seg_var)
    seg_val_list.append(temperature_band)
    mean_fa_list.append(epc.totalFloorArea.mean())
    n_fa_list.append(len(epc))
    mean_bdrm_list.append(serl_survey.loc[serl_survey.B6>0, 'B6'].mean())
    n_bdrm_list.append((serl_survey.B6>0).sum())
    mean_occ_list.append(serl_survey.loc[serl_survey.C1_new>0, 'C1_new'].mean())
    n_occ_list.append((serl_survey.C1_new>0).sum())

seg_var = 'weekday_weekend'
for day_type in ['weekday', 'weekend']:
    seg_var_list.append(seg_var)
    seg_val_list.append(day_type)
    mean_fa_list.append(epc.totalFloorArea.mean())
    n_fa_list.append(len(epc))
    mean_bdrm_list.append(serl_survey.loc[serl_survey.B6>0, 'B6'].mean())
    n_bdrm_list.append((serl_survey.B6>0).sum())
    mean_occ_list.append(serl_survey.loc[serl_survey.C1_new>0, 'C1_new'].mean())
    n_occ_list.append((serl_survey.C1_new>0).sum())

    

for [seg_var, data] in segmentation_list:
    for seg_val in data[seg_var].unique():
        seg_var_list.append(seg_var)
        seg_val_list.append(seg_val)
        puprn_seg_val = data.loc[data[seg_var]==seg_val,'PUPRN']
        mean_fa_list.append(epc.loc[epc.PUPRN.isin(puprn_seg_val) ,'totalFloorArea'].mean())
        n_fa_list.append(len(epc.loc[epc.PUPRN.isin(puprn_seg_val),]))
        mean_bdrm_list.append(serl_survey.loc[(serl_survey.PUPRN.isin(puprn_seg_val)) & (serl_survey.B6>0), 'B6'].mean())
        n_bdrm_list.append(len(serl_survey.loc[(serl_survey.PUPRN.isin(puprn_seg_val)) & (serl_survey.B6>0),]))
        mean_occ_list.append(serl_survey.loc[(serl_survey.PUPRN.isin(puprn_seg_val)) & (serl_survey.C1_new>0), 'C1_new'].mean())
        n_occ_list.append(len(serl_survey.loc[(serl_survey.PUPRN.isin(puprn_seg_val)) & (serl_survey.C1_new>0), ]))
        if (seg_var == 'num_occupants') & (seg_val == 'No data'):
            n_occ_list[-1] = (len(serl_survey.loc[(serl_survey.PUPRN.isin(puprn_seg_val)) & (serl_survey.C1_new<0), ]))
        if (seg_var == 'num_bedrooms') & (seg_val == 'No data'):
            n_bdrm_list[-1] = (len(serl_survey.loc[(serl_survey.PUPRN.isin(puprn_seg_val)) & (serl_survey.B6<0),]))
        
            

summary_dict = {'segmentation_variable_1': seg_var_list, 'segment_1_value': seg_val_list, 
                        'mean_floor_area': mean_fa_list, 'n_mean_floor_area': n_fa_list,
                        'mean_bedrooms': mean_bdrm_list, 'n_mean_bedrooms': n_bdrm_list, 
                        'mean_occupants': mean_occ_list, 'n_mean_occupants': n_occ_list}
summary = pd.DataFrame(summary_dict)

# check all n above 10
for n_column in ['n_mean_floor_area', 'n_mean_bedrooms', 'n_mean_occupants']:
    if (summary[n_column]<=10).any():
        print('check ' + n_column + 'for counts less than 10')

# save additional information 
output_folder = ''
summary.to_csv(output_folder + 'supplementary_AR_info_ed4.csv', index = False)













