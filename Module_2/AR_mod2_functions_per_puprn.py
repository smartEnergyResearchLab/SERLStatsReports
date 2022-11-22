# -*- coding: utf-8 -*-
"""
Created on Fri Jan 14 15:35:41 2022
The functions in this script can be used to generate daily and diurnal mean energy 
consumption per puprn using the outputs of module 1. Means can be generated 
per month, per year, per heating season, and per external temperature band. 

@author: j.few, contact jessica.few@ucl.ac.uk
"""

import numpy as np
import pandas as pd
import os
import warnings
import time
import sys


def get_missing_data_threshold(fuel_type, month= np.nan, Hh=np.nan, temperature_banding = False):
    # for now require 50% data across the board.
    # if np.isnan(hour): daily missing data threshold
    # else: hh missing data threshold
    return 0.5
        

def get_hh_means_for_one_puprn(hh_data_path, hh_files, file_n, temperature_banding = False):
    '''Summarise the half hourly data for one PUPRN, return lists storing required data'''
    # get data for this puprn and convert the dates to a format we can use            
    puprn_i_hh_data = pd.read_csv(hh_data_path + hh_files[file_n])
    puprn_i_hh_data['Read_date_effective_local'] = pd.to_datetime(puprn_i_hh_data['Read_date_effective_local'])
    puprn_i_hh_data['month_of_consumption'] = puprn_i_hh_data['Read_date_effective_local'].dt.month
    Hh_column = 'Readings_from_midnight_local'
    
    # remove leading zeroes
    for consumption_column in ['Clean_elec_net_Wh', 'Clean_gas_Wh']:
        if puprn_i_hh_data[consumption_column].notna().any():
            if ((puprn_i_hh_data[consumption_column]!=0) & ~pd.isnull(puprn_i_hh_data[consumption_column])).sum() != 0: # if there are any non- zero values
                non_zero_idx = puprn_i_hh_data.index[puprn_i_hh_data[consumption_column] >0][0]
                puprn_i_hh_data.loc[0:non_zero_idx-1, consumption_column] = np.nan
            else: # if all values are zero then make them all nan
                puprn_i_hh_data.loc[:, consumption_column] = np.nan
    
    # initialise lists for storing data and converting to a data frame at the end
    puprn_list = []
    elec_consumption_list = []
    gas_consumption_list = []
    hh_list = []
    temp_elec_list = []
    temp_gas_list = [] 
    temp_weighted_list = []
    
    # temperature banding processing
    if temperature_banding:
        # initialise list for storing temperature bands
        temp_band_list = []
        # define the temperature limits of the temperature bands
        temp_ranges = [[0,5], [5,10], [10,15], [15,20], [4.5,5.5]]
        
        puprn_i_hh_data['day_of_consumption'] = puprn_i_hh_data['Read_date_effective_local'].dt.day
        # calculate mean temperature for the day
        puprn_i_mean_daily_temp = puprn_i_hh_data[['month_of_consumption', 'day_of_consumption', 'temp_C']].groupby(by = ['month_of_consumption', 'day_of_consumption']).mean().reset_index()
        puprn_i_mean_daily_temp = puprn_i_mean_daily_temp.rename(columns = {"temp_C":"mean_temp_C"})
        puprn_i_hh_data = pd.merge(puprn_i_hh_data, puprn_i_mean_daily_temp, how = "left", on = ['month_of_consumption','day_of_consumption'])
        # processing for each temperature band
        for temp_range in temp_ranges:
            # extract data within given temperature band
            puprn_i_temp_range = puprn_i_hh_data[(puprn_i_hh_data['mean_temp_C']>=temp_range[0]) & (puprn_i_hh_data['mean_temp_C']<temp_range[1])]
            # processing for each half hour
            for hh_y in range(1,49):
                # get data for this hh
                month_hour_minute_data = puprn_i_temp_range[puprn_i_temp_range[Hh_column]== hh_y]
                # get the missing data threshold for this type of data
                elec_missing_data_threshold = get_missing_data_threshold('elec', hh_y, temperature_banding = True)
                gas_missing_data_threshold = get_missing_data_threshold('gas', hh_y, temperature_banding = True)
                # get mean consumption values and temperature data
                elec_mean, temp_elec_vals = get_mean_consumption_and_temp_vals(month_hour_minute_data, elec_missing_data_threshold, 'Clean_elec_net_Wh', 'temp_C')
                gas_mean, temp_gas_vals = get_mean_consumption_and_temp_vals(month_hour_minute_data, gas_missing_data_threshold, 'Clean_gas_Wh', 'temp_C')
                # append these mean values into relevant lists
                elec_consumption_list.append(elec_mean)
                gas_consumption_list.append(gas_mean)      
                # get the mean temperature values
                temp_elec_mean, temp_gas_mean, temp_weighted = get_temp_means(temp_elec_vals, temp_gas_vals)
                # append mean temperature values into relevant lists                           
                temp_elec_list.append(temp_elec_mean)
                temp_gas_list.append(temp_gas_mean)
                temp_weighted_list.append(temp_weighted)
                # append this half hour into relevant list
                hh_list.append(hh_y)
            # append this temperature band value into 48 values of the temp_band_list to record this information for the whole profile
            temp_band_list.extend([str(temp_range[0])+ '_to_' + str(temp_range[1])]*48)
           
    # if we're not doing temperature band processing 
    else: 
        # initialise lists for month by month processing
        month_local_list = []
        weekday_weekend_list = []
        days_in_month_list = [] # record the number of days, weekdays and weekend days in each month for correct calculation of average annual consumption later
        puprn_i_hh_data['day_of_week'] = puprn_i_hh_data.Read_date_effective_local.dt.dayofweek # monday = 0, sunday = 6
        # get data for each month in the year
        for month_x in range(1,13):
            i = 0
            # extract the data for this month
            puprn_i_month_x =  puprn_i_hh_data[puprn_i_hh_data['month_of_consumption'] == month_x]
            # processing for each type of day
            for day_type in ['both', 'weekday', 'weekend']:
                # get the first day of the month and the last day so we can use np.busday_count to count the 
                # number of weekday/weekend days in this month. Note that the last_day is the first day of the 
                # next month to get required answer from np.busday_count
                first_day = puprn_i_month_x.Read_date_effective_local.iloc[0].date()
                last_day = puprn_i_month_x.Read_date_effective_local.iloc[-1].date()+pd.Timedelta(days=1)
                # get required data for this day type and count the relevant number of days in this month
                if day_type == 'both':
                    puprn_i_month_x_daytype = puprn_i_month_x
                    days_in_month = puprn_i_month_x_daytype.Read_date_effective_local.iloc[0].days_in_month
                elif day_type == 'weekday':
                    puprn_i_month_x_daytype = puprn_i_month_x[puprn_i_month_x['day_of_week'] <5]
                    days_in_month = np.busday_count(first_day, last_day)
                elif day_type == 'weekend':
                    puprn_i_month_x_daytype = puprn_i_month_x[puprn_i_month_x['day_of_week'] >4]
                    days_in_month = np.busday_count(first_day, last_day, weekmask = [0,0,0,0,0,1,1])
                # get data for each half hour
                for hh_y in range(1,49):
                    # get data for this half hour
                    month_hour_minute_data = puprn_i_month_x_daytype[puprn_i_month_x_daytype[Hh_column]== hh_y]
                    # get missing data thresholds for this half hour
                    elec_missing_data_threshold = get_missing_data_threshold('elec', month_x, hh_y)
                    gas_missing_data_threshold = get_missing_data_threshold('gas', month_x, hh_y)
                    # get mean consumpation data and temperature data for this half hour
                    elec_mean, temp_elec_vals = get_mean_consumption_and_temp_vals(month_hour_minute_data, elec_missing_data_threshold, 'Clean_elec_net_Wh', 'temp_C')
                    gas_mean, temp_gas_vals = get_mean_consumption_and_temp_vals(month_hour_minute_data, gas_missing_data_threshold, 'Clean_gas_Wh', 'temp_C')
                    # append consumption data to relevant lists                    
                    elec_consumption_list.append(elec_mean)
                    gas_consumption_list.append(gas_mean)      
                    # get mean temperature data
                    temp_elec_mean, temp_gas_mean, temp_weighted = get_temp_means(temp_elec_vals, temp_gas_vals)
                    # append temperature data to relevant lists
                    temp_elec_list.append(temp_elec_mean)
                    temp_gas_list.append(temp_gas_mean)
                    temp_weighted_list.append(temp_weighted)
                    # append this half hour to relevant list
                    hh_list.append(hh_y)
                    i = i + 1 
                # append this day type and number of days in the month in relevant list to store it for the whole profile
                weekday_weekend_list.extend([day_type]*48)
                days_in_month_list.extend([days_in_month]*48)
            # append the month to the relevant list for the total number of half hours we just summarised
            month_local_list.extend([month_x]*i)
    # append the puprn to the relevant list 
    puprn_list.extend([hh_files[file_n][0:8]]*len(elec_consumption_list)) # puprn is the first 8 letters of the csv file
    # return required lists depending on whether we processed temperature banded data or not
    if temperature_banding:
        return puprn_list, elec_consumption_list, gas_consumption_list, hh_list, temp_band_list, temp_elec_list, temp_gas_list, temp_weighted_list
    else:
        return puprn_list, elec_consumption_list, gas_consumption_list, hh_list, weekday_weekend_list, month_local_list, days_in_month_list, temp_elec_list, temp_gas_list, temp_weighted_list


def get_hh_means_all_puprn(hh_data_path, temperature_banding = False):
    '''Get required half hourly summaries for all half hourly files in the hh_data_path. Return a dataframe 
    with mean half hourly profiles for each puprn - either one profile per month and day type, or one profile per
    temperature band'''
    # get a list of the files we want to summarise
    hh_files = os.listdir(hh_data_path)
    # time how long this processing takes
    start = time.process_time()
    # initialise lists for storing data, we will convert these to a data frame later
    all_puprn_list = []
    all_elec_consumption_list = []
    all_gas_consumption_list = []
    all_hh_list = []
    all_temp_elec_list = []
    all_temp_gas_list = []
    all_temp_weighted_list = []
    # initialise lists specific to the type of processing
    if temperature_banding:
        all_temp_band_list = []
    else:
        all_month_local_list = []
        all_weekday_weekend_list = []
        all_days_in_month_list = []
    # loop through each file
    for file_n in range(0, len(hh_files)):
        print(file_n) # to keep track of progress through the processing
        if temperature_banding:
            # get the required data for this file
            puprn_list, elec_consumption_list, gas_consumption_list, hh_list, temp_band_list, temp_elec_list, temp_gas_list, temp_weighted_list = get_hh_means_for_one_puprn(hh_data_path, hh_files, file_n, temperature_banding = True)
            # append data to relevant lists
            all_puprn_list.extend(puprn_list)
            all_elec_consumption_list.extend(elec_consumption_list)
            all_gas_consumption_list.extend(gas_consumption_list)
            all_hh_list.extend(hh_list)
            all_temp_band_list.extend(temp_band_list)
            all_temp_elec_list.extend(temp_elec_list)
            all_temp_gas_list.extend(temp_gas_list)
            all_temp_weighted_list.extend(temp_weighted_list)
                
        else:
            # get required data for this file
            puprn_list, elec_consumption_list, gas_consumption_list, hh_list, weekday_weekend_list, month_local_list, days_in_month_list, temp_elec_list, temp_gas_list, temp_weighted_list = get_hh_means_for_one_puprn(hh_data_path, hh_files, file_n)
            # append data to relevant lists
            all_puprn_list.extend(puprn_list)
            all_elec_consumption_list.extend(elec_consumption_list)
            all_gas_consumption_list.extend(gas_consumption_list)
            all_hh_list.extend(hh_list)
            all_weekday_weekend_list.extend(weekday_weekend_list)
            all_month_local_list.extend(month_local_list)
            all_days_in_month_list.extend(days_in_month_list)
            all_temp_elec_list.extend(temp_elec_list)
            all_temp_gas_list.extend(temp_gas_list)
            all_temp_weighted_list.extend(temp_weighted_list)
    
    # convert lists to dictionaries         
    if temperature_banding:
        summary_dict = {'PUPRN': all_puprn_list, 'Clean_elec_net_Wh_hh_mean': all_elec_consumption_list, 
                        'Clean_gas_Wh_hh_mean': all_gas_consumption_list,
                        'temp_band': all_temp_band_list, 'Hh_local_time': all_hh_list, 
                        'temp_elec_C': all_temp_elec_list, 'temp_gas_C': all_temp_gas_list, 
                        'temp_weighted_C': all_temp_weighted_list}   
    else:              
        summary_dict = {'PUPRN': all_puprn_list, 'Clean_elec_net_Wh_hh_mean': all_elec_consumption_list, 
                        'Clean_gas_Wh_hh_mean': all_gas_consumption_list, 'weekday_weekend': all_weekday_weekend_list,
                        'month_local_time': all_month_local_list, 'Hh_local_time': all_hh_list, 
                        'days_in_month': all_days_in_month_list, 'temp_elec_C': all_temp_elec_list, 'temp_gas_C': all_temp_gas_list, 
                        'temp_weighted_C': all_temp_weighted_list}
    # convert dictionaries to data frames
    puprn_summary = pd.DataFrame(summary_dict)
    # add some further columns to the output files
    puprn_summary['hour_local_time'] = puprn_summary['Hh_local_time']//2
    puprn_summary.loc[puprn_summary['hour_local_time']==24, 'hour_local_time'] = 0
    puprn_summary['minute_local_time'] = (puprn_summary['Hh_local_time'])%2*30
    # note that the total column is not used in the final statistics
    puprn_summary['Clean_total_Wh_hh_mean'] = puprn_summary['Clean_elec_net_Wh_hh_mean'] + puprn_summary['Clean_gas_Wh_hh_mean']
    
    print(time.process_time() - start)

    
    return puprn_summary


def get_mean_consumption_and_temp_vals(puprn_data_chunk, missing_data_threshold, fuel_col, temp_col, hdd_col = ''):
    # check if there is sufficent data to use the consumption data
    fuel_sufficient_data = (~pd.isnull(puprn_data_chunk[fuel_col])).sum()/(len(puprn_data_chunk)) >= missing_data_threshold
    if fuel_sufficient_data: 
        fuel_mean = puprn_data_chunk[fuel_col].mean(skipna = True)
        temp_fuel_vals = puprn_data_chunk.loc[~pd.isnull(puprn_data_chunk[fuel_col]), temp_col]
        if hdd_col != '':
            hdd_fuel_vals = puprn_data_chunk.loc[~pd.isnull(puprn_data_chunk[fuel_col]), hdd_col]
    # return nans if too much data is missing
    else: 
        fuel_mean = np.nan
        temp_fuel_vals = np.nan
        if hdd_col != '':
            hdd_fuel_vals = np.nan
    
    # only return hdd vals if needed
    if hdd_col == '':
        return fuel_mean, temp_fuel_vals
    else: 
        return fuel_mean, temp_fuel_vals, hdd_fuel_vals



def get_daily_means_all_puprn(daily_data_path, year, temperature_banding = False):
    '''Get required daily summaries for all daily data in the file for this year. Return a dataframe 
    with mean daily consumption for each puprn - either one value per month and day type, or one value per
    temperature band'''
    # find relevant file
    daily_dir = os.listdir(daily_data_path)
    daily_csvs = list(filter(lambda f: f.endswith('.csv'), daily_dir))
    daily_files = list(filter(lambda f: str(year) in f, daily_csvs))
    # keep track of processing time
    start = time.process_time()
    # load data and convert date type as required
    daily_data = pd.read_csv(daily_data_path + daily_files[0], parse_dates =[1], infer_datetime_format = True)
    daily_data['month'] = daily_data['Read_date_effective_local'].dt.month
    daily_data['day_of_week'] = daily_data.Read_date_effective_local.dt.dayofweek
    
    # initialise lists for storing data and converting to a data frame at the end
    puprn_list = []
    elec_consumption_list = []
    gas_consumption_list = []
    temp_elec_list = []
    temp_gas_list = [] 
    temp_weighted_list = []
    hdd_elec_list = []
    hdd_gas_list = [] 
    hdd_weighted_list = []
    # initialise lists specific to this type of processing
    if temperature_banding:
        temp_band_list = []
        # define the temperature bands
        temp_ranges = [[0,5], [5,10], [10,15], [15,20], [4.5,5.5]]
    else: 
        month_local_list = []
        days_in_month_list = []
        weekday_weekend_list = []
    i = 0
    for puprn_i in daily_data.PUPRN.unique():
        print(i) # keep track of progress through the processing
        i=i+1
        # get the data for this puprn
        puprn_i_data = daily_data[daily_data.PUPRN == puprn_i]
        # processing by month 
        if temperature_banding == False:
            for month_x in range(1,13):
                # get the data for this puprn and this month
                puprn_i_month_x =  puprn_i_data[puprn_i_data['month'] == month_x]
                # run processing for each different type of day
                for day_type in ['both', 'weekday', 'weekend']:
                    # get the first day of the month and the last day so we can use np.busday_count to count the 
                    # number of weekday/weekend days in this month. Note that the last_day is the first day of the 
                    # next month to get required answer from np.busday_count
                    first_day = puprn_i_month_x.Read_date_effective_local.iloc[0].date()
                    last_day = puprn_i_month_x.Read_date_effective_local.iloc[-1].date()+pd.Timedelta(days=1)
                    if day_type == 'both':
                        puprn_i_month_x_daytype = puprn_i_month_x
                        days_in_month_x = puprn_i_month_x_daytype.Read_date_effective_local.iloc[0].days_in_month
                    elif day_type == 'weekday':
                        puprn_i_month_x_daytype = puprn_i_month_x[puprn_i_month_x['day_of_week'] <5]
                        days_in_month_x = np.busday_count(first_day, last_day)
                    elif day_type == 'weekend':
                        puprn_i_month_x_daytype = puprn_i_month_x[puprn_i_month_x['day_of_week'] >4]
                        days_in_month_x = np.busday_count(first_day, last_day, weekmask = [0,0,0,0,0,1,1])         
                
                    # get missing data thresholds for this type of data
                    elec_missing_data_threshold = get_missing_data_threshold('elec', month_x)
                    gas_missing_data_threshold = get_missing_data_threshold('gas', month_x)
                    # get mean consumption data and temperature data
                    elec_mean, temp_elec_vals, hdd_elec_vals = get_mean_consumption_and_temp_vals(puprn_i_month_x_daytype, elec_missing_data_threshold, 'Clean_elec_net_d_kWh', 'mean_temp_C', hdd_col = 'hdd')
                    gas_mean, temp_gas_vals, hdd_gas_vals = get_mean_consumption_and_temp_vals(puprn_i_month_x_daytype, gas_missing_data_threshold, 'Clean_gas_d_kWh', 'mean_temp_C', hdd_col = 'hdd')
                    # append consumption data to relevant lists
                    elec_consumption_list.append(elec_mean)
                    gas_consumption_list.append(gas_mean)
                    # get mean temperature data 
                    temp_elec_mean, temp_gas_mean, temp_weighted = get_temp_means(temp_elec_vals, temp_gas_vals)
                    hdd_elec_mean, hdd_gas_mean, hdd_weighted = get_temp_means(hdd_elec_vals, hdd_gas_vals)
                    # append temperature and hdd data to relevant lists
                    temp_elec_list.append(temp_elec_mean)
                    temp_gas_list.append(temp_gas_mean)
                    temp_weighted_list.append(temp_weighted)
                    
                    hdd_elec_list.append(hdd_elec_mean)
                    hdd_gas_list.append(hdd_gas_mean)
                    hdd_weighted_list.append(hdd_weighted)
                    # append the month and type of day this mean is for
                    month_local_list.append(month_x)
                    days_in_month_list.append(days_in_month_x)
                    weekday_weekend_list.append(day_type)
            puprn_list.extend([puprn_i]*12*3) # want the puprn recorded for all 12 months and all 3 day types
        # processing for temperature band data
        else: 
            # loop through temperature bands
            for temp_range in temp_ranges:
                # get data for this puprn in this temperature range
                puprn_i_temp_range = puprn_i_data[(puprn_i_data['mean_temp_C']>=temp_range[0]) & (puprn_i_data['mean_temp_C']<temp_range[1])]
                # find missing data threshold for this data
                elec_missing_data_threshold = get_missing_data_threshold('elec', temperature_banding = True)
                gas_missing_data_threshold = get_missing_data_threshold('gas', temperature_banding = True)
                # find mean consumption values and temperature values
                elec_mean, temp_elec_vals, hdd_elec_vals = get_mean_consumption_and_temp_vals(puprn_i_temp_range, elec_missing_data_threshold, 'Clean_elec_net_d_kWh', 'mean_temp_C', hdd_col = 'hdd')
                gas_mean, temp_gas_vals, hdd_gas_vals = get_mean_consumption_and_temp_vals(puprn_i_temp_range, gas_missing_data_threshold, 'Clean_gas_d_kWh', 'mean_temp_C', hdd_col = 'hdd')
                # append consumption data
                elec_consumption_list.append(elec_mean)
                gas_consumption_list.append(gas_mean)
                # find mean temperature data
                temp_elec_mean, temp_gas_mean, temp_weighted = get_temp_means(temp_elec_vals, temp_gas_vals)
                hdd_elec_mean, hdd_gas_mean, hdd_weighted = get_temp_means(hdd_elec_vals, hdd_gas_vals)
                # append temperature and hdd data to relevant lists
                temp_elec_list.append(temp_elec_mean)
                temp_gas_list.append(temp_gas_mean)
                temp_weighted_list.append(temp_weighted)
                
                hdd_elec_list.append(hdd_elec_mean)
                hdd_gas_list.append(hdd_gas_mean)
                hdd_weighted_list.append(hdd_weighted)
                temp_band_list.append(str(temp_range[0])+ '_to_' + str(temp_range[1]))
            # record the puprn this data relates to for each temperature band
            puprn_list.extend([puprn_i]*len(temp_ranges)) 
                
    # convert lists storing data into dictionary
    if temperature_banding:
        summary_dict = {'PUPRN': puprn_list, 'Clean_elec_net_kWh_d_mean': elec_consumption_list, 
                        'Clean_gas_kWh_d_mean': gas_consumption_list,
                        'temp_band': temp_band_list, 'temp_elec_C': temp_elec_list, 
                        'temp_gas_C': temp_gas_list, 'temp_weighted_C':temp_weighted_list, 
                        'hdd_elec': hdd_elec_list, 'hdd_gas': hdd_gas_list, 
                        'hdd_weighted_C':hdd_weighted_list}
    else:
        summary_dict = {'PUPRN': puprn_list, 'Clean_elec_net_kWh_d_mean': elec_consumption_list, 
                        'Clean_gas_kWh_d_mean': gas_consumption_list, 'weekday_weekend': weekday_weekend_list,
                        'month_local_time': month_local_list, 'days_in_month': days_in_month_list, 
                        'temp_elec_C': temp_elec_list, 
                        'temp_gas_C': temp_gas_list, 'temp_weighted_C':temp_weighted_list, 
                        'hdd_elec': hdd_elec_list, 'hdd_gas': hdd_gas_list, 
                        'hdd_weighted_C':hdd_weighted_list}
    # convert dictionary to data frame
    puprn_summary = pd.DataFrame(summary_dict)
    # calculate the total - note that we will not use this in the final report.
    puprn_summary['Clean_total_kWh_d_mean'] = puprn_summary['Clean_elec_net_kWh_d_mean'] + puprn_summary['Clean_gas_kWh_d_mean']
    print(time.process_time() - start)
    return puprn_summary


def get_daily_means_one_puprn(daily_data, puprn_i, temperature_banding = False):
    
    # initialise lists 
    puprn_list = []
    elec_consumption_list = []
    gas_consumption_list = []
    temp_elec_list = []
    temp_gas_list = [] 
    temp_weighted_list = []
    hdd_elec_list = []
    hdd_gas_list = [] 
    hdd_weighted_list = []
    
    if temperature_banding:
        temp_band_list = []
        temp_ranges = [[0,5], [5,10], [10,15], [15,20], [4.5,5.5]]
    
    else: 
        month_local_list = []
        days_in_month_list = []
        weekday_weekend_list = []
    
    # select relevant data    
    puprn_i_data = daily_data[daily_data.PUPRN == puprn_i]
    
    
    if temperature_banding == False:
        for month_x in range(1,13):
            puprn_i_month_x =  puprn_i_data[puprn_i_data['month'] == month_x]
            
            for day_type in ['both', 'weekday', 'weekend']:
                first_day = puprn_i_month_x.Read_date_effective_local.iloc[0].date()
                last_day = puprn_i_month_x.Read_date_effective_local.iloc[-1].date()+pd.Timedelta(days=1)
                if day_type == 'both':
                    puprn_i_month_x_daytype = puprn_i_month_x
                    days_in_month = puprn_i_month_x_daytype.Read_date_effective_local.iloc[0].days_in_month
                elif day_type == 'weekday':
                    puprn_i_month_x_daytype = puprn_i_month_x[puprn_i_month_x['day_of_week'] <5]
                    days_in_month = np.busday_count(first_day, last_day)
                elif day_type == 'weekend':
                    puprn_i_month_x_daytype = puprn_i_month_x[puprn_i_month_x['day_of_week'] >4]
                    days_in_month = np.busday_count(first_day, last_day, weekmask = [0,0,0,0,0,1,1])                
                
                
                elec_missing_data_threshold = get_missing_data_threshold('elec', month_x)
                gas_missing_data_threshold = get_missing_data_threshold('gas', month_x)
                
                elec_mean, temp_elec_vals, hdd_elec_vals = get_mean_consumption_and_temp_vals(puprn_i_month_x_daytype, elec_missing_data_threshold, 'Clean_elec_net_d_kWh', 'mean_temp_C', hdd_col = 'hdd')
                gas_mean, temp_gas_vals, hdd_gas_vals = get_mean_consumption_and_temp_vals(puprn_i_month_x_daytype, gas_missing_data_threshold, 'Clean_gas_d_kWh', 'mean_temp_C', hdd_col = 'hdd')
                
                elec_consumption_list.append(elec_mean)
                gas_consumption_list.append(gas_mean)
                
                temp_elec_mean, temp_gas_mean, temp_weighted = get_temp_means(temp_elec_vals, temp_gas_vals)
                hdd_elec_mean, hdd_gas_mean, hdd_weighted = get_temp_means(hdd_elec_vals, hdd_gas_vals)
                           
                temp_elec_list.append(temp_elec_mean)
                temp_gas_list.append(temp_gas_mean)
                temp_weighted_list.append(temp_weighted)
                
                hdd_elec_list.append(hdd_elec_mean)
                hdd_gas_list.append(hdd_gas_mean)
                hdd_weighted_list.append(hdd_weighted)
                
                month_local_list.append(month_x)
                days_in_month_list.append(days_in_month)
                weekday_weekend_list.append(day_type)
        puprn_list.extend([puprn_i]*12*3) # want the puprn recorded for all 12 months and each daytype 

    else: 
        for temp_range in temp_ranges:
            puprn_i_temp_range = puprn_i_data[(puprn_i_data['mean_temp_C']>=temp_range[0]) & (puprn_i_data['mean_temp_C']<temp_range[1])]
            elec_missing_data_threshold = get_missing_data_threshold('elec', temperature_banding = True)
            gas_missing_data_threshold = get_missing_data_threshold('gas', temperature_banding = True)
            
            elec_mean, temp_elec_vals, hdd_elec_vals = get_mean_consumption_and_temp_vals(puprn_i_temp_range, elec_missing_data_threshold, 'Clean_elec_net_d_kWh', 'mean_temp_C', hdd_col = 'hdd')
            gas_mean, temp_gas_vals, hdd_gas_vals = get_mean_consumption_and_temp_vals(puprn_i_temp_range, gas_missing_data_threshold, 'Clean_gas_d_kWh', 'mean_temp_C', hdd_col = 'hdd')
            
            elec_consumption_list.append(elec_mean)
            gas_consumption_list.append(gas_mean)
            
            temp_elec_mean, temp_gas_mean, temp_weighted = get_temp_means(temp_elec_vals, temp_gas_vals)
            hdd_elec_mean, hdd_gas_mean, hdd_weighted = get_temp_means(hdd_elec_vals, hdd_gas_vals)
                       
            temp_elec_list.append(temp_elec_mean)
            temp_gas_list.append(temp_gas_mean)
            temp_weighted_list.append(temp_weighted)
            
            hdd_elec_list.append(hdd_elec_mean)
            hdd_gas_list.append(hdd_gas_mean)
            hdd_weighted_list.append(hdd_weighted)
            temp_band_list.append(str(temp_range[0])+ '_to_' + str(temp_range[1]))
        puprn_list.extend([puprn_i]*len(temp_ranges)) 
        
    if temperature_banding:
        return puprn_list, elec_consumption_list, gas_consumption_list, temp_band_list, temp_elec_list, temp_gas_list, temp_weighted_list, hdd_elec_list, hdd_gas_list, hdd_weighted_list
    else:
        return puprn_list, elec_consumption_list, gas_consumption_list, weekday_weekend_list, month_local_list, days_in_month_list, temp_elec_list, temp_gas_list, temp_weighted_list, hdd_elec_list, hdd_gas_list, hdd_weighted_list



def get_temp_means(temp_elec_vals, temp_gas_vals):
    # note that temp could be the temperature or hdd
    if np.isnan(temp_elec_vals).any():
        temp_elec_mean = np.nan
    else: 
        temp_elec_mean = temp_elec_vals.mean()
    
    if np.isnan(temp_gas_vals).any():
        temp_gas_mean = np.nan
    else:
        temp_gas_mean = temp_gas_vals.mean()
    
        
    if (~np.isnan(temp_elec_vals).any()) & (~np.isnan(temp_gas_vals).any()):
        temp_weighted = (temp_elec_vals.sum() + temp_gas_vals.sum())/(len(temp_elec_vals) + len(temp_gas_vals))
    else:
        temp_weighted = np.nan
        
    return temp_elec_mean, temp_gas_mean, temp_weighted
        
def custom_mean(df):
    return df.mean(skipna = False)

def custom_sum(df):
    return df.sum(skipna = False)

def get_annual_summary(puprn_summary_df, year):
    # aggregate to get average over the whole year
    # note that groupby.mean(skipna = False) gives an error and .mean() ignores nans so 
    # define a custom function and use .agg
    # apply weighting factors to account for different number of days/weekdays/weekend days in each month to get true daily averages over the year, rather than mean of monthly daily consumption
    
    if 'Hh_local_time' in puprn_summary_df.columns:
        days_in_year = np.busday_count(str(year)+'-01', str(year+1)+'-01', weekmask = [1,1,1,1,1,1,1])
        weekdays_in_year = np.busday_count(str(year)+'-01', str(year+1)+'-01', weekmask = [1,1,1,1,1,0,0])
        weekenddays_in_year = np.busday_count(str(year)+'-01', str(year+1)+'-01', weekmask = [0,0,0,0,0,1,1])
        puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='both','days_in_month_weight'] = puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='both','days_in_month']/days_in_year
        puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekday','days_in_month_weight'] = puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekday','days_in_month']/weekdays_in_year
        puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekend','days_in_month_weight'] = puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekend','days_in_month']/weekenddays_in_year
    
        cols_to_weight = ['Clean_elec_net_Wh_hh_mean', 'Clean_gas_Wh_hh_mean', 'temp_elec_C', 'temp_gas_C', 'temp_weighted_C', 'Clean_total_Wh_hh_mean']
        puprn_summary_df[cols_to_weight] = puprn_summary_df[cols_to_weight].multiply(puprn_summary_df['days_in_month_weight'], axis='index')
    
        puprn_summary_annual = puprn_summary_df.groupby(by = ['PUPRN', 'Hh_local_time', 'hour_local_time','minute_local_time', 'weekday_weekend']).agg(custom_sum)
    else:
        days_in_year = np.busday_count(str(year)+'-01', str(year+1)+'-01', weekmask = [1,1,1,1,1,1,1])
        weekdays_in_year = np.busday_count(str(year)+'-01', str(year+1)+'-01', weekmask = [1,1,1,1,1,0,0])
        weekenddays_in_year = np.busday_count(str(year)+'-01', str(year+1)+'-01', weekmask = [0,0,0,0,0,1,1])
        puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='both','days_in_month_weight'] = puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='both','days_in_month']/days_in_year
        puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekday','days_in_month_weight'] = puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekday','days_in_month']/weekdays_in_year
        puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekend','days_in_month_weight'] = puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekend','days_in_month']/weekenddays_in_year
        
        cols_to_weight = ['Clean_elec_net_kWh_d_mean', 'Clean_gas_kWh_d_mean', 'temp_elec_C', 'temp_gas_C', 'temp_weighted_C', 'hdd_elec', 'hdd_gas', 'hdd_weighted_C', 'Clean_total_kWh_d_mean']
        puprn_summary_df[cols_to_weight] = puprn_summary_df[cols_to_weight].multiply(puprn_summary_df['days_in_month_weight'], axis='index')
    
        
        puprn_summary_annual = puprn_summary_df.groupby(by = ['PUPRN', 'weekday_weekend']).agg(custom_sum)
        
    puprn_summary_annual = puprn_summary_annual.reset_index()
    puprn_summary_annual = puprn_summary_annual.drop(['days_in_month_weight', 'days_in_month', 'month_local_time'], axis = 1)
    
    return puprn_summary_annual



def get_heating_season_summary(puprn_summary_df1, year1, puprn_summary_df2, year2):
    # aggregate to get average over the whole year
    # note that groupby.mean(skipna = False) gives an error and .mean() ignores nans so 
    # define a custom function and use .agg
    # apply weighting factors to account for different number of days/weekdays/weekend days in each month to get true daily averages over the year, rather than mean of monthly daily consumption
    # take october to december from df1 and jan to may for df2 and join together into puprn_summary_df
    
    puprn_summary_df1 = puprn_summary_df1.loc[puprn_summary_df1.month_local_time >= 10, ]
    puprn_summary_df2 = puprn_summary_df2.loc[puprn_summary_df2.month_local_time <= 5, ]
    
    puprn_summary_df = pd.concat([puprn_summary_df1, puprn_summary_df2], ignore_index = True)
    
    if 'Hh_local_time' in puprn_summary_df.columns:
        # use sap heating season - October to May inclusive
        days_in_year = np.busday_count(str(year1)+'-10', str(year2)+'-06', weekmask = [1,1,1,1,1,1,1])
        weekdays_in_year = np.busday_count(str(year1)+'-10', str(year2)+'-06', weekmask = [1,1,1,1,1,0,0])
        weekenddays_in_year = np.busday_count(str(year1)+'-10', str(year2)+'-06', weekmask = [0,0,0,0,0,1,1])
        puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='both','days_in_month_weight'] = puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='both','days_in_month']/days_in_year
        puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekday','days_in_month_weight'] = puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekday','days_in_month']/weekdays_in_year
        puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekend','days_in_month_weight'] = puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekend','days_in_month']/weekenddays_in_year
    
        cols_to_weight = ['Clean_elec_net_Wh_hh_mean', 'Clean_gas_Wh_hh_mean', 'temp_elec_C', 'temp_gas_C', 'temp_weighted_C', 'Clean_total_Wh_hh_mean']
        puprn_summary_df[cols_to_weight] = puprn_summary_df[cols_to_weight].multiply(puprn_summary_df['days_in_month_weight'], axis='index')
    
        puprn_summary_annual = puprn_summary_df.groupby(by = ['PUPRN', 'Hh_local_time', 'hour_local_time','minute_local_time', 'weekday_weekend']).agg(custom_sum)
    else:
        days_in_year = np.busday_count(str(year1)+'-10', str(year2)+'-06', weekmask = [1,1,1,1,1,1,1])
        weekdays_in_year = np.busday_count(str(year1)+'-10', str(year2)+'-06', weekmask = [1,1,1,1,1,0,0])
        weekenddays_in_year = np.busday_count(str(year1)+'-10', str(year2)+'-06', weekmask = [0,0,0,0,0,1,1])
        puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='both','days_in_month_weight'] = puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='both','days_in_month']/days_in_year
        puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekday','days_in_month_weight'] = puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekday','days_in_month']/weekdays_in_year
        puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekend','days_in_month_weight'] = puprn_summary_df.loc[puprn_summary_df['weekday_weekend']=='weekend','days_in_month']/weekenddays_in_year
        
        cols_to_weight = ['Clean_elec_net_kWh_d_mean', 'Clean_gas_kWh_d_mean', 'temp_elec_C', 'temp_gas_C', 'temp_weighted_C', 'hdd_elec', 'hdd_gas', 'hdd_weighted_C', 'Clean_total_kWh_d_mean']
        puprn_summary_df[cols_to_weight] = puprn_summary_df[cols_to_weight].multiply(puprn_summary_df['days_in_month_weight'], axis='index')
    
        
        puprn_summary_annual = puprn_summary_df.groupby(by = ['PUPRN', 'weekday_weekend']).agg(custom_sum)
        
    puprn_summary_annual = puprn_summary_annual.reset_index()
    puprn_summary_annual = puprn_summary_annual.drop(['days_in_month_weight', 'days_in_month', 'month_local_time'], axis = 1)
    
    return puprn_summary_annual










