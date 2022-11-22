--------------- ABOUT -----------------
SERL energy data selector - for Edition 3 dataset
Authors: Martin Pullinger (University of Edinburgh / UCL) and Jonathan Kilgour (University of Edinburgh)
martin.pullinger@ed.ac.uk / jonathan.kilgour@ed.ac.uk 
Licence: CC-BY 4.0

--------------- SUMMARY -----------------
This allows you to select a subset of the SERL energy daily or half-hourly data,  add in some extra variables, and return the result back to your script, or as a single csv file, or as a set of csv files, one per participant (PUPRN), for use in your projects.                  

--------------- DETAILS -----------------
**This version uses SERL Edition 3 data**
NB. For each argument below, the first value listed is the default, if not otherwise user-specified.
See examples in data_picker_examples.ipynb.

**Step 1: Initialise the SERLDataSelector: arguments:**
- res = ‘daily’, or ‘hh’
- folder_path = Name of the folder path to load and save data from/to.

**Step 2: Load data with load_data**
- res = 'daily' or 'hh'. The resolution of energy data you want.
- usecols = 'All', or a list. The list must include 'PUPRN' and 'Read_date_effective_local', or you'll get an error. Include 'Read_date_time_local' for the hh data too, if you want to add extra local time variables in. Use ‘All’ with care – large files will result!
- first_date = 'earliest_date', or date as 'YYYY-MM-DD'
- last_date = 'latest_date', or date as 'YYYY-MM-DD'
- add_local_time_cols = False, or True. For hh data, this option adds additional time columns: Read_time_local, Read_time_local_midpoint (Read_time_local minus 15 minutes), Time_zone, Readings_from_midnight_local (similar to the ‘HH’ column, but based on local time, whereas HH is based on UTC. Note that on days when the clocks change, each value is based on the assumption that the day started at midnight in the current timezone, so the 01:30 readings are always reading 5 even if there are two of them that day, the 12:00 reading is always reading 24, etc.
- add_sum_gas_column = False, or True. For daily data only: this option adds an additional gas usage column, Gas_hh_sum_kWh, based on Gas_hh_sum_m3 times a standard conversion factor. NB. res must be set to daily, or this will be ignored.
- add_net_electricity_column = False, or True. For half-hourly data only: this option adds an additional net electricity column, Elec_act_net_hh_Wh, based on import minus export, i.e. Elec_act_imp_hh_Wh - Elec_act_exp_hh_Wh. NB. res must be set to hh, or this will be ignored. 
- filter_rows_on_participant_cat_data = 'No_filters', or a dictionary, where keys are column names from the participant data table; values are lists of selection values, e.g. {'Region': ['SCOTLAND','NORTH WEST'],'IMD_quintile':['3']} . Works with categorical columns only, using 'isin'.
- filter_rows_on_survey_cat_data = 'No_filters', or a dictionary. Functions as above for filter_rows_on_participant_cat_data. 
- filter_Valid_read_time = 'All', or True, or False. Indicates which values of Valid_read_time you want to keep. Note, this requires Valid_read_time to be included in the usecols list above.
- filter_Gas_flag = 'All', or a list of integers, indicating which of the error flag codes you want to keep. Note, this requires the relevant flag column to be included in the usecols list above.
- filter_Elec_act_imp_flag = 'All', or a list of integers, indicating which of the error flag codes you want to keep. Note, this requires the relevant flag column to be included in the usecols list above.
- filter_Elec_act_exp_flag = 'All', or a list of integers, indicating which of the error flag codes you want to keep. Note, this requires the relevant flag column to be included in the usecols list above, and only applies to half-hourly data.
    - Note: If you want to calculate net electricity (you've set add_net_electricity_column = True), then set filter_Elec_act_exp_flag = [1,2], to include flags 1 and 2, for valid reads and for 'no meter', respectively.
- filter_Elec_react_imp_flag = 'All', or a list of integers, indicating which of the error flag codes you want to keep. Note, this requires the relevant flag column to be included in the usecols list above, and only applies to half-hourly data.
- filter_Elec_react_exp_flag = 'All', or a list of integers, indicating which of the error flag codes you want to keep. Note, this requires the relevant flag column to be included in the usecols list above, and only applies to half-hourly data.
- inc_time_change_days = True, or False, 23 or 25. If False, it will remove days when the clocks change from GMT to BST, or vice versa, based on the dates saved in bst_dates_to_2024_restricted.csv. If 23 or 25, it will keep only days that have 23 or 25 hours (respectively), i.e. when the clocks either moved forwards, or backwards, respectively.
- merge_participant_data_variables=False, or a list. If you want variables left-joined to the energy data from the participant data, list them here (you don't need to include PUPRN in the list).
- merge_survey_data_variables=False, or a list. If you want variables left-joined to the energy data from the survey data, list them here (you don't need to include PUPRN in the list).

**Step 3: (Optional) Save data with save_data**
- save_method = ‘per_home’ or ‘single_file’. Save your results as one csv per home, called {PUPRN}.csv (each sorted by datetime UTC), or a single file (sorted by PUPRN then by datetime UTC).
- output_filename='Default' or 'Arbitrary_string'. NB. If ‘per_home’ is selected as save_method, then this is used as a folder name for the outputs. If ‘single_file’ is selected as save_method, then this is used as the filename, and a '.csv' extension is automatically added.
- output_directory=’Data’ or 'Existing\file\path'. 

Note a csv of metadata will be saved too, detailing the parameters used and some characteristics of the resultant files. This has a filename starting 'Metadata_about'

------------- REQUIREMENTS --------------
- locations.py  This is a separate Python script specifying the locations of all the different SERL data within the SERL AWS secure environment.