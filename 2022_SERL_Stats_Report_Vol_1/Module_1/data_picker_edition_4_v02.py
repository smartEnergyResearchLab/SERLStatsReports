"""
SERL energy data picker
"""
import re
import warnings
import pandas as pd 
import numpy as np
from pathlib import Path
import os
import glob
import dask.dataframe as dd
from datetime import datetime
from datetime import date
import csv
import locations # This is a separate Python script specifying the locations of all the different SERL data within the SERL secure environment.

DEFAULT_START_YEAR = 2018
DEFAULT_START_MONTH = '06'

class SerlDataSelector(object):
    """Interface to the SERL dataset edition4"""
    
    def __init__(self, res=None, folder_path=None):        
        warnings.filterwarnings("always", category=UserWarning, module="SerlDataSelector")
        if folder_path is None:
            if res is None:
                self.res='daily'
            else:
                self.res=res
            if self.res=='hh':
                self.folder_path=Path(os.path.join(locations.serl_data_path,locations.energy_hh_directory))
            else:
                self.folder_path=Path(os.path.join(locations.serl_data_path,locations.energy_daily_directory))
            print("Res: ",self.res,"\nPath: ",self.folder_path)
        else:
            print("Not none: ",folder_path)
            self.folder_path = Path(folder_path)
        self.file_identifier = "serl_smart_meter*.csv"        
        self.filenames = self._mapping(self.folder_path)
        self.datecol='Read_date_effective_local'
        
        if len(self.filenames) == 0:
            warnings.warn('The specified path does not seem to contain any serl data')
        
    def _mapping(self, folder_path):
        filenames = list()
        for file in folder_path.glob(self.file_identifier):
            filenames.append(str(file.name))
            
        print('Found {} data files'.format(len(filenames)))
        return filenames

    def load_data(self, filename=locations.energy_daily_regexp,
                res = 'daily', 
                usecols = 'All',
                filter_rows_on_participant_cat_data = 'No_filters',
                filter_rows_on_survey_cat_data = 'No_filters',
                filter_Valid_read_time = 'All',
                filter_Gas_flag = 'All',
                filter_Elec_act_imp_flag = 'All',
                filter_Elec_act_exp_flag = 'All',
                filter_Elec_react_imp_flag = 'All',
                filter_Elec_react_exp_flag = 'All',
                inc_time_change_days = True, 
                first_date = 'earliest_date', last_date = 'latest_date',
                add_local_time_cols = False,
                merge_participant_data_variables = False,
                merge_survey_data_variables = False,
                add_net_electricity_column = False,
                add_sum_gas_column = False,
                output_filename = 'Default'):                

        self.res = res
        self.add_gas_hh_sum = add_sum_gas_column
        self.add_net_electricity_column = add_net_electricity_column
        participant_data = pd.read_csv(os.path.join(locations.serl_data_path,locations.participant_data_file))
        survey_data = pd.read_csv(os.path.join(locations.serl_data_path,locations.survey_data_file),low_memory=False)
        print(usecols)
        print(last_date)
        if (first_date==''):
            first_date='earliest_date'
        if (last_date==''):
            last_date='latest_date'
        # 'Load' in the columns requested using dask.
        filelist=[]
        yearreplace=False
        monthreplace=False
        startyear=DEFAULT_START_YEAR
        startmonth=1
        startday=1
        endyear=datetime.today().year
        endmonth=datetime.today().month

        if self.res=='hh':
            if filename==locations.energy_daily_regexp:
                filename=locations.energy_hh_regexp
                
        if re.search("YYYY",filename):
            if first_date=='earliest_date' and last_date=='latest_date':
                filename = filename.replace('YYYY','*')
            else:
                yearreplace=True
            if re.search("MM",filename):
                if first_date=='earliest_date' and last_date=='latest_date':
                    filename = filename.replace('MM','*')
                else:
                    monthreplace=True
            
        if not yearreplace:
            filelist = glob.glob(os.path.join(self.folder_path,filename))
        else:
            if first_date!='earliest_date':
                earlydate=datetime.strptime(first_date,'%Y-%m-%d')
                startyear=earlydate.year
                startmonth=earlydate.month
                startday=earlydate.day
            if last_date!='latest_date':
                latedate=datetime.strptime(last_date,'%Y-%m-%d')
                endyear=latedate.year
                endmonth=latedate.month
            for year in range(startyear, endyear+1):
                files = filename.replace('YYYY',str(year))
                if monthreplace:
                    emon=12
                    smon=1
                    if year==startyear:
                        smon=startmonth
                        if smon>1 and startday==1:
                            smon=startmonth-1
                    if year==endyear:
                        emon=endmonth
                    for month in range(smon,emon+1):
                        mfiles=files.replace('MM',str(month).rjust(2,'0'))
                        filelist.extend(glob.glob(os.path.join(self.folder_path,mfiles)))
                else:
                    filelist.extend(glob.glob(os.path.join(self.folder_path,files)))
        print("Found "+str(len(filelist))+" files to load / select from (" +str(startyear)+' to '+str(endyear)+").")
        #print(str(filelist))
        self.drop_columns_before_save=[]
        if res=='hh':
            self.datecol='Read_date_time_local'
            if usecols == 'All':
                data = dd.read_csv(filelist,
                                dtype={'Elec_act_imp_hh_Wh': 'float64','Elec_act_exp_hh_Wh': 'float64','Gas_hh_Wh': 'float64'})
            else:
                if add_net_electricity_column!=False:
                    if 'Elec_act_imp_hh_Wh' not in usecols:
                        self.drop_columns_before_save.append('Elec_act_imp_hh_Wh')
                        usecols.append('Elec_act_imp_hh_Wh')
                    if 'Elec_act_exp_hh_Wh' not in usecols:
                        self.drop_columns_before_save.append('Elec_act_exp_hh_Wh')
                        usecols.append('Elec_act_exp_hh_Wh')
                data = dd.read_csv(filelist,
                               usecols=usecols,
                               dtype={'Elec_act_imp_hh_Wh': 'float64','Elec_act_exp_hh_Wh': 'float64','Gas_hh_Wh': 'float64'})
        else:
            if usecols == 'All':
                print("FS1")
                data = dd.read_csv(filelist)
            else:
                print("FS2")
                print(filelist)
                if ('Gas_hh_sum_m3' not in usecols) and (add_sum_gas_column != False):
                    self.drop_columns_before_save.append('Gas_hh_sum_m3')
                    usecols.append('Gas_hh_sum_m3')
                data = dd.read_csv(filelist, usecols=usecols)

        # Filter out dates oustide the desired range
        if first_date!='earliest_date':
            data = data.loc[data.Read_date_effective_local>=first_date]
        if last_date!='latest_date':
            data = data.loc[data.Read_date_effective_local<=last_date]

        # Filter out other rows as requested
        # Get a list of PUPRNs that match the participant filters specified
        if len(filter_rows_on_survey_cat_data)==0:
            filter_rows_on_survey_cat_data='No_filters'
        if len(filter_rows_on_participant_cat_data)==0:
            filter_rows_on_participant_cat_data='No_filters'

        if filter_rows_on_participant_cat_data!='No_filters':
            for key, value in filter_rows_on_participant_cat_data.items():
                participant_data = participant_data[participant_data[key].isin(value)]
            participant_list_ppt = participant_data.PUPRN.unique().tolist() #.unique() shouldn't be necessary, but just in case
            participant_list=participant_list_ppt # Final participant list to include will be this, if not updated below.
        # Get a list of PUPRNs that match the survey filters specified
        if filter_rows_on_survey_cat_data!='No_filters':
            for key, value in filter_rows_on_survey_cat_data.items():
                survey_data = survey_data[survey_data[key].isin(value)]
            participant_list_survey = survey_data.PUPRN.unique().tolist() #.unique() shouldn't be necessary, but just in case
            participant_list=participant_list_survey # Final participant list to include will be this, if not updated below.
        # Create the final set of PUPRNS that match all criteria
        if filter_rows_on_participant_cat_data!='No_filters' and filter_rows_on_survey_cat_data!='No_filters':
            participant_list=[x for x in participant_list_ppt if x in participant_list_survey] # Final participant list to include will be this, if there were filters based on participant and survey data.
        # Then filter data on that list of PUPRNs
        if filter_rows_on_participant_cat_data!='No_filters' or filter_rows_on_survey_cat_data!='No_filters':
            data = data[data.PUPRN.isin(participant_list)]        

        # Filter out dates when the clocks change, if inc_time_change_days == False
        if inc_time_change_days != True:
            clock_changes = pd.read_csv(os.path.join(locations.serl_data_path,locations.bst_dates),index_col=False)
            if inc_time_change_days == False:
                droplist= clock_changes.Read_date_effective_local.tolist()
            if inc_time_change_days == 25:
                droplist= clock_changes[clock_changes.n_hh==46].Read_date_effective_local.tolist()
            if inc_time_change_days == 23:
                droplist= clock_changes[clock_changes.n_hh==50].Read_date_effective_local.tolist()
            data=data.loc[~data.Read_date_effective_local.isin(droplist)]

        # Filter out flagged reads, as specified in any of the filter arguments
        if filter_Valid_read_time!='All':
            data = data[data.Valid_read_time==filter_Valid_read_time]
        if filter_Gas_flag!='All':
            data = data[data.Gas_flag.isin(filter_Gas_flag)]
        if filter_Elec_act_imp_flag!='All':
            data = data[data.Elec_act_imp_flag.isin(filter_Elec_act_imp_flag)]
        if filter_Elec_act_exp_flag!='All' and res == 'hh':
            data = data[data.Elec_act_exp_flag.isin(filter_Elec_act_exp_flag)]    
        if filter_Elec_react_imp_flag!='All' and res == 'hh':
            data = data[data.Elec_react_imp_flag.isin(filter_Elec_react_imp_flag)]
        if filter_Elec_react_exp_flag!='All' and res == 'hh':
            data = data[data.Elec_react_exp_flag.isin(filter_Elec_react_exp_flag)]

        # Merge in (left-join) variables from the participant data and the survey data if requested to do so
        if merge_participant_data_variables!=False:
            merge_participant_data_variables.insert(0,'PUPRN')
            data = data.merge(participant_data[merge_participant_data_variables],on='PUPRN',how='left')
        if merge_survey_data_variables!=False:
            merge_survey_data_variables.insert(0,'PUPRN')
            data = data.merge(survey_data[merge_survey_data_variables],on='PUPRN',how='left')

        # add net electricity use (import - export)
        if res=='hh' and 'Elec_act_imp_hh_Wh' in usecols  and 'Elec_act_exp_hh_Wh' in usecols and add_net_electricity_column!=False:
            data['Elec_act_net_hh_Wh']=data.Elec_act_imp_hh_Wh-data.Elec_act_exp_hh_Wh.fillna(0)

        # Add Gas_hh_sum_kWh, for daily data. 
        # NB. Figures returned to 3dp to be in keeping with measurement accuracy reported for variables already in the df
        if res!='hh' and 'Gas_hh_sum_m3' in usecols and add_sum_gas_column != False:
            data['Gas_hh_sum_kWh']=data.Gas_hh_sum_m3 * 1.02264 * 39.5 / 3.6
            data.round({'Gas_hh_sum_kWh':3})

        # add_local_time_cols: for hh data, add: Read_time_local, Read_time_local_midpoint, Time_zone, Midpoint_seconds_from_midnight
        # NB. Dask doesn't fully support string splitting - the workaround here converts the time string to a list, so individual elements can be extracted.
        if res=='hh' and add_local_time_cols== True:
            data['Time_Temp']=data.Read_date_time_local.str.split(n=2)
            data['Read_time_local']=data.apply(lambda x: x['Time_Temp'][1], axis=1, meta=pd.Series())
            data['Read_time_local_midpoint']=(dd.to_datetime(data.Read_time_local,format="%H:%M:%S")
                                              - pd.Timedelta(minutes=15))
            data['Timezone']=data.apply(lambda x: x['Time_Temp'][2], axis=1, meta=pd.Series())
            data['Readings_from_midnight_local'] = (data.Read_time_local_midpoint.dt.hour
                                                    + data.Read_time_local_midpoint.dt.minute/60)*2 +0.5
            data.Read_time_local_midpoint = data.Read_time_local_midpoint.dt.time
            data.Readings_from_midnight_local = data.Readings_from_midnight_local.astype('int32')
            data = data.drop(['Time_Temp'],axis=1)

        # That's all we can do (quickly) in dask.
        data=data.drop(self.drop_columns_before_save,axis=1)
        data=data.compute()

        self.data = data
        self.filter_rows_on_participant_cat_data = filter_rows_on_participant_cat_data
        self.filter_rows_on_survey_cat_data = filter_rows_on_survey_cat_data
        self.filter_Valid_read_time = filter_Valid_read_time
        self.filter_Gas_flag = filter_Gas_flag
        self.filter_Elec_act_imp_flag = filter_Elec_act_imp_flag
        self.filter_Elec_act_exp_flag = filter_Elec_act_exp_flag
        self.filter_Elec_react_imp_flag = filter_Elec_react_imp_flag
        self.filter_Elec_react_exp_flag = filter_Elec_react_exp_flag
        self.inc_time_change_days = inc_time_change_days 
        self.first_date = first_date
        self.last_date = last_date
        self.add_local_time_cols = add_local_time_cols
        self.inc_time_change_days=inc_time_change_days
        self.usecols=usecols
        return data

    def save_data(self, output_filename="Default", output_directory="Data", metadata_filename="meta.csv", save_method="per_home"):
        data = self.data
           
        timenow = datetime.now().strftime("%Y_%m_%d_%H-%M-%S")
    
        if output_filename=='Default':
            if usecols!='All' or filter_rows_on_participant_cat_data!='No_filters' or inc_time_change_days != True:
                selection='_abridged'
            else: selection=''

            output_filename="energy_"+res+selection+"_From_"+first_date+"_To_"+last_date+"_Created_"+timenow+".csv"
            metadata_filename = "Metadata_about_the_file_created_"+timenow+".csv"
        else:
            metadata_filename = "Metadata_about_"+output_filename+".csv"

        self.puprns=sorted(data.PUPRN.unique(), key=str.lower) # Get a list of PUPRNs in the output data, sorted alphanumerically (case insensitive)
        self.first_date_in_df = data.Read_date_effective_local.min()
        self.last_date_in_df = data.Read_date_effective_local.max()
        self.save_metadata(metadata_filename=metadata_filename, output_directory=output_directory)
        if save_method=='single_file':
            data = data.sort_values(by=['PUPRN',self.datecol])
            data.to_csv(os.path.join(output_directory,output_filename+".csv"),index=False)
        elif save_method=='per_home':
            if not os.path.exists(os.path.join(output_directory,output_filename)):
                os.makedirs(os.path.join(output_directory,output_filename))
            for pup in self.puprns:
                print("Save for home "+pup)
                homedata = data[data.PUPRN==pup]
                #homedata = homedata.compute()
                #print(homedata.head())
                #homedata = homedata.sort_values(by=[self.datecol])
                        #homedata.to_csv(os.path.join(output_directory,output_filename,pup+".csv"),single_file=True)
                homedata.to_csv(os.path.join(output_directory,output_filename,pup+".csv"), index=False)
        print("\nProcess completed successfully - all requested data should now be saved.")

    # Save the metadata about what the file does and doesn't include
    def save_metadata (self, metadata_filename='metadata.txt', output_directory='Output'):
        data = self.data
        # Gathers some metadata to report back
        cols_in_df=data.columns.tolist()
        try:
            potential_PUPRNs_included=len(participant_list)
        except:
            potential_PUPRNs_included='All'
        number_rows_in_df = len(data.index)
        #number_PUPRNs_in_df = len(data.PUPRN.unique().tolist())
        # the next three lines take a long time to run - at least make the list of PUPRNs re-usable..
        number_PUPRNs_in_df = len(self.puprns)
        first_date_in_df = self.first_date_in_df
        last_date_in_df = self.last_date_in_df
        metadata ={'Data resolution':self.res,
               'Columns included':cols_in_df,
               'Participant data filters applied':self.filter_rows_on_participant_cat_data,
               'Survey data filters applied':self.filter_rows_on_survey_cat_data,
               'Resultant number of PUPRNs (participants) that match those filters':potential_PUPRNs_included,
               'Valid_read_time values included':self.filter_Valid_read_time,
               'Gas_flag values included':self.filter_Gas_flag,
               'Elec_act_imp_flag values included':self.filter_Elec_act_imp_flag,
               'Elec_act_exp_flag values included (only applies to hh data)':self.filter_Elec_act_exp_flag,
               'Elec_react_imp_flag values included (only applies to hh data)':self.filter_Elec_react_imp_flag,
               'Elec_react_exp_flag values included (only applies to hh data)':self.filter_Elec_react_exp_flag,
               'First date included (This is the earliest date in the df-may be later than what you requested depending on the available data and the filters applied)':first_date_in_df,
               'Last date included (This is the latest date in the df-may be earlier than what you requested depending on the available data and the filters applied)':last_date_in_df,
               'Were additional local time columns added? (only applies to hh data)':self.add_local_time_cols,
               'Were dates with clock changes included? (NB. 23 or 25 indicates only days when clocks moved forwards or backwards respectively are included)':self.inc_time_change_days,
               'Was an additional gas kWh column added based on Gas_hh_sum_m3? (only applies to daily data with Gas_hh_sum_m3 selected)':self.add_gas_hh_sum,
               'Was an additional net electricity kWh column added based on (Elec_act_imp_hh_Wh - Elec_act_exp_hh_Wh)? (only applies to half-hourly data with Elec_act_imp_hh_Wh and Elec_act_exp_hh_Wh selected)':self.add_net_electricity_column ,
               'Resultant number of unique PUPRNs in the dataframe that met all criteria AND had energy data':number_PUPRNs_in_df,
               'Resultant number of rows':number_rows_in_df
               }
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        with open(os.path.join(output_directory,metadata_filename), 'w',newline='') as f:
            for key in metadata.keys():
                f.write("%s, %s\n" % (key, metadata[key]))
    
        # Feed back some info about the outputs
        print("File saved in directory: ",output_directory,
            "\nDescription of its contents saved as: ",metadata_filename,
            "\n\nHere is that description, for reference:\n")
        for key, value in metadata.items():
            print(key, ': ', value)
        print("\nHere's a sample of the dataframe (the head):\n(NB. The saved csv excludes the index.)\n", data.head())
