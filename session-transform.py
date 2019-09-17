# system imports
import datetime
import json
import os
import time
import zipfile
import re
import pandas as pd
from pandas.io.json import json_normalize


def read_zips_from_folder(folder_name):
    sessions_folder = [folder_name]
    folder_items = sorted(os.listdir(folder_name))
    zip_files = [sessions_folder[0] + '/' + s for s in folder_items if s.endswith('.zip')]
    return zip_files


def read_data_files(sessions):
    df_all = pd.DataFrame()  # Dataframe with all summarised data
    df_ann = pd.DataFrame()  # Dataframe containing the annotations
    # for each session in the list of sessions
    for s in sessions:
        # 1. Reading data from zip file
        with zipfile.ZipFile(s) as z:
            # get current absolute time in seconds. This is necessary to add the delta correctly
            for info in z.infolist():
                file_datetime = datetime.datetime(*info.date_time)
            current_time_offset = pd.to_datetime(pd.to_datetime(file_datetime, format='%H:%M:%S.%f'), unit='s')
            # First look for annotation.json
            for filename in z.namelist():
                if not os.path.isdir(filename):
                    if '.json' in filename:
                        with z.open(filename) as f:
                            data = json.load(f)
                        if  'frames' in data:
                            sensor_file_start_loading = time.time()
                            df = sensor_file_to_array(data, current_time_offset)
                            sensor_file_stop_loading = time.time()
                            # Concatenate this dataframe in the dfALL and then sort dfALL by index
                            df_all = pd.concat([df_all, df], ignore_index=False, sort=False).sort_index()
    return df_all

def sensor_file_to_array(data, offset):
    # concatenate the data with the intervals normalized and drop attribute 'frames'
    df = pd.concat([pd.DataFrame(data),
                    json_normalize(data['frames'])],
                   axis=1).drop('frames', 1)
    # remove underscore from column-file e.g. 3_Ankle_Left_X becomes 3AnkleLeftX
    df.columns = df.columns.str.replace("_", "")

    # from string to timedelta + offset
    df['frameStamp'] = pd.to_timedelta(df['frameStamp']) + offset

    # retrieve the application name
    # app_name = df.applicationName.all()
    # remove the prefix 'frameAttributes.' from the column names
    df.columns = df.columns.str.replace("frameAttributes", df.applicationName.all())

    # set the timestamp as index
    df = df.set_index('frameStamp').iloc[:, 2:]
    # exclude duplicates (taking the first occurence in case of duplicates)
    df = df[~df.index.duplicated(keep='first')]
    # convert to numeric (when reading from JSON it converts into object in the pandas DF)
    # with the parameter 'ignore' it will skip all the non-numerical fields
    # df = df.apply(pd.to_numeric, errors='ignore')
    df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    # Keep the numeric types only (categorical data are not supported now)
    df = df.select_dtypes(include=['float64', 'int64'])
    # Remove columns in which the sum of attributes is 0 (meaning there the information is 0)
    df = df.loc[:, (df.sum(axis=0) != 0)]

    # Exclude irrelevant attributes
    #for el in to_exclude:
    #    df = df[[col for col in df.columns if el not in col]]
    df = df.apply(pd.to_numeric).fillna(method='bfill')
    return df

expert_sessions = read_zips_from_folder('selected_sessions/expert')
expert_data = read_data_files(expert_sessions)
expert_penpressure = expert_data['CTEG.PenPressure'].dropna().to_numpy()
print("Expert")
print(expert_penpressure)

novice1_sessions = read_zips_from_folder('selected_sessions/novice1')
novice1_data = read_data_files(expert_sessions)
novice1_penpressure = expert_data['CTEG.PenPressure'].dropna().to_numpy()
print("Novice1")
print(novice1_penpressure)

novice2_sessions = read_zips_from_folder('selected_sessions/novice2')
novice2_data = read_data_files(expert_sessions)
novice2_penpressure = expert_data['CTEG.PenPressure'].dropna().to_numpy()
print("Novice2")
print(novice2_penpressure)