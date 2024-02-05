import os
import concurrent.futures
import time
import pandas as pd

# Code is still very messy. #
# But asynchronous batch processing is now implemented. #
# This has improved performance by approximately 4x. #
# Code improvements and cleaning up are the next step. #

# Get rid of FileDump Folder Structure.

file_structure_dict = {}

head_fn = 'headers.txt'
source_dir = 'NOAA Quality Controlled Datasets_raw'

master_folder = 'NOAA Quality Controlled Datasets_csv'
os.mkdir(master_folder)

dataset_list = [x for x in os.listdir(source_dir) if x.startswith('CRN')]

filepath_list_monthly = []
filepath_list_daily = []
filepath_list_hourly = []
filepath_list_subhourly = []

# Build Remaining File Structure... bundle into function?
# God there must be a better way...better coded to save on space, but it works for now and is semi-logical...
for folder in dataset_list:
    if folder.startswith('CRND'):
        os.mkdir(master_folder + '/' + folder)
        subfolder_list = [x for x in os.listdir(source_dir + '/' + folder) if x.startswith('1') or x.startswith('2')]
        for subfolder in subfolder_list:
            os.mkdir(master_folder + '/' + folder + '/' + subfolder)

    elif folder.startswith('CRNH'):
        os.mkdir(master_folder + '/' + folder)
        subfolder_list = [x for x in os.listdir(source_dir + '/' + folder) if x.startswith('1') or x.startswith('2')]
        for subfolder in subfolder_list:
            os.mkdir(master_folder + '/' + folder + '/' + subfolder)

    elif folder.startswith('CRNS'):
        os.mkdir(master_folder + '/' + folder)
        subfolder_list = [x for x in os.listdir(source_dir + '/' + folder) if x.startswith('1') or x.startswith('2')]
        for subfolder in subfolder_list:
            os.mkdir(master_folder + '/' + folder + '/' + subfolder)

# Get Filepath List for Datasets
# Structuring as a big list so that they can be handled synchronously way more easily.
for dataset in dataset_list:
    if dataset.startswith('CRND'):
        folder_list = [dataset + '/' + x for x in os.listdir(source_dir + '/' + dataset) if x.startswith('1') or x.startswith('2')]
        for folder in folder_list:
            filepath_list_daily = filepath_list_daily + [(folder + '/' + x) for x in os.listdir(source_dir + '/' + folder)]
    if dataset.startswith('CRNH'):
        folder_list = [dataset + '/' + x for x in os.listdir(source_dir + '/' + dataset) if x.startswith('1') or x.startswith('2')]
        for folder in folder_list:
            filepath_list_hourly = filepath_list_hourly + [(folder + '/' + x) for x in os.listdir(source_dir + '/' + folder)]
    if dataset.startswith('CRNS'):
        folder_list = [dataset + '/' + x for x in os.listdir(source_dir + '/' + dataset) if x.startswith('1') or x.startswith('2')]
        for folder in folder_list:
            filepath_list_subhourly = filepath_list_subhourly + [(folder + '/' + x) for x in os.listdir(source_dir + '/' + folder)]

    elif dataset.startswith('CRNM'):
        filepath_list_monthly = [(dataset + '/' + x) for x in os.listdir(source_dir + '/' + dataset) if x.startswith('CRNM')]


# Publish CSVs somehow...
def publish_test_daily(source_path, subfolder_destination, col_names):
    data_raw = open(source_dir + '/' + source_path, 'r', encoding='utf-8')
    data_lines = data_raw.readlines()
    data_line_list = []

    for i in range(len(data_lines)):
        data_line_list.append(data_lines[i].split())

    export_df = pd.DataFrame(data_line_list)
    export_df.columns = col_names
    # export_df['UTC_DATE'] = pd.to_datetime(export_df['UTC_DATE'])
    export_df['LST_DATE'] = pd.to_datetime(export_df['LST_DATE'])

    export_df.to_csv(subfolder_destination + '/' + source_path.replace('txt', 'csv'),
                     index=False,
                     date_format='%Y-%m-%d',
                     escapechar='*',
                     encoding='utf-8')

    data_raw.close()


def publish_test_hourly(source_path, subfolder_destination, col_names):
    data_raw = open(source_dir + '/' + source_path, 'r', encoding='utf-8')
    data_lines = data_raw.readlines()
    data_line_list = []

    for i in range(len(data_lines)):
        data_line_list.append(data_lines[i].split())

    export_df = pd.DataFrame(data_line_list)
    export_df.columns = col_names
    export_df['UTC_DATE'] = pd.to_datetime(export_df['UTC_DATE'])
    export_df['LST_DATE'] = pd.to_datetime(export_df['LST_DATE'])

    export_df.to_csv(subfolder_destination + '/' + source_path.replace('txt', 'csv'),
                     index=False,
                     date_format='%Y-%m-%d',
                     escapechar='*',
                     encoding='utf-8')

    data_raw.close()


def publish_test_subhourly(source_path, subfolder_destination, col_names):
    data_raw = open(source_dir + '/' + source_path, 'r', encoding='utf-8')
    data_lines = data_raw.readlines()
    data_line_list = []

    for i in range(len(data_lines)):
        data_line_list.append(data_lines[i].split())

    export_df = pd.DataFrame(data_line_list)
    export_df.columns = col_names
    export_df['UTC_DATE'] = pd.to_datetime(export_df['UTC_DATE'])
    export_df['LST_DATE'] = pd.to_datetime(export_df['LST_DATE'])

    export_df.to_csv(subfolder_destination + '/' + source_path.replace('txt', 'csv'),
                     index=False,
                     date_format='%Y-%m-%d',
                     escapechar='*',
                     encoding='utf-8')

    data_raw.close()


start = time.time()

with concurrent.futures.ProcessPoolExecutor() as executor:
    for ds in dataset_list:

        head_raw = open(source_dir + '/' + ds + '/' + head_fn, 'r', encoding='utf-8')
        col_index = head_raw.readlines(1)[0].split()
        col_names = head_raw.readlines(2)[0].split()

        if ds.startswith('CRNH'):
            print('Publishing Hourly...')

            for filepath in filepath_list_hourly:
                executor.submit(publish_test_hourly, filepath, master_folder, col_names)

print(f'Processing Time ::: {round(time.time() - start)/60} minute(s)\n')

start = time.time()

with concurrent.futures.ProcessPoolExecutor() as executor:
    for ds in dataset_list:
        head_raw = open(source_dir + '/' + ds + '/' + head_fn, 'r', encoding='utf-8')
        col_index = head_raw.readlines(1)[0].split()
        col_names = head_raw.readlines(2)[0].split()
        if ds.startswith('CRND'):
            print('Publishing Daily...')
            for filepath in filepath_list_daily:
                executor.submit(publish_test_daily, filepath, master_folder, col_names)

print(f'Performance ::: {round(time.time() - start)/60} minute(s)\n')

start = time.time()

with concurrent.futures.ProcessPoolExecutor() as executor:
    for ds in dataset_list:
        head_raw = open(source_dir + '/' + ds + '/' + head_fn, 'r', encoding='utf-8')
        col_index = head_raw.readlines(1)[0].split()
        col_names = head_raw.readlines(2)[0].split()
        if ds.startswith('CRNS'):
            print('Publishing SubHourly...')
            for filepath in filepath_list_subhourly:
                executor.submit(publish_test_subhourly, filepath, master_folder, col_names)

print(f'Performance ::: {round(time.time() - start)/60} minute(s)\n')
