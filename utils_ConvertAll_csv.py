import os
import sys
import pandas as pd
import time

run_time = time.time()
set_step_time = time.time()
folder_step_time = time.time()

dir_dict = {}
head_fn = 'headers.txt'

master_folder = 'NOAA Quality Controlled Datasets_csv'
source_dir = 'NOAA Quality Controlled Datasets_dl'
os.mkdir(master_folder)

daily_keys = [x for x in os.listdir(source_dir) if x.startswith('CRN')]
daily_keys_sub = []

for key in daily_keys:
    if key.startswith('CRNM') is False:
        folder_ls = [x for x in os.listdir(source_dir + '/' + key) if x.startswith('1') or x.startswith('2')]
        dir_dict[key] = {}
        for folder in folder_ls:
            dir_dict[key][folder] = os.listdir(source_dir + '/' + key + '/' + folder)
    elif key.startswith('CRNM') is True:
        dir_dict[key] = [x for x in os.listdir(source_dir + '/' + key) if x.startswith('CRNM')]

for key in dir_dict:
    head_raw = open(source_dir + '/' + key + '/' + head_fn, 'r', encoding='utf-8')
    col_index = head_raw.readlines(1)[0].split()
    col_names = head_raw.readlines(2)[0].split()

    if key.startswith('CRNM'):
        print('Publishing Monthly...')
        export_path = master_folder + '/MonthlyFileDump_csv'
        os.mkdir(export_path)

        for file in dir_dict[key]:
            data_raw = open(source_dir + '/' + key + '/' + file, 'r', encoding='utf-8')
            data_lines = data_raw.readlines()
            data_line_list = []

            for i in range(len(data_lines)):
                data_line_list.append(data_lines[i].split())

            export_df = pd.DataFrame(data_line_list)
            export_df.columns = col_names
            export_df['LST_YRMO'] = pd.to_datetime(export_df['LST_YRMO'].astype(str) + '01')
            export_df.to_csv(export_path + '/' + file.replace('txt', 'csv'),
                             index=False,
                             date_format='%Y-%m',
                             encoding='utf-8')

            data_raw.close()

        print(f'Time Elapsed: {round((time.time() - set_step_time) / 60)}m\n')
        set_step_time = time.time()

    else:
        if key.startswith('CRND'):
            print('Publishing Daily...')
            export_path = master_folder + '/DailyFileDump_csv'
            os.mkdir(export_path)
            for folder in dir_dict[key]:
                os.mkdir(export_path + '/' + folder)

                for file in dir_dict[key][folder]:
                    data_raw = open(source_dir + '/' + key + '/' + folder + '/' + file, 'r', encoding='utf-8')
                    data_lines = data_raw.readlines()
                    data_line_list = []

                    for i in range(len(data_lines)):
                        data_line_list.append(data_lines[i].split())

                    export_df = pd.DataFrame(data_line_list)
                    export_df.columns = col_names
                    export_df['LST_DATE'] = pd.to_datetime(export_df['LST_DATE'])

                    export_df.to_csv(export_path + '/' + folder + '/' + file.replace('txt', 'csv'),
                                     index=False,
                                     date_format='%Y-%m-%d',
                                     escapechar='*',
                                     encoding='utf-8')

                    data_raw.close()

                print(f'Folder {folder} Completed: {round(time.time() - folder_step_time)}s, '
                      f'{round((time.time() - set_step_time) / 60)}m')
                folder_step_time = time.time()
            print(f'Set HourlyFileDump_csv Competed: {round((time.time() - set_step_time) / 60)}m\n')
            set_step_time = time.time()

        elif key.startswith('CRNH'):
            print('Publishing Hourly...')
            export_path = master_folder + '/HourlyFileDump_csv'
            os.mkdir(export_path)
            for folder in dir_dict[key]:
                os.mkdir(export_path + '/' + folder)

                for file in dir_dict[key][folder]:
                    data_raw = open(source_dir + '/' + key + '/' + folder + '/' + file, 'r', encoding='utf-8')
                    data_lines = data_raw.readlines()
                    data_line_list = []

                    for i in range(len(data_lines)):
                        data_line_list.append(data_lines[i].split())

                    export_df = pd.DataFrame(data_line_list)
                    export_df.columns = col_names
                    export_df['UTC_DATE'] = pd.to_datetime(export_df['UTC_DATE'])
                    export_df['LST_DATE'] = pd.to_datetime(export_df['LST_DATE'])

                    export_df.to_csv(export_path + '/' + folder + '/' + file.replace('txt', 'csv'),
                                     index=False,
                                     date_format='%Y-%m-%d',
                                     escapechar='*',
                                     encoding='utf-8')

                    data_raw.close()

                print(f'Folder {folder} Completed: {round(time.time() - folder_step_time)}s, '
                      f'{round((time.time() - set_step_time) / 60)}m')
                folder_step_time = time.time()
            print(f'Set HourlyFileDump_csv Competed: {round((time.time() - set_step_time) / 60)}m\n')
            set_step_time = time.time()

        elif key.startswith('CRNS'):
            print('Publishing SubHourly...')
            export_path = master_folder + '/SubHourlyFileDump_csv'
            os.mkdir(export_path)
            for folder in dir_dict[key]:
                os.mkdir(export_path + '/' + folder)

                for file in dir_dict[key][folder]:
                    data_raw = open(source_dir + '/' + key + '/' + folder + '/' + file, 'r', encoding='utf-8')
                    data_lines = data_raw.readlines()
                    data_line_list = []

                    for i in range(len(data_lines)):
                        data_line_list.append(data_lines[i].split())

                    export_df = pd.DataFrame(data_line_list)
                    export_df.columns = col_names
                    export_df['UTC_DATE'] = pd.to_datetime(export_df['UTC_DATE'])
                    export_df['LST_DATE'] = pd.to_datetime(export_df['LST_DATE'])

                    export_df.to_csv(export_path + '/' + folder + '/' + file.replace('txt', 'csv'),
                                     index=False,
                                     date_format='%Y-%m-%d',
                                     escapechar='*',
                                     encoding='utf-8')

                    data_raw.close()

                print(f'Folder {folder} Completed: {round(time.time() - folder_step_time)}s, '
                      f'{round((time.time() - set_step_time) / 60)}m')
                folder_step_time = time.time()
            print(f'Set HourlyFileDump_csv Competed: {round((time.time() - set_step_time) / 60)}m\n')
            set_step_time = time.time()

print(f'Total Run Time: {round((time.time() - run_time) / 60)}m')
folder_step_time = time.time()
