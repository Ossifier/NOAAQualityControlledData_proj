import os
import sys
import requests
import concurrent.futures
import time

from datetime import date
from tqdm import tqdm
from bs4 import BeautifulSoup

today = date.today().strftime('%Y%m%d')

if os.path.isdir('NOAA Quality Controlled Datasets_dl/') is False:
    os.mkdir('NOAA Quality Controlled Datasets_dl/')

dl_flag = input('Enter a Key to Select a Dataset:\n\n'
                'NOAA Quality Controlled Monthly (M)\n'
                'NOAA Quality Controlled Daily (D)\n'
                'NOAA Quality Controlled Hourly (H)\n'
                'NOAA Quality Controlled SubHourly (S)\n\n'
                'Please Enter Your Selection: ')

if dl_flag.upper() == 'M':
    dl_folder_name = 'CRNM' + str(today)
    master_url = 'https://www.ncei.noaa.gov/pub/data/uscrn/products/monthly01/'
elif dl_flag.upper() == 'D':
    dl_folder_name = 'CRND' + str(today)
    master_url = 'https://www.ncei.noaa.gov/pub/data/uscrn/products/daily01/'
elif dl_flag.upper() == 'H':
    dl_folder_name = 'CRNH' + str(today)
    master_url = 'https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/'
elif dl_flag.upper() == 'S':
    dl_folder_name = 'CRNS' + str(today)
    master_url = 'https://www.ncei.noaa.gov/pub/data/uscrn/products/subhourly01/'
else:
    print('Incorrect Selection. Quitting...')
    sys.exit()

run_time = time.time()

os.mkdir('NOAA Quality Controlled Datasets_dl/' + dl_folder_name)

page = requests.get(master_url)
soup = BeautifulSoup(page.content, 'html.parser')
hrefs = soup.find_all('a', href=True)
dl_file_list = []


def download_file(file, flag):
    dl_file_url = requests.get(master_url + file)
    open('NOAA Quality Controlled Datasets_dl' + '/' + dl_folder_name + '/' + file, 'wb').write(dl_file_url.content)
    if flag == 'M':
        print(f'File Downloaded: {file}')
    else:
        print(f'File Downloaded: {file[5:]}')


print(f'Building Local File Structure... NOAA Quality Controlled Datasets_dl/{dl_folder_name}')
if dl_flag == 'M':
    for i in hrefs:
        if "CRNM" in i.text or 'headers' in i.text or 'readme' in i.text:
            dl_file_list.append(i.text)
else:
    for i in tqdm(hrefs):
        if i.text.startswith('2'):
            os.mkdir('NOAA Quality Controlled Datasets_dl/' + dl_folder_name + '/' + i.text[:4])

            dl_url = master_url + i.text
            dl_page = requests.get(dl_url)
            dl_soup = BeautifulSoup(dl_page.content, 'html.parser')
            dl_hrefs = dl_soup.find_all('a', href=True)

            for f in dl_hrefs:
                if "CRN" in f.text:
                    dl_file_list.append(i.text + f.text)

        elif 'header' in i.text or 'readme' in i.text:
            dl_misc_url = requests.get(master_url + i.text)
            open('NOAA Quality Controlled Datasets_dl/' + dl_folder_name + '/' + i.text, 'wb').write(dl_misc_url.content)

with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = []
    for f in dl_file_list:
        futures.append(executor.submit(download_file, file=f, flag=dl_flag))

print(f'\n---COMPLETE---')
print(f'Files Downloaded: {master_url}')
print(f'Total Runtime: {round((time.time() - run_time) / 60)}m')
