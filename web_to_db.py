import os
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
import urllib.request, urllib.parse, urllib.error
import pandas as pd


def web_scraping():

    mobiles_info = dict()
    mobiles_info['Brand_Name'] = []
    mobiles_info['Model_Name'] = []
    mobiles_info['Color'] = []
    mobiles_info['Original_Price(in Rs)'] = []
    mobiles_info['Discount(%)'] = []
    mobiles_info['Offer_Price(in Rs)'] = []
    mobiles_info['RAM'] = []
    mobiles_info['ROM'] = []
    mobiles_info['Expandable_Upto'] = []
    mobiles_info['Resolution'] = []
    mobiles_info['Primary_Camera'] = []
    mobiles_info['Secondary_Camera'] = []
    mobiles_info['Battery_Capacity'] = []
    mobiles_info['Battery_Type'] = []
    mobiles_info['Processor'] = []

    more_data = True
    page_no = 1
    while more_data:
        base_url = 'https://www.flipkart.com/search?q=mobiles&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=off&as=off&page='
        url = base_url + str(page_no)
        page_no = page_no + 1
        req = urllib.request.urlopen(url).read().decode()
        soup = BeautifulSoup(req, features='lxml')

        entries = soup.findAll('a', href=True, attrs={'class': '_1fQZEK'})

        if len(entries) > 0:
            for entry in entries:

                model_name = entry.find('div', attrs={'class': '_4rR01T'}).text.upper()
                brand_name = model_name.split(" ")[0]
                color = model_name[model_name.find("(") + len("("):model_name.find(",")]
                if len(color) < 2: color = "?"
                try: offer = int(entry.find('div', attrs={'class': '_30jeq3 _1_WHN1'}).text[1:].replace(',', ''))
                except: offer = "?"

                try:
                    discount = int(entry.find('div', attrs={'class': '_3Ay6Sb'}).string[:-5])
                except:
                    discount = 0

                if discount == 0:
                    original = offer
                else:
                    original = int(entry.find('div', attrs={'class': '_3I9_wc _27UcVY'}).text.strip()[1:].replace(',', ''))

                features = entry.find('ul', attrs={'class': '_1xgFaf'})
                features_list = []

                for feature in features:
                    features_list.append(feature.text.split("|"))

                try:
                    if 'RAM' in features_list[0][0].upper(): ram = features_list[0][0][:features_list[0][0].upper().find("RAM")]
                    else: ram = "?"
                except: ram = "?"

                try:
                    if 'ROM' in features_list[0][1].upper(): rom = features_list[0][1][:features_list[0][1].upper().find("ROM")]
                    else: rom = "?"
                except: rom = "?"

                try:
                    if 'UPTO' in features_list[0][2].upper(): expand_upto = features_list[0][2][features_list[0][2].upper().find("UPTO")+len("UPTO"):]
                    else: expand_upto = "?"
                except: expand_upto = "?"

                try:
                    if 'DISPLAY' in features_list[1][0].upper(): resolution = features_list[1][0].upper().strip()
                    else: resolution = "?"
                except: resolution = "?"

                try:
                    if 'MP' in features_list[2][0].upper(): primary_cam = features_list[2][0].upper().replace('REAR', '').replace('CAMERA', '').strip()
                    else: primary_cam = "?"
                except: primary_cam = "?"

                try:
                    if 'FRONT' in features_list[2][1].upper(): secondary_cam = features_list[2][1].upper().replace('FRONT', '').replace('CAMERA', '').strip() # .replace('FRONT', '').replace('CAMERA', '')
                    else: secondary_cam = "?"
                except: secondary_cam = "?"

                try:
                    if 'BATTERY' in features_list[3][0].upper():
                        battery_cap = features_list[3][0][:features_list[3][0].find('mAh')].upper().strip()
                        battery_type = features_list[3][0][features_list[3][0].find('mAh')+len('mAh'):].upper().strip()
                    else:
                        battery_cap = "?"
                        battery_type = "?"
                except:
                    battery_cap = "?"
                    battery_type = "?"

                try:
                    if 'PROCESSOR' in features_list[4][0].upper(): processor = features_list[4][0].upper().strip()
                    else: processor = "?"
                except: processor = "?"

                mobiles_info['Brand_Name'].append(brand_name)
                mobiles_info['Model_Name'].append(model_name)
                mobiles_info['Color'].append(color)
                mobiles_info['Original_Price(in Rs)'].append(original)
                mobiles_info['Discount(%)'].append(int(discount))
                mobiles_info['Offer_Price(in Rs)'].append(offer)
                mobiles_info['RAM'].append(ram.strip())
                mobiles_info['ROM'].append(rom.strip())
                mobiles_info['Expandable_Upto'].append(expand_upto.strip())
                mobiles_info['Resolution'].append(resolution)
                mobiles_info['Primary_Camera'].append(primary_cam)
                mobiles_info['Secondary_Camera'].append(secondary_cam)
                mobiles_info['Battery_Capacity'].append(battery_cap + ' mAh')
                mobiles_info['Battery_Type'].append(battery_type)
                mobiles_info['Processor'].append(processor)

        else: more_data = False

    return mobiles_info


def create_dataset(mobiles_info):

    folder_name = 'Mobiles-Flipkart'
    os.makedirs(folder_name, exist_ok=True)
    file_name = 'Mobiles_Data.csv'
    file_loc = os.path.join(folder_name, file_name)
    dataset_mobiles = pd.DataFrame(mobiles_info)
    dataset_mobiles.to_csv(file_loc)
    print("Done")
    return dataset_mobiles, file_loc


def create_database(dataset_mobiles):
    sqlite_file_name = 'Flipkart_Mobiles.sqlite'
    connection_established = False
    try:
        connection = sqlite3.connect(sqlite_file_name)
        connection_established = True

    except:
        print("Unable to connect to the database !!!")
        connection_established = False

    if connection_established:
        dataset_mobiles.to_sql('mobiles', connection, if_exists='replace', index=False)
        connection.commit()
        connection.close()

    return connection_established


if __name__ == '__main__':

    mobiles_info = web_scraping()
    dataset_mobiles, file_loc = create_dataset(mobiles_info)
    connection_established = create_database(dataset_mobiles)
    if connection_established:
        print("--- Data is added to the Database ---")