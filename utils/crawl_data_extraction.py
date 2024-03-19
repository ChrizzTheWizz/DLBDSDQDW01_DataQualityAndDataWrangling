from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
import re
from pathlib import Path
from requests.exceptions import RequestException


def extract_file_url(url):
    """
    Extract file url from website with class c-publication FTxlsx

    :param url: url from website
    :return: file url
    """
    try:
        # send API-Request
        response = requests.get(url)
        if response.status_code == 200:
            # create BeautifulSoup-Object
            soup = BeautifulSoup(response.text, 'html.parser')
            # Search for <a>-Tag with specific class
            link_tag = soup.find('a', class_='c-publication FTxlsx')
            if link_tag:
                # extract href attribute
                link_href = link_tag.get('href')
                # complete url
                base_url = "https://www.kba.de"
                full_link = base_url + link_href
                return full_link
            else:
                return None
        else:
            return response.status_code
    except Exception as error:
        return error


def data_download(url, path):
    """
    Download file from given url

    :param url: url of file
    :param path: file path where file is written to
    :return: status of request
    """
    save_dir = Path(path)

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            with open(save_dir, 'wb') as file:
                file.write(response.content)
            return response.status_code
        else:
            return response.status_code
    except RequestException as error:
        return error
    except IOError as error:
        return error
    except Exception as error:
        return error


# WEATHER DATA
def extract_weather_data(url):
    """
    Extract weather data from website with class temp cell c3

    :param url: url of website
    :return: temperature, precipitation, wind_speed and status of request
    """
    temperature = np.nan
    precipitation = np.nan
    wind_speed = np.nan

    try:
        # receive api response
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # extract temperature
            temperature_element = soup.find('div', class_='temp cell c3')
            if temperature_element:
                temperature = extract_numerical_value(temperature_element.find('span').text)

            # find precipitation and wind speed
            list_items = soup.find_all('li')
            for item in list_items:
                if 'Niederschlagsmenge:' in item.text:
                    precipitation = extract_numerical_value(item.find('span').text)
                elif 'Windstärke:' in item.text:
                    wind_speed = extract_numerical_value(item.find('span').text)

            return temperature, precipitation, wind_speed, response.status_code
        else:
            return None, None, None, response.status_code

    except Exception as e:
        return None, None, None, e


def extract_numerical_value(text):
    """
    Finds and extracts numerical data within given text
    :param text: given text
    :return: numerical value
    """
    # find numerical data
    match = re.search(r'\d+\.?\d*', text)

    return match.group()


# AIR QUALITY DATA
def extract_air_quality_stations(url):
    """
    Extract air quality stations from specific url

    :param url: url to extract air quality station data
    :return: air quality station data and response code
    """
    try:
        # receive api response
        response = requests.get(url)

        # if response is success
        if response.status_code == 200:
            data = response.json()

            return data, response.status_code
        else:
            return None, response.status_code
    except Exception as error:
        return None, error


def extract_station_data(url):
    """
    Extract data from air quality station

    :param url: url to extract air quality station data values
    :return: air quality station data values and response code
    """
    try:
        # receive api response
        response = requests.get(url)

        # if response is success
        if response.status_code == 200:
            # assign data
            data = response.json()

            return data, response.status_code
        else:
            return None, response.status_code
    except Exception as error:
        return None, error


def check_air_quality_station_data(data):
    """
    Checks whether the data is complete or not

    :param data: air quality station data
    :return: List of dates missing
    """
    # to ensure that response is complete
    core_date_check = {}
    core_date_check_result = {'complete': 0, 'incomplete': 0}
    for entry in data:
        if entry['core'] not in core_date_check.keys():
            core_date_check[entry['core']] = []

        core_date_check[entry['core']].append(entry['datetime'])

    for core, dates in core_date_check.items():
        if len(dates) < 24:
            core_date_check_result['incomplete'] += 1
        else:
            core_date_check_result['complete'] += 1

    return core_date_check_result['incomplete']


def extract_traffic_sensors(main_url):
    """
    Extracts metadata about available traffic sensors

    :param main_url: main url for extraction
    :return: metadata about all available traffic sensors
    """
    def extract_single_traffic_sensor_data(url):
        """
        Extracts metadata for specified sensor

        :param url: url for sensor
        :return: sensor metadata
        """
        try:
            # receive api response
            response = requests.get(url)

            # if response is success
            if response.status_code == 200:
                data = response.json()

                return data
            else:
                return None
        except Exception as e:
            return None

    # all sensor ids which have been checked forehand
    sensor_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
                  29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53,
                  54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78,
                  79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102,
                  103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122,
                  123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142,
                  143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162,
                  163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182,
                  183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202,
                  203, 204, 205, 206, 207, 208, 209, 210, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233,
                  234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253,
                  254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273,
                  274, 275, 276, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291]

    # initialize list for metadata
    list_sensors = []

    for sensor_id in sensor_ids:
        # format for specified sensor ID
        sensor_url = main_url.format(sensor_id)

        # extract main sensor metadata
        sensor_data = extract_single_traffic_sensor_data(sensor_url)

        # assign main sensor metadata to dataframe and drop unneeded column 'properties'
        s_station_info = pd.DataFrame(sensor_data)
        s_station_info = s_station_info.iloc[0]
        s_station_info = s_station_info.drop('properties')

        # location of the station
        url_location = s_station_info['Locations@iot.navigationLink']

        # extract location url of sensor
        location_data = extract_single_traffic_sensor_data(url_location)

        # assign location sensor metadata to dataframe
        df_location_info = pd.json_normalize(location_data, record_path=['value'])

        # split coordinates into latitude and longitude
        coordinates = df_location_info['location.coordinates'].values[0]

        # !!! BE AWARE OF THE FOLLOWING COMMENTS

        # THIS IS THE CORRECT CODE! USE THE CODE LIKE BELOW IF YOU RE-INITIALIZE EVERYTHING
        s_station_info['location_latitude'] = coordinates[1]
        s_station_info['location_longitude'] = coordinates[0]

        # THIS WAS THE (SWAPPED) FALSE ASSIGNMENT
        # IF YOU WANT TO COLLECT MORE DATA WITHOUT RE-INITIALIZE EVERYTHING USE THE CODE BELOW
        # s_station_info['location_latitude'] = coordinates[0]
        # s_station_info['location_longitude'] = coordinates[1]

        # extract location url of sensor
        url_datastream = s_station_info['Datastreams@iot.navigationLink']

        # extract observation urls of sensor
        datastream_data = extract_single_traffic_sensor_data(url_datastream)

        # assign observation sensor metadata to dataframe
        df_datastream_info = pd.json_normalize(datastream_data, record_path=['value'])

        # extract specific observation url
        url_observation_index = df_datastream_info[df_datastream_info['description'] ==
                                                   'Anzahl KFZ pro Stunde für TEU: MQ - Messquerschnitt'].index
        url_observation = df_datastream_info.loc[url_observation_index, 'Observations@iot.navigationLink']

        s_station_info['observation_url'] = url_observation.values[0]

        # add single and complete station metadata to list
        list_sensors.append(s_station_info)

    # convert list to dataframe
    df_sensors = pd.DataFrame(list_sensors)
    df_sensors = df_sensors.reset_index(drop=True)

    return df_sensors


def extract_traffic_data(url):
    """
    Extracts observation values from traffic sensor

    :param url: API URL for extraction
    :return: observation values and request status
    """
    try:
        # receive api response
        response = requests.get(url)

        # if response is success
        if response.status_code == 200:
            # assign data
            data = response.json()
            s_traffic_data = pd.json_normalize(data, record_path=['value'])

            return s_traffic_data, response.status_code
        else:
            return None, response.status_code
    except Exception as error:
        return None, error
