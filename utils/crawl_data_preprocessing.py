import pandas as pd
import json
from datetime import datetime


def read_constructions(path):
    """
    reading constructions data from JSON file

    :param path: file path to JSON file
    :return: dataframe with constructions data
    """
    if path.exists():
        with open(path, 'r', encoding='UTF8') as file:
            construction_data = json.load(file)

        # normalize JSON data
        df_constructions = pd.json_normalize(construction_data, record_path=['features'])

        return df_constructions

    else:
        return None


def preprocess_constructions(path, df_constructions_old):
    """
    Preprocess constructions data

    :param path: file path to JSON file
    :param df_constructions_old: Already existing construction data
    :return: dataframe with construction data to be added to HDF5 File
    """

    # read current construction data from JSON file
    df_constructions = read_constructions(path)

    # if older construction data is available
    if df_constructions_old is not None:
        # determine columns to check duplicates
        check_columns = df_constructions.columns.difference(['geometry.coordinates', 'geometry.geometries'])

        # concat both dataframes
        df_constructions = pd.concat([df_constructions.assign(source='new'), df_constructions_old.assign(source='old')])

        # drop columns related to the columns to be checked
        df_constructions = df_constructions.drop_duplicates(subset=check_columns, keep=False)

        # keep only new data
        df_constructions = df_constructions[df_constructions['source'] == 'new']

    # filter data
    df_constructions = df_constructions[df_constructions['properties.subtype'].isin(
        ['Baustelle', 'Bauarbeiten', 'Sperrung']
    )]

    # only adopt required columns
    df_constructions = df_constructions[['properties.id', 'properties.tstore',
                                         'properties.subtype', 'properties.severity',
                                         'properties.validity.from', 'properties.validity.to',
                                         'properties.direction',
                                         'geometry.type', 'geometry.coordinates', 'geometry.geometries']]

    return df_constructions


def read_car_regs(path):
    """
    Reads car registration data from Excel file

    :param path: path to excel file
    :return: dataframe with car registration data
    """
    if path.exists():
        # read all rows from sheet FZ1.2 and columns B,F,G,H,I,K,L
        all_data = pd.read_excel(path, sheet_name='FZ1.2', header=None, usecols='B,F,G,H,I,K,L')

        # filter for berlin
        row_index = all_data[all_data[1] == 'BERLIN INSGESAMT'].index
        row_data = all_data.iloc[row_index, 1:]

        # replace - with zeros
        row_data = row_data.replace('-', 0)

        return row_data

    else:
        raise FileNotFoundError


def preprocess_car_regs(data, date):
    """
    Adds new column "Year" to dataframe

    :param data: dataframe with car registrations data
    :param date: date (year)
    :return: dataframe with car registrations data and date
    """
    year = int(date)
    data.insert(0, 'Year', year)
    data = data.astype('uint32')

    return data


def read_new_car_regs(path):
    """
    Reads new car registration data from Excel file

    :param path: path to excel file
    :return: dataframe with new car registration data
    """
    if path.exists():
        # read all rows from sheet FZ 8.6 and columns B,C,G,K,L,M,N,P
        all_data = pd.read_excel(path, sheet_name='FZ 8.6', header=None, usecols='B,C,G,K,L,M,N,P')

        # filter for berlin
        row_index = all_data[all_data[1] == 'Berlin'].index
        row_data = all_data.iloc[row_index, 1:]

        # replace - with zeros
        row_data = row_data.replace('-', 0)

        # combine two columns and drop second
        row_data[10] = row_data[10] + row_data[11].fillna(0)
        row_data = row_data.drop(11, axis=1)

        return row_data

    else:
        return None


def preprocess_new_car_regs(data, date):
    """
    Adds two new columns "Year" and "Month" to dataframe

    :param data: dataframe with new car registrations data
    :param date: date (year and month)
    :return: dataframe with new car registrations data and date
    """
    year = int(date[:4])
    month = int(date[4:])
    data.insert(0, 'Month', month)
    data.insert(0, 'Year', year)
    data = data.astype('uint32')

    return data


def preprocess_traffic_data(data):
    """
    Creates timestamp for traffic data and returns traffic value as result

    :param data: dataframe with traffic data
    :return: timestamp and traffic value
    """

    # transform timestamp
    timestamp_string = data['phenomenonTime'].values[0]
    timestamp_string = timestamp_string.split('/')[-1].replace('Z', '')
    timestamp = datetime.fromisoformat(timestamp_string)
    timestamp = timestamp.timestamp()

    # read result value
    result = data['result'].values[0]

    return timestamp, result
