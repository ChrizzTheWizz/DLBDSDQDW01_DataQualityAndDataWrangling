from pathlib import Path
import h5py
import pandas as pd
import numpy as np
import json
from dateutil import parser
from datetime import datetime


def add_air_quality_data(path_h5, data):
    """
    Adds air quality data to HDF5 File

    :param path_h5: path to HDF5 File
    :param data: Data to be added
    :return:
    """
    hdf5_file = Path(path_h5)
    if hdf5_file.exists():
        with h5py.File(path_h5, 'a') as hdf5_file:
            for entry in data:
                # converting the date to a Unix timestamp
                timestamp = parser.parse(entry['datetime']).timestamp()

                # determine dataset path within station group
                dataset_path = f"air_quality/{entry['station']}/{entry['component']}"

                if dataset_path in hdf5_file:
                    # expanding the existing dataset
                    dataset = hdf5_file[dataset_path]
                    dataset.resize((dataset.shape[0] + 1, 2))

                    # add data
                    dataset[-1] = [timestamp, entry['value']]

                else:
                    print(f"{entry['component']} not in {entry['station']}")

    else:
        raise FileNotFoundError


def add_weather_data(path_h5, temperature, precipitation, wind_speed):
    """
    Adds weather data to HDF5 File

    :param path_h5: path to HDF5 File
    :param temperature: temperature
    :param precipitation: precipitation
    :param wind_speed: wind speed
    :return:
    """
    hdf5_file = Path(path_h5)
    if hdf5_file.exists():
        with h5py.File(path_h5, 'a') as hdf5_file:
            # determine dataset path
            dataset_path = 'weather/weather_data'

            if dataset_path in hdf5_file:
                dataset = hdf5_file[dataset_path]

                # create timestamp
                timestamp = datetime.now().timestamp()

                # expanding the existing dataset
                dataset.resize((dataset.shape[0] + 1, 4))

                # add data
                dataset[-1] = [timestamp, temperature, precipitation, wind_speed]
            else:
                print('Dataset not found')
    else:
        raise FileNotFoundError


def add_car_regs_data(path_h5, data):
    """
    Adds car registrations data to HDF5 File

    :param path_h5: path to HDF5 File
    :param data: car registrations data
    :return:
    """
    hdf5_file = Path(path_h5)
    if hdf5_file.exists():
        with h5py.File(path_h5, 'a') as hdf5_file:
            # determine dataset path
            dataset_path = 'car_registrations/car_registrations_data'

            if dataset_path in hdf5_file:
                dataset = hdf5_file[dataset_path]

                # expanding the existing dataset
                dataset.resize((dataset.shape[0] + 1, 7))
                data_array = np.array(data)

                # add data
                dataset[-1] = data_array
            else:
                print('Dataset not found')
    else:
        raise FileNotFoundError


def add_new_car_regs_data(path_h5, data):
    """
    Adds new car registrations data to HDF5 File

    :param path_h5: path to HDF5 File
    :param data: car registrations data
    :return:
    """
    hdf5_file = Path(path_h5)
    if hdf5_file.exists():
        with h5py.File(path_h5, 'a') as hdf5_file:
            # determine dataset path
            dataset_path = 'new_car_registrations/new_car_registrations_data'

            if dataset_path in hdf5_file:
                dataset = hdf5_file[dataset_path]

                # expanding the existing dataset
                dataset.resize((dataset.shape[0] + 1, 8))
                data_array = np.array(data)

                # add data
                dataset[-1] = data_array
            else:
                print('Dataset not found')

    else:
        raise FileNotFoundError


def add_construction_data(path_h5, data):
    """
    Adds construction data to HDF5 File

    :param path_h5: path to HDF5 File
    :param data: construction data
    :return:
    """
    def convert_row_to_string(cell):
        if isinstance(cell, (list, dict)):
            return json.dumps(cell)
        else:
            return str(cell)

    def reconvert_cells(cell):
        try:
            return json.loads(cell)
        except json.JSONDecodeError:
            return cell.decode('utf-8')

    hdf5_file_path = Path(path_h5)

    if hdf5_file_path.exists():
        # determine dataset path
        dataset_path = '/constructions/construction_data'

        with h5py.File(hdf5_file_path, 'a') as hdf5_file:
            if dataset_path in hdf5_file:
                # read existing data
                dataset = hdf5_file[dataset_path]

                # read column names from attributes
                columns = [column.decode('utf-8') for column in dataset.attrs['columns']]

                # create dataframe from existing dataset data
                existing_data = pd.DataFrame(dataset[:], columns=columns)
                existing_data = existing_data.applymap(reconvert_cells)

                for index, row in data.iterrows():
                    construction_id = str(row['properties.id'])

                    # conversion of the line into a numpy array
                    string_row = row.apply(convert_row_to_string)
                    string_array = np.array(string_row.tolist(), dtype=h5py.string_dtype())

                    if existing_data.empty:
                        # if there's no data only expand dataset and add data
                        dataset.resize((dataset.shape[0] + 1,) + dataset.shape[1:])
                        dataset[-1] = string_array
                    else:
                        # check for existing ID and save index if ID exists
                        existing_row_index = existing_data.index[existing_data['ID'] == construction_id].tolist()

                        # if there's data check for IDs already existing
                        if existing_row_index:
                            # if ID is already in dataset: overwrite data
                            dataset[existing_row_index[0]] = string_array
                        else:
                            # if ID is not in dataset: write new data
                            dataset.resize((dataset.shape[0] + 1,) + dataset.shape[1:])
                            dataset[-1] = string_array

                return True

            else:
                print('Dataset not found')
                return False
    else:
        raise FileNotFoundError


def add_traffic_data(path_h5, sensor_name, timestamp, result):
    """
    Adds traffic data to HDF5 File

    :param path_h5: path to HDF5 File
    :param sensor_name: sensor name/code
    :param timestamp: timestamp
    :param result: result / value
    :return:
    """
    hdf5_file_path = Path(path_h5)
    if hdf5_file_path.exists():

        # determine dataset path
        dataset_path = f'/traffic/{sensor_name}'

        with h5py.File(hdf5_file_path, 'a') as hdf5_file:
            if dataset_path in hdf5_file:
                dataset = hdf5_file[dataset_path]

                # expanding the existing dataset
                dataset.resize((dataset.shape[0] + 1, 2))

                # add data
                dataset[-1] = [timestamp, result]
            else:
                print('Dataset not found')
    else:
        raise FileNotFoundError
