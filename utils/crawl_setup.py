from pathlib import Path
import yaml
import h5py
import os
import numpy as np
import json


class HDF5PreconditionError(Exception):
    def __init__(self, message="Error occurred when obtaining necessary data to initialize the HDF5 file"):
        self.message = message
        super().__init__(self.message)


def read_config_file():
    """
    Reading config file containing
    - filepaths
    - subject URLs

    :return: configuration data
    """
    file = Path('./config/config.yaml')

    if file.exists():
        with open(file, 'r') as config_file:
            config_data = yaml.safe_load(config_file)
        return config_data
    else:
        raise FileNotFoundError


def check_data_files(files):
    """
    Checks if main file listed in dictionary exists

    :param files: dictionary with key subject and according file path
    :return: True/False for every key subject in dictionary
    """
    status = files.copy()
    for key, path in status.items():
        file = Path(path)
        status[key] = file.exists()

    return status


def check_directories(directories):
    """
    Checks if directory within dictionary exists

    :param directories: dictionary with key subject and according directory
    :return: True/False for every key subject in dictionary
    """
    status = directories.copy()
    for key, path in directories.items():
        directory = Path(path)
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            status[key] = f'{directory} created'
        else:
            status[key] = f'{directory} already exists'

    return status


def initialize_h5_file(path, stations, sensors):
    """
    Initializes HDF5 File.

    :param path: Path to HDF5 File
    :param stations: Air Quality Stations for grouping HDF5 File (single groups)
    :param sensors: Traffic Sensors for grouping HDF5 File (single datasets)
    :return: -nothing- (creates HDF5 File)
    """
    file = Path(path)
    if file.exists():
        os.remove(path)

    hdf5_file = h5py.File(file, 'w')

    # ------------------------------------------------------------------------------------------------------------------
    # air quality ------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    air_quality_group = hdf5_file.create_group('air_quality')

    # create group for every station
    for station in stations:
        station_group = air_quality_group.create_group(station['code'])

        # assign attributes to group
        station_group.attrs['name'] = \
            station['name'] if station['name'] is not None else 'None'

        station_group.attrs['codeEu'] = \
            station['codeEu'] if station['codeEu'] is not None else 'None'

        station_group.attrs['address'] = \
            station['address'] if station['address'] is not None else 'None'

        station_group.attrs['lat'] = \
            station['lat'] if station['lat'] is not None else 'None'

        station_group.attrs['lng'] = \
            station['lng'] if station['lng'] is not None else 'None'

        station_group.attrs['stationgroups'] = \
            station['stationgroups'] if station['stationgroups'] is not None else 'None'

        station_group.attrs['information'] = \
            station['information'] if station['information'] is not None else 'None'

        # create dataset for every active component
        for component in station['activeComponents']:
            # only if component doesn't already exist and belongs to 1h values (_1h)
            if component not in station_group and component[-3:] == '_1h':
                # create dataset
                dataset = station_group.create_dataset(component, shape=(0, 2), data=[], maxshape=(None, 2), dtype='f')

                # assign attributes to dataset
                dataset.attrs['columns'] = np.array(['Timestamp', 'Value'], dtype='S')

    # ------------------------------------------------------------------------------------------------------------------
    # traffic sensors --------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------

    # create group
    traffic_group = hdf5_file.create_group('traffic')

    for index, row in sensors.iterrows():
        # extract sensor name and encode in UTF-8
        sensor_name = row['name']
        sensor_name = sensor_name.encode('utf-8')

        sensor_info_json = json.dumps({
            '@iot.selfLink': row['@iot.selfLink'],
            '@iot.id': row['@iot.id'],
            'name': row['name'],
            'description': row['description'],
            'HistoricalLocations@iot.navigationLink': row['HistoricalLocations@iot.navigationLink'],
            'Locations@iot.navigationLink': row['Locations@iot.navigationLink'],
            'Datastreams@iot.navigationLink': row['Datastreams@iot.navigationLink'],
            'location_latitude': row['location_latitude'],
            'location_longitude': row['location_longitude'],
            'observation_url': row['observation_url']
        })

        # create dataset
        sensor_dataset = traffic_group.create_dataset(sensor_name, shape=(0, 2), data=[], maxshape=(None, 2), dtype='f')

        # assign attributes
        sensor_dataset.attrs['columns'] = np.array(['Timestamp', 'Traffic'], dtype='S')
        sensor_dataset.attrs['sensor_info_columns'] = np.array(['@iot.selfLink', '@iot.id', 'name',
                                                                'description',
                                                                'HistoricalLocations@iot.navigationLink',
                                                                'Locations@iot.navigationLink',
                                                                'Datastreams@iot.navigationLink',
                                                                'location_latitude', 'location_longitude',
                                                                'observation_url'],
                                                               dtype='S')
        sensor_dataset.attrs['sensor_information'] = np.void(sensor_info_json.encode('utf-8'))

    # ------------------------------------------------------------------------------------------------------------------
    # weather ----------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------

    # create group
    weather_group = hdf5_file.create_group('weather')

    # create dataset
    weather_dataset = weather_group.create_dataset('weather_data', shape=(0, 4), data=[], maxshape=(None, 4), dtype='f')

    # assign attributes
    weather_dataset.attrs['columns'] = np.array(['Timestamp', 'Temperature', 'Precipitation', 'Wind Speed'], dtype='S')

    # ------------------------------------------------------------------------------------------------------------------
    # constructions ----------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------

    # create group
    constructions_group = hdf5_file.create_group('constructions')

    # create dataset
    constructions_dataset = constructions_group.create_dataset('construction_data', shape=(0, 10), data=[],
                                                               maxshape=(None, 10), dtype=h5py.string_dtype())

    # assign attributes
    constructions_dataset.attrs['columns'] = np.array(['ID', 'Timestamp', 'Subtype', 'Severity', 'Valid from',
                                                       'Valid to', 'Direction', 'Geo type', 'Coordinates',
                                                       'Geometries'], dtype='S')

    # ------------------------------------------------------------------------------------------------------------------
    # car_registrations ------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------

    # create group
    car_registrations_group = hdf5_file.create_group('car_registrations')

    # create dataset
    car_registrations_dataset = car_registrations_group.create_dataset('car_registrations_data',
                                                                       shape=(0, 7), data=[],
                                                                       maxshape=(None, 7), dtype='uint32')

    # assign attributes
    car_registrations_dataset.attrs['columns'] = np.array(['Year', 'Gasoline', 'Diesel',
                                                           'LPG+CNG', 'Hybrid', 'BEV', 'Other'],
                                                          dtype='S')

    # ------------------------------------------------------------------------------------------------------------------
    # new_car_registrations --------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------

    # create group
    new_car_registrations_group = hdf5_file.create_group('new_car_registrations')

    # create dataset
    new_car_registrations_dataset = new_car_registrations_group.create_dataset('new_car_registrations_data',
                                                                               shape=(0, 8), data=[],
                                                                               maxshape=(None, 8), dtype='uint32')

    # assign attributes
    new_car_registrations_dataset.attrs['columns'] = np.array(['Year', 'Month', 'Gasoline', 'Diesel',
                                                               'LPG+CNG', 'BEV', 'Hybrid', 'Other'],
                                                              dtype='S')

    # ------------------------------------------------------------------------------------------------------------------

    hdf5_file.close()
