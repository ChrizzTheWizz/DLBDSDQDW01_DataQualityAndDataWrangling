from pathlib import Path
import h5py
import pandas as pd
import json


def read_air_quality_stations(path_h5, subject):
    """
    Reads existing metadata for air quality stations from HDF5 File

    :param path_h5: path to HDF5 File
    :param subject: subject (= air quality)
    :return: dataframe with air quality stations
    """
    file = Path(path_h5)
    h5_group = subject

    if file.exists():
        stations_info = {}

        with h5py.File(file, 'r') as hdf5_file:
            air_quality_group = hdf5_file[h5_group]

            # passing through all stations in the 'air_quality' group
            for station_code in air_quality_group:
                # access to the subgroup of the respective station
                station_group = air_quality_group[station_code]

                # collect attributes
                attributes = {attr: station_group.attrs[attr] for attr in station_group.attrs}

                # adding the collected attributes to the dictionary
                stations_info[station_code] = attributes

        # convert dictionary to dataframe
        df = pd.DataFrame.from_dict(stations_info, orient='index')

        # use code as index
        df.index.name = 'code'

        return df

    else:
        raise FileNotFoundError


def read_air_quality_data(path):
    """
    Reads air quality data from HDF5 File

    :param path: path to HDF5 File
    :return: dataframe with data from all air quality stations
    """
    file = Path(path)
    if file.exists():
        with h5py.File(file, 'r') as hdf5_file:
            air_quality = hdf5_file['air_quality']

            # list for storing the data
            all_data = []

            for group_name, subgroup in air_quality.items():
                for component, dataset in subgroup.items():
                    # read data from dataset
                    data = dataset[:]

                    # Create a temporary dataframe for the current dataset
                    df_station = pd.DataFrame(data, columns=['Timestamp', component])
                    df_station['Timestamp'] = pd.to_datetime(df_station['Timestamp'], unit='s')
                    df_station.set_index('Timestamp', inplace=True)

                    # adding the column MultiIndex structure (station, component)
                    df_station.columns = pd.MultiIndex.from_tuples([(group_name, component)])

                    # add data to list
                    all_data.append(df_station)

            # concat dataframe to existing and drop "empty" data
            df = pd.concat(all_data, axis=1)
            df = df.dropna(axis=1, how='all')

            return df
    else:
        raise FileNotFoundError


def read_air_quality_station_data(path, station):
    """
    Reads specific station data from HDF5 File

    :param path: path to HDF5 File
    :param station: station to be read
    :return: address and coordinates
    """
    file = Path(path)
    if file.exists():
        with h5py.File(file, 'r') as hdf5_file:
            station_group = hdf5_file['air_quality'][station]

            address = station_group.attrs['address']
            lat = station_group.attrs['lat']
            lng = station_group.attrs['lng']

            return address, float(lat), float(lng)
    else:
        raise FileNotFoundError


def read_traffic_sensors(path, subject):
    """
    Read available sensors from HDF5 File

    :param path: path to HDF5 File
    :param subject: subject (traffic)
    :return:
    """
    file = Path(path)
    h5_group = subject

    if file.exists():
        sensor_info_list = []

        with h5py.File(file, 'r') as hdf5_file:
            traffic_group = hdf5_file[h5_group]

            # running through all sensors in the group
            for sensor_name in traffic_group:
                # access to the dataset of the respective sensor
                sensor_dataset = traffic_group[sensor_name]

                # check whether the 'sensor_information' attribute exists
                if 'sensor_information' in sensor_dataset.attrs:
                    # extracting and converting the 'sensor_information' JSON string
                    sensor_info_json = sensor_dataset.attrs['sensor_information'].tobytes().decode('utf-8')
                    sensor_info = json.loads(sensor_info_json)

                    # adding the sensor name
                    sensor_info['name'] = sensor_name

                    # add the extracted information to the list
                    sensor_info_list.append(sensor_info)

        # convert list to dataframe
        df = pd.DataFrame(sensor_info_list)

        # set name as index
        df.set_index('name', inplace=True)

        return df

    else:
        raise FileNotFoundError


def read_traffic_data(path):
    """
    Reads traffic data from HDF5 file

    :param path: path to HDF5 file
    :return: dataframe with traffic data
    """
    file = Path(path)
    h5_group = 'traffic'

    if file.exists():
        df = pd.DataFrame(columns=['name', 'Timestamp', 'Traffic'])

        with h5py.File(file, 'r') as hdf5_file:
            traffic_group = hdf5_file[h5_group]

            # running through all sensors in the group
            for sensor_name in traffic_group:
                # access to the dataset of the respective sensor
                sensor_dataset = traffic_group[sensor_name]

                # read column names from attributes
                columns = [column.decode('utf-8') for column in sensor_dataset.attrs['columns']]

                # read data
                data = sensor_dataset[:]

                # assign data to temporary dataframe
                df_temp = pd.DataFrame(data, columns=columns)

                # conversion of the timestamp into a readable date
                df_temp['Timestamp'] = pd.to_datetime(df_temp['Timestamp'], unit='s')

                # add name to dataframe
                df_temp['name'] = sensor_name

                # add the extracted information to the list
                df = pd.concat([df, df_temp])

            df = df.set_index(['name', 'Timestamp'])

        return df

    else:
        raise FileNotFoundError


def read_weather_data(path):
    """
    Reads weather data from HDF5 File

    :param path: path to HDF5 file
    :return: dataframe with weather data
    """
    file = Path(path)
    if file.exists():
        with h5py.File(file, 'r') as hdf5_file:
            # determine dataset path
            dataset_path = 'weather/weather_data'

            if dataset_path in hdf5_file:
                # read data from dataset
                dataset = hdf5_file[dataset_path]
                data = dataset[:]

                # read column names from attributes
                columns = [column.decode('utf-8') for column in dataset.attrs['columns']]

                # create dataframe from data
                df = pd.DataFrame(data, columns=columns)

                # conversion of the timestamp into a readable date and set it as index
                df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')
                df.set_index('Timestamp', inplace=True)

                return df
            else:
                return pd.DataFrame()
    else:
        raise FileNotFoundError


def read_construction_data(path):
    """
    Reads construction data from HDF5 File

    :param path: path to HDF5 File
    :return: dataframe with constructions data
    """
    def reconvert_cells(cell):
        try:
            return json.loads(cell)
        except json.JSONDecodeError:
            return cell.decode('utf-8')

    file = Path(path)
    if file.exists():
        with h5py.File(file, 'r') as hdf5_file:
            dataset_path = 'constructions/construction_data'

            if dataset_path in hdf5_file:
                # read data
                dataset = hdf5_file[dataset_path]
                data = dataset[:]

                # read column names from attributes
                columns = [column.decode('utf-8') for column in dataset.attrs['columns']]
                df = pd.DataFrame(data, columns=columns)

                # applying the conversion function to each cell
                df = df.applymap(reconvert_cells)

                return df
            else:
                return pd.DataFrame()
    else:
        raise FileNotFoundError


def read_car_registration_data(path):
    """
    Reads car registrations data from HDF5 File

    :param path: path to HDF5 File
    :return: dataframe with car registrations data
    """
    file = Path(path)
    if file.exists():
        with h5py.File(file, 'r') as hdf5_file:
            # determine dataset path
            dataset_path = 'car_registrations/car_registrations_data'

            if dataset_path in hdf5_file:
                # read data
                dataset = hdf5_file[dataset_path]
                data = dataset[:]

                # read column names from attributes
                columns = [column.decode('utf-8') for column in dataset.attrs['columns']]

                # create dataframe from data
                df = pd.DataFrame(data, columns=columns)

                return df
            else:
                return pd.DataFrame()
    else:
        raise FileNotFoundError


def read_new_car_registration_data(path):
    """
    Reads new car registrations data from HDF5 File

    :param path: path to HDF5 File
    :return: dataframe with new car registrations data
    """
    file = Path(path)
    if file.exists():
        with h5py.File(file, 'r') as hdf5_file:
            # determine dataset path
            dataset_path = 'new_car_registrations/new_car_registrations_data'

            if dataset_path in hdf5_file:
                # read data
                dataset = hdf5_file[dataset_path]
                data = dataset[:]

                # read column names from attributes
                columns = [column.decode('utf-8') for column in dataset.attrs['columns']]

                # create dataframe from data
                df = pd.DataFrame(data, columns=columns)

                return df
            else:
                return pd.DataFrame()
    else:
        raise FileNotFoundError
