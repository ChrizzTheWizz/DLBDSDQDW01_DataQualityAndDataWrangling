from utils.crawl_setup import *
from utils.crawl_initialization import *

from utils.crawl_data_preprocessing import *
from utils.hdf5_file_input import *
from utils.hdf5_file_output import *

from utils.crawl_data_extraction import *

import os
from pathlib import Path

# determine script directory for scheduled execution and change working directory
script_directory = Path(__file__).parent.absolute()
os.chdir(script_directory)

# read config file (utils.setup)
config = read_config_file()

# determine current datetime
current_datetime = datetime.now()

# read list of essential files
main_files = config['main_files']
# check if main files do exist
status_main_files = check_data_files(main_files)

# assign hdf5-filepath to variable
h5_file = main_files['hdf5_file']

# read subjects and according logging / data paths
logging_paths = config['logging_paths']
status_logging_directories = check_directories(logging_paths)

data_paths = config['data_paths']
status_data_directories = check_directories(data_paths)

# create logger dictionary for easies access
logger = {}
status_logging_files = logging_paths.copy()

# assign logger to dictionary for every subject in config file
for subject, path in logging_paths.items():
    logger[subject], file = initialize_logger(subject, path, current_datetime)
    subject_date = determine_subject_date(subject, current_datetime)
    status_logging_files[subject] = check_status(subject, file, subject_date)

# logging status after initialization for checked areas
general_log_initialization(logger['general'], 'main_files', status_main_files)
general_log_initialization(logger['general'], 'directories', status_data_directories)
general_log_initialization(logger['general'], 'directories', status_logging_directories)
general_log_initialization(logger['general'], 'logging_files', status_logging_files)

# in case HDF5 File does not already exist
if not status_main_files['hdf5_file']:
    # logging to general log file
    logger['general'].info('Subject H5-File: Process started for creating file')
    logger['general'].info('Subject H5-File: Reading config data')

    # read API url for extracting air quality stations data
    url = config['url']['air_quality_stations']

    logger['general'].info(f'Subject H5-File: Extracting air quality stations data for groups with url {url}')

    # extract air quality stations data
    air_quality_stations, status_code_crawl = extract_air_quality_stations(url)

    # log if crawling was successful or not
    if status_code_crawl == 200:
        logger['general'].info(f'Subject H5-File: Successfully extracted air quality stations data')
    else:
        logger['general'].error('ERROR API crawling for air quality stations data')
        logger['general'].error('Subject H5-File: Process ended with ERROR --> see log file')
        raise HDF5PreconditionError("Unable to extract air quality stations")

    # read API url for extracting traffic sensors data
    url = config['url']['traffic_sensors']

    logger['general'].info(f'Subject H5-File: Extracting traffic sensors data for datasets with url {url}')

    # extract traffic sensors data
    traffic_sensors = extract_traffic_sensors(url)

    # log if crawling was successful or not
    if status_code_crawl == 200:
        logger['general'].info(f'Subject H5-File: Successfully extracted traffic sensors data')
        logger['general'].info(f'Subject H5-File: Initializing H5-File')
    else:
        logger['general'].error('ERROR API crawling for air quality stations data')
        logger['general'].error('Subject H5-File: Process ended with ERROR --> see log file')
        raise HDF5PreconditionError("Unable to extract traffic sensors")

    # initialize HDF5 File
    initialize_h5_file(h5_file, air_quality_stations, traffic_sensors)
    logger['general'].info('Subject H5-File: Successfully initialized H5-File')
    logger['general'].info('Subject H5-File: Process COMPLETED for creating file')

for subject, status in status_logging_files.items():
    # AIR QUALITY ######################################################################################################
    if subject == 'air_quality' and status == 'OPEN':
        # determine subject date
        api_date = determine_subject_date(subject, current_datetime)

        logger['general'].info(f'Subject {subject}: Process started for {api_date}')
        logger['general'].info(f'Subject {subject}: Logging to {logging_paths[subject]})')

        logger[subject].info(f'Starting API crawling for {api_date}')

        # read api url
        logger[subject].info('Reading config data')
        url = config['url']['air_quality_data']

        # first read stations for API-request
        logger[subject].info('Reading stations data from H5-File')
        df_stations = read_air_quality_stations(h5_file, subject)

        if not df_stations.empty:
            logger[subject].info('Successfully read stations data from H5-File')
        else:
            logger[subject].error(f'ERROR occurred loading stations data from H5-File')
            logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
            continue

        # create empty data container
        data_air_quality = []

        # extract data via api interface
        logger[subject].info(f'Crawling data for {api_date}')

        for station_code in df_stations.index:
            # formatting url in combination with api date
            url_station = url.format(station_code, api_date, api_date)
            logger[subject].info(f'{station_code}: Crawling data with url {url_station}')

            # extracting station data
            station_data, status_code_crawl = extract_station_data(url_station)

            if station_data is not None:
                logger[subject].info(f'{station_code}: Validating data')

                # validating extracted data
                missing_values = check_air_quality_station_data(station_data)
                if missing_values == 0:
                    data_air_quality.extend(station_data)
                    logger[subject].info(f'{station_code}: Data complete')
                else:
                    logger[subject].error(f'{station_code}: Data incomplete - {missing_values} values missing')
                    logger[subject].error(f'ERROR API crawling for {api_date}')
                    logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
                    continue

            else:
                logger[subject].error(f'ERROR occurred crawling data from H5-File')
                logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
                continue

        # add data to HDF5 File
        logger[subject].info('Saving collected data to H5-File')
        add_air_quality_data(h5_file, data_air_quality)
        logger[subject].info('Successfully saved data to H5-File')

        logger[subject].info(f'Process COMPLETED for {api_date}')
        logger['general'].info(f'Subject {subject}: Process COMPLETED for {api_date}')

    # TRAFFIC ##########################################################################################################
    if subject == 'traffic' and status == 'OPEN':
        # determine subject date
        api_date = determine_subject_date(subject, current_datetime)

        logger['general'].info(f'Subject {subject}: Process started for {api_date}')
        logger['general'].info(f'Subject {subject}: Logging to {logging_paths[subject]})')

        logger[subject].info(f'Starting API crawling for {api_date}')

        # read api url
        logger[subject].info('Reading config data')
        url_component = config['url']['traffic_data']

        # first read stations for API-request
        logger[subject].info('Reading sensor data from H5-File')
        df_sensors = read_traffic_sensors(h5_file, subject)

        if not df_sensors.empty:
            logger[subject].info('Successfully read stations data from H5-File')
        else:
            logger[subject].error(f'ERROR occurred loading stations data from H5-File')
            logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
            continue

        for name, row in df_sensors.iterrows():
            logger[subject].info(f'Crawling data for {api_date}')

            # formatting url in combination with api date
            url_sensor = row['observation_url'] + url_component + api_date
            logger[subject].info(f'{name}: Crawling data with url {url_sensor}')

            # extracting sensor data
            sensor_data, status_code_crawl = extract_traffic_data(url_sensor)

            if status_code_crawl == 200:
                logger[subject].info(f'{name}: Successfully crawled data')

                if not sensor_data.empty:
                    logger[subject].info(f'{name}: Preprocessing crawled data')

                    # extract needed data from sensor data
                    timestamp, result = preprocess_traffic_data(sensor_data)
                    logger[subject].info(f'{name}: Saving data to H5-File')

                    # add data to HDF5 File
                    add_traffic_data(h5_file, name, timestamp, result)
                else:
                    logger[subject].error(f'{name}: No data found')
            else:
                logger[subject].error(f'{name}: ERROR crawling data from {url_sensor}')
                logger['general'].error(f'{subject}: ERROR occurred crawling data for {name}')
                continue

        logger[subject].info('Successfully saved data to H5-File')

        logger[subject].info(f'Process COMPLETED for {api_date}')
        logger['general'].info(f'Subject {subject}: Process COMPLETED for {api_date}')

    # CAR REGISTRATIONS ################################################################################################
    elif subject == 'car_registrations' and status == 'OPEN':
        # determine subject date
        download_date = determine_subject_date(subject, current_datetime)

        logger['general'].info(f'Subject {subject}: Process started for {download_date}')
        logger['general'].info(f'Subject {subject}: Logging to {logging_paths[subject]})')

        # reading config data
        logger[subject].info('Reading config data')
        url = config['url']['car_registrations']
        path = config['data_file_names']['car_registrations']

        # determining url and path for download url and filename
        logger[subject].info('Determining url and path for download url and filename')
        path = path.format(download_date)
        path = Path(path)

        # extract file URL from website
        file_url = extract_file_url(url)
        check_string = 'fz1_' + download_date

        if check_string not in file_url:
            logger[subject].error(f'ERROR occurred: File not available yet')
            logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
            continue

        logger[subject].info('Downloading data')

        # download data from file URL
        status_code_download = data_download(file_url, path)

        if status_code_download == 200:
            logger[subject].info('Successfully downloaded data')
        else:
            logger[subject].error(f'ERROR occurred: {status_code_download}')
            logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
            continue

        # read downloaded file
        data_car_regs = read_car_regs(path)

        # add download date (year) to dataframe
        data_car_regs = preprocess_car_regs(data_car_regs, download_date)

        if data_car_regs is not None:
            logger[subject].info('Start saving data to H5-File')

            # add data to HDF5 File
            add_car_regs_data(h5_file, data_car_regs)
            logger[subject].info('Successfully saved data to H5-File')
        else:
            logger[subject].error(f'ERROR occurred: No data found')
            logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
            continue

        logger[subject].info(f'Process COMPLETED for {download_date}')
        logger['general'].info(f'Subject {subject}: Process COMPLETED for {download_date}')

    # NEW CAR REGISTRATION #############################################################################################
    elif subject == 'new_car_registrations' and status == 'OPEN':
        # determine subject date
        download_date = determine_subject_date(subject, current_datetime)

        logger['general'].info(f'Subject {subject}: Process started for {download_date}')
        logger['general'].info(f'Subject {subject}: Logging to {logging_paths[subject]})')

        # reading config data
        logger[subject].info('Reading config data')
        url = config['url']['new_car_registrations']
        path = config['data_file_names']['new_car_registrations']

        # determining url and path for download url and filename
        logger[subject].info('Determining url and path for download url and filename')
        path = path.format(download_date)
        path = Path(path)

        # extract file URL from website
        file_url = extract_file_url(url)
        check_string = 'fz8_' + download_date

        if check_string not in file_url:
            logger[subject].error(f'ERROR occurred: File not available yet')
            logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
            continue

        # download data from file URL
        logger[subject].info('Downloading data')
        status_code_download = data_download(file_url, path)

        if status_code_download == 200:
            logger[subject].info('Successfully downloaded data')
        else:
            logger[subject].error(f'ERROR occurred: {status_code_download}')
            logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
            continue

        # read downloaded file
        data_new_car_regs = read_new_car_regs(path)

        # add download date (year) to dataframe
        data_new_car_regs = preprocess_new_car_regs(data_new_car_regs, download_date)

        if data_new_car_regs is not None:
            logger[subject].info('Start saving data to H5-File')

            # add data to HDF5 File
            add_new_car_regs_data(h5_file, data_new_car_regs)
            logger[subject].info('Successfully saved data to H5-File')
        else:
            logger[subject].error(f'ERROR occurred: No data found')
            logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
            continue

        logger[subject].info(f'Process COMPLETED for {download_date}')
        logger['general'].info(f'Subject {subject}: Process COMPLETED for {download_date}')

    # WEATHER ##########################################################################################################
    elif subject == 'weather' and status == 'OPEN':
        # determine subject date
        crawl_date = determine_subject_date(subject, current_datetime)

        logger['general'].info(f'Subject {subject}: Process started for {crawl_date}')
        logger['general'].info(f'Subject {subject}: Logging to {logging_paths[subject]})')

        logger[subject].info(f'Starting crawling for {crawl_date}')

        # reading config data
        logger[subject].info('Reading config data')
        url = config['url']['weather_data']

        logger[subject].info(f'Crawling data with url {url}')

        # extract data from website
        temperature, precipitation, wind_speed, status_code_crawl = extract_weather_data(url)

        if status_code_crawl == 200:
            logger['weather'].info('Successfully crawled weather data for Berlin')
        else:
            logger[subject].error(f'ERROR occurred crawling weather data from {url}')
            logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
            continue

        logger[subject].info('Start saving data to H5-File')

        # add data to HDF5 File
        add_weather_data(h5_file, temperature, precipitation, wind_speed)
        logger[subject].info('Successfully saved data to H5-File')

        logger[subject].info(f'Process COMPLETED for {crawl_date}')
        logger['general'].info(f'Subject {subject}: Process COMPLETED for {crawl_date}')

    # CONSTRUCTIONS ####################################################################################################
    elif subject == 'constructions' and status == 'OPEN':
        # determine subject date
        download_date = determine_subject_date(subject, current_datetime)

        logger['general'].info(f'Subject {subject}: Process started for {download_date}')
        logger['general'].info(f'Subject {subject}: Logging to {logging_paths[subject]})')

        logger[subject].info(f'Starting crawling for {download_date}')

        # read config data
        logger[subject].info('Reading config data')
        url = config['url']['constructions']
        path = Path(config['data_file_names']['constructions'])

        # read previous constructions data if available (otherwise None)
        logger[subject].info('Reading previous data')
        prev_data_constructions = read_constructions(path)
        if prev_data_constructions is not None:
            logger[subject].info('Successfully loaded previous data')
        else:
            logger[subject].info('No previous data available')

        # download new constructions data
        logger[subject].info(f'Downloading data with url {url}')
        status_code_download = data_download(url, path)
        if status_code_download == 200:
            logger[subject].info('Successfully downloaded data')
        else:
            logger[subject].error(f'ERROR occurred: {status_code_download}')
            logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
            continue

        # preprocess constructions data (data comparison)
        logger[subject].info('Preprocessing data')
        data_constructions = preprocess_constructions(path, prev_data_constructions)
        logger[subject].info('Successfully preprocessed data')

        # add preprocessed data to HDF5 File
        if not data_constructions.empty:
            logger[subject].info('Saving preprocessed data to JSON')
            path_pre = Path(config['data_file_names']['pre_constructions'])
            data_constructions.to_json(path_pre)

            logger[subject].info('Saving data to H5-File')

            # add data to HDF5 File
            status_add_data = add_construction_data(h5_file, data_constructions)
            if status_add_data:
                logger[subject].info(f'Successfully saved ({len(data_constructions)} new or updated) data to H5-File')
            else:
                logger[subject].error('ERROR occurred while adding data to H5-File')
                logger['general'].error(f'Subject {subject}: Process ended with ERROR --> see log file')
                continue
        else:
            logger[subject].info('No new data to add to H5-File')

        logger[subject].info(f'Process COMPLETED for {download_date}')
        logger['general'].info(f'Subject {subject}: Process COMPLETED for {download_date}')
