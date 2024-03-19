import logging
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def determine_subject_date(subject, date):
    """
    Determines necessary api date (-time) based on subject

    :param subject: subject
    :param date: current datetime
    :return: subject date as string
    """

    if subject == 'air_quality':
        # for air quality api date is last day
        subject_date = date - timedelta(days=1)
        subject_date = subject_date.strftime('%Y-%m-%d')
    elif subject == 'car_registrations':
        # for car registrations api date is last year
        subject_date = date - relativedelta(years=1)
        subject_date = subject_date.strftime("%Y")
    elif subject == 'new_car_registrations':
        # for new car registrations api date is penultimate month
        subject_date = date - relativedelta(months=1)
        subject_date = subject_date.strftime("%Y%m")
    elif subject == 'weather':
        # for weather no need of api date - log date is current date + time
        subject_date = date.strftime('%Y-%m-%d_%H:%M')
    elif subject == 'traffic':
        # for traffic api date is current datetime rounded minus 3 and 2 hours
        if date.minute >= 30:
            date += timedelta(hours=1)
        subject_date_raw = date.replace(minute=0, second=0, microsecond=0)

        subject_date_begin = subject_date_raw - timedelta(hours=3)
        subject_date_begin = subject_date_begin.strftime("%Y-%m-%dT%H:%M:%SZ")

        subject_date_end = subject_date_raw - timedelta(hours=2)
        subject_date_end = subject_date_end.strftime("%Y-%m-%dT%H:%M:%SZ")

        subject_date = f'{subject_date_begin}/{subject_date_end}'

    else:  # subject in ['constructions', 'general']:
        # for other subjects (constructions and general (logging)) api time is current date
        subject_date = date.strftime('%Y-%m-%d')

    return subject_date


def initialize_logger(subject, base_path, current_date):
    """
    Initializes logger for every subject.

    :param subject: Subject
    :param base_path: Directory, where log-file is written to
    :param current_date: current datetime
    :return: Logger and complete log-file-path
    """

    # file name for log file differs for (new) car registrations
    if subject == 'car_registrations':
        log_date = current_date.strftime('%Y')
    elif subject == 'new_car_registrations':
        log_date = current_date.strftime('%Y-%m')
    else:
        log_date = current_date.strftime('%Y-%m-%d')

    # complete file path
    log_file = Path(base_path) / f'{subject}_{log_date}.log'

    # logger configuration
    logger = logging.getLogger(subject)
    logger.setLevel(logging.INFO)

    # no duplicated handlers
    if not logger.hasHandlers():
        # logging-format
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # filehandler for logging to a file
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    return logger, log_file


def check_status(subject, path, subject_date):
    """
    Checks current status for subject either the task is open or completed

    :param subject: Subject
    :param path: Directory of subject's log-file
    :param subject_date: subject date as string
    :return: Result: 'OPEN' or 'COMPLETED
    """
    file = Path(path)

    if file.exists():
        with open(file, 'r') as logging_file:
            logging_data = logging_file.readlines()

        if logging_data:
            # only read last row
            last_log = logging_data[-1]

            # only read message part
            last_log_message = last_log.split(' - ')[-1].rstrip('\n')

            # extract process status (first 2 words)
            process_status = ' '.join(last_log_message.split()[:2])

            if process_status == 'Process COMPLETED':
                if subject == 'weather':
                    target_date = datetime.strptime(subject_date.split(' ')[-1], '%Y-%m-%d_%H:%M')
                    last_log_date = datetime.strptime(last_log_message.split(' ')[-1], '%Y-%m-%d_%H:%M')

                    if target_date - timedelta(minutes=10) <= last_log_date <= target_date + timedelta(minutes=10):
                        return 'COMPLETED'
                    else:
                        return 'OPEN'

                elif subject == 'traffic':
                    target_date = datetime.strptime(subject_date.split('Z/')[0], '%Y-%m-%dT%H:%M:%S')
                    last_log_date = last_log_message.split(' ')[-1]
                    last_log_date = last_log_date.split('Z/')[0]
                    last_log_date = datetime.strptime(last_log_date, '%Y-%m-%dT%H:%M:%S')

                    if target_date - timedelta(minutes=15) <= last_log_date <= target_date + timedelta(minutes=15):
                        return 'COMPLETED'
                    else:
                        return 'OPEN'

                else:
                    if last_log_message.split(' ')[-1] == subject_date:
                        return 'COMPLETED'
                    else:
                        return 'OPEN'
            else:
                return 'OPEN'

        else:
            return 'OPEN'

    else:
        raise FileNotFoundError


def general_log_initialization(logger, subject, status_data):
    """
    Logging the status after initialization

    :param logger: Logger for general logging
    :param subject: Subject of initialization
    :param status_data: Status of subject
    :return: None
    """
    for key, log_status in status_data.items():
        if subject == 'main_files':
            if log_status:
                logger.info(f'Subject {subject}: {key} available')
            else:
                logger.info(f'Subject {subject}: {key} missing')
        elif subject == 'directories':
            if log_status == 'Directory created':
                logger.info(f'Directory {log_status } for {key}')
            else:
                pass
        elif subject == 'logging_files':
            logger.info(f'Subject {key}: Process {log_status}')
