#!/home/uwcc-admin/curw_mike_data_handler/venv/bin/python3
"only she bang, root dir, output dir and filename are different from generic one"

import pymysql
from datetime import datetime, timedelta
import traceback
import json
import os
import sys
import getopt
import pandas as pd
import numpy as np

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params
from db_adapter.base import get_Pool, destroy_Pool

from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.curw_sim.timeseries.tide import Timeseries
from db_adapter.constants import COMMON_DATE_TIME_FORMAT

ROOT_DIRECTORY = '/home/uwcc-admin/curw_mike_data_handler'
# ROOT_DIRECTORY = 'D:\curw_mike_data_handlers'
OUTPUT_DIRECTORY = "/mnt/disks/curwsl_nfs/mike/inputs"


def read_attribute_from_config_file(attribute, config, compulsory=False):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :param compulsory: Boolean value: whether the attribute is must present or not in the config file
    :return:

    """
    if attribute in config and (config[attribute] != ""):
        return config[attribute]
    elif compulsory:
        print("{} not specified in config file.".format(attribute))
        exit(1)
    else:
        # print("{} not specified in config file.".format(attribute))
        return None


def check_time_format(time):
    try:
        time = datetime.strptime(time, DATE_TIME_FORMAT)

        if time.strftime('%S') != '00':
            print("Seconds should be always 00")
            exit(1)
        if time.strftime('%M') not in ('00', '15', '30', '45'):
            print("Minutes should be always multiple of 15")
            exit(1)

        return True
    except Exception:
        print("Time {} is not in proper format".format(time))
        exit(1)


def makedir_if_not_exist_given_filepath(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            pass


def list_of_lists_to_df_first_row_as_columns(data):
    """

    :param data: data in list of lists format
    :return: equivalent pandas dataframe
    """

    return pd.DataFrame.from_records(data[1:], columns=data[0])


def replace_negative_99999_with_nan(df):
    num = df._get_numeric_data()
    print(num)
    num[num == -99999] = np.nan
    return df


def prepare_mike_dis_input(start, end, tide_id):

    try:

        pool = get_Pool(host=con_params.CURW_SIM_HOST, port=con_params.CURW_SIM_PORT, user=con_params.CURW_SIM_USERNAME,
                        password=con_params.CURW_SIM_PASSWORD,
                        db=con_params.CURW_SIM_DATABASE)
        TS = Timeseries(pool)
        ts = TS.get_timeseries(id_=tide_id, start_date=start, end_date=end)
        ts.insert(0, ['time', 'value'])
        ts_df = list_of_lists_to_df_first_row_as_columns(ts)
        ts_df['value'] = ts_df['value'].astype('float64')

        tide_ts_df = pd.DataFrame()
        tide_ts_df['time'] = pd.date_range(start=start, end=end, freq='15min')

        tide_ts_df = pd.merge(tide_ts_df, ts_df, how="left", on='time')

        tide_ts_df.set_index('time', inplace=True)

        tide_ts_df = replace_negative_99999_with_nan(tide_ts_df)

        if tide_ts_df.iloc[-1, 0] is np.nan:
            tide_ts_df.iloc[-1, 0] = 0

        tide_ts_df = ts_df.dropna()

        return tide_ts_df

    except Exception:
        traceback.print_exc()
    finally:
        destroy_Pool(pool)


def usage():
    usageText = """
    Usage: .\gen_mike_input_tide_linux.py [-s "YYYY-MM-DD HH:MM:SS"] [-e "YYYY-MM-DD HH:MM:SS"]

    -h  --help          Show usage
    -s  --start_time    Mike rainfall timeseries start time (e.g: "2019-06-05 00:00:00"). Default is 00:00:00, 3 days before today.
    -e  --end_time      Mike rainfall timeseries end time (e.g: "2019-06-05 23:00:00"). Default is 00:00:00, 2 days after.
    """
    print(usageText)


if __name__ == "__main__":

    set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config.json'))

    try:

        start_time = None
        end_time = None

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:s:e:",
                                       ["help", "start_time=", "end_time="])
        except getopt.GetoptError:
            usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-s", "--start_time"):
                start_time = arg.strip()
            elif opt in ("-e", "--end_time"):
                end_time = arg.strip()

        # Load config params
        config = json.loads(open(os.path.join('inputs', 'tide_config.json')).read())

        output_dir = read_attribute_from_config_file('output_dir', config)
        file_name = read_attribute_from_config_file('output_file_name', config)

        tide_id = read_attribute_from_config_file('tide_id', config)

        if start_time is None:
            start_time = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=start_time)

        if end_time is None:
            end_time = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=end_time)

        if output_dir is None:
            output_dir = os.path.join(OUTPUT_DIRECTORY, (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d_%H-00-00'))
        if file_name is None:
            file_name = 'mike_tide.txt'

        mike_tide_file_path = os.path.join(output_dir, file_name)

        if not os.path.isfile(mike_tide_file_path):
            makedir_if_not_exist_given_filepath(mike_tide_file_path)
            print("{} start preparing mike rainfall input".format(datetime.now()))
            mike_discharge = prepare_mike_dis_input(start=start_time, end=end_time, tide_id=tide_id)
            mike_discharge.to_csv(mike_tide_file_path, header=False, index=True)
            print("{} completed preparing mike rainfall input".format(datetime.now()))
            print("Mike input rainfall file is available at {}".format(mike_tide_file_path))
        else:
            print('Mike rainfall input file already in path : ', mike_tide_file_path)

    except Exception:
        traceback.print_exc()

