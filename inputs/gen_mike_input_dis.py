#########!/home/uwcc-admin/curw_mike_data_handler/venv/bin/python3

####!"D:\curw_mike_data_handlers\venv\Scripts\python.exe"
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

from db_adapter.base import get_Pool, destroy_Pool

from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.curw_sim.timeseries.discharge import Timeseries
from db_adapter.constants import COMMON_DATE_TIME_FORMAT


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


def create_dir_if_not_exists(path):
    """
    create directory(if needed recursively) or paths
    :param path: string : directory path
    :return: string
    """
    if not os.path.exists(path):
        os.makedirs(path)

    return path


def list_of_lists_to_df_first_row_as_columns(data):
    """

    :param data: data in list of lists format
    :return: equivalent pandas dataframe
    """

    return pd.DataFrame.from_records(data[1:], columns=data[0])


def replace_negative_numbers_with_nan(df):
    num = df._get_numeric_data()
    num[num < 0] = np.nan
    return df


def prepare_mike_dis_input(start, end, dis_id):

    try:

        dis_ts_df = pd.DataFrame()
        dis_ts_df['time'] = pd.date_range(start=start, end=end, freq='15min')

        pool = get_Pool(host=CURW_SIM_HOST, port=CURW_SIM_PORT, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                        db=CURW_SIM_DATABASE)
        TS = Timeseries(pool)

        ts = TS.get_timeseries(id_=dis_id, start_date=start, end_date=end)
        ts.insert(0, ['time', 'value'])
        ts_df = list_of_lists_to_df_first_row_as_columns(ts)

        print(ts_df)

        dis_ts_df = pd.merge(dis_ts_df, ts_df, how="left", on='time')

        dis_ts_df.set_index('time', inplace=True)

        dis_ts_df.fillna(method='ffill').fillna(method='bfill')

        pd.set_option('display.max_rows', dis_ts_df.shape[0]+1)
        pd.set_option('display.max_columns', dis_ts_df.shape[1]+1)
        print(dis_ts_df)

        return dis_ts_df

    except Exception:
        traceback.print_exc()
    finally:
        destroy_Pool(pool)


def usage():
    usageText = """
    Usage: .\gen_mike_input_rf.py [-s "YYYY-MM-DD HH:MM:SS"] [-e "YYYY-MM-DD HH:MM:SS"]

    -h  --help          Show usage
    -s  --start_time    Mike rainfall timeseries start time (e.g: "2019-06-05 00:00:00"). Default is 00:00:00, 3 days before today.
    -e  --end_time      Mike rainfall timeseries end time (e.g: "2019-06-05 23:00:00"). Default is 00:00:00, 2 days after.
    """
    print(usageText)


if __name__ == "__main__":

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
        config = json.loads(open(os.path.join('inputs', 'dis_config.json')).read())

        output_dir = read_attribute_from_config_file('output_dir', config)
        file_name = read_attribute_from_config_file('output_file_name', config)

        dis_id = read_attribute_from_config_file('dis_id', config)

        if start_time is None:
            start_time = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=start_time)

        if end_time is None:
            end_time = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=end_time)

        if output_dir is None:
            output_dir = os.getcwd()
        if file_name is None:
            file_name = 'mike_dis_{}_{}.txt'.format(start_time, end_time).replace(' ', '_').replace(':', '-')

        mike_dis_file_path = os.path.join(output_dir, file_name)

        if not os.path.isfile(mike_dis_file_path):
            print("{} start preparing mike rainfall input".format(datetime.now()))
            mike_discharge = prepare_mike_dis_input(start=start_time, end=end_time, dis_id=dis_id)
            mike_discharge.to_csv(mike_dis_file_path, header=False, index=True)
            print("{} completed preparing mike rainfall input".format(datetime.now()))
            print("Mike input rainfall file is available at {}".format(mike_dis_file_path))
        else:
            print('Mike rainfall input file already in path : ', mike_dis_file_path)

    except Exception:
        traceback.print_exc()
