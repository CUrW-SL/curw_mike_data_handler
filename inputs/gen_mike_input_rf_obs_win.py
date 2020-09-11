import csv
from datetime import datetime, timedelta
import traceback
import json
import os
import sys
import getopt
import pandas as pd
import numpy as np
import operator
import collections
from math import acos, cos, sin, radians

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params
from db_adapter.base import get_Pool, destroy_Pool

from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.curw_sim.common import extract_obs_rain_5_min_ts
from db_adapter.constants import COMMON_DATE_TIME_FORMAT

# ROOT_DIRECTORY = '/home/shadhini/dev/repos/curw-sl/curw_mike_data_handlers'
ROOT_DIRECTORY = 'C:\curw\curw_mike_data_handler'


def read_csv(file_name):
    """
    Read csv file
    :param file_name: <file_path/file_name>.csv
    :return: list of lists which contains each row of the csv file
    """

    with open(file_name, 'r') as f:
        data = [list(line) for line in csv.reader(f)][1:]

    return data


def write_to_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))


def append_to_file(file_name, data):
    with open(file_name, 'a+') as f:
        f.write('\n'.join(data))


def append_file_to_file(file_name, file_content):
    with open(file_name, 'a+') as f:
        f.write('\n')
        f.write(file_content)


def makedir_if_not_exist_given_filepath(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            pass


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


def replace_nan_with_row_average(df):
    m = df.mean(axis=1)
    for i, col in enumerate(df):
        df.iloc[:, i] = df.iloc[:, i].fillna(m)
    return df


def prepare_mike_rf_input(start, end, step):

    try:
        mike_obs_stations = read_csv(os.path.join(ROOT_DIRECTORY, 'inputs', 'params', 'mike_rainfall_stations.csv'))
        # [hash_id,station_id,station_name,latitude,longitude]

        station_dict = {}
        for i in range(len(mike_obs_stations)):
            # { station_id: [station_hash_id, station_name]
            station_dict[mike_obs_stations[i][1]] = [mike_obs_stations[i][0], mike_obs_stations[i][2]]

        ts_df = pd.DataFrame()
        ts_df['time'] = pd.date_range(start=start, end=end, freq='5min')

        obs_pool = get_Pool(host=con_params.CURW_OBS_HOST, port=con_params.CURW_OBS_PORT, user=con_params.CURW_OBS_USERNAME,
                        password=con_params.CURW_OBS_PASSWORD,
                        db=con_params.CURW_OBS_DATABASE)

        connection = obs_pool.connection()

        for obs_id in station_dict.keys():

            ts = extract_obs_rain_5_min_ts(connection=connection, id=station_dict.get(obs_id)[0],
                                           start_time=start, end_time=end)
            ts.insert(0, ['time', obs_id])
            df = list_of_lists_to_df_first_row_as_columns(ts)
            df[obs_id] = df[obs_id].astype('float64')

            ts_df = pd.merge(ts_df, df, how="left", on='time')

        ts_df.set_index('time', inplace=True)
        ts_df = ts_df.resample('{}min'.format(step), label='right', closed='right').sum()

        mike_input = replace_negative_numbers_with_nan(ts_df)

        mike_input = mike_input.fillna('')
        mike_input = mike_input.round(1)

        for col in mike_input.columns:
            mike_input = mike_input.rename(columns={col: station_dict.get(col)[1]})

        # pd.set_option('display.max_rows', mike_input.shape[0]+1)
        # pd.set_option('display.max_columns', mike_input.shape[1]+1)
        # print(mike_input)
        return mike_input

    except Exception:
        traceback.print_exc()
    finally:
        connection.close()
        destroy_Pool(obs_pool)


def usage():
    usageText = """
    Usage: .\inputs\gen_mike_input_rf_obs_win.py [-s "YYYY-MM-DD HH:MM:SS"] [-e "YYYY-MM-DD HH:MM:SS"] [-t XXX]

    -h  --help          Show usage
    -s  --start_time    Mike rainfall timeseries start time (e.g: "2019-06-05 00:00:00"). Default is 00:00:00, 3 days before today.
    -e  --end_time      Mike rainfall timeseries end time (e.g: "2019-06-05 23:00:00"). Default is 00:00:00, 2 days after.
    -t  --step          Time step in minutes (time difference between 2 consecutive readings)(e.g: 10, 30, 240). 
                        Default is 15 (15 mins).
    """
    print(usageText)


if __name__ == "__main__":

    os.system("cd {}".format(ROOT_DIRECTORY))

    set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config.json'))

    try:

        start_time = None
        end_time = None
        time_step = None

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:s:e:t:",
                                       ["help", "start_time=", "end_time=", "step="])
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
            elif opt in ("-t", "--step"):
                time_step = arg.strip()

        # Load config params
        config = json.loads(open(os.path.join('inputs', 'configs', 'rain_config_ws.json')).read())
        # config = json.loads(open('rain_config.json').read())

        output_dir = read_attribute_from_config_file('output_dir', config)
        file_name = read_attribute_from_config_file('output_file_name', config)

        if start_time is None:
            start_time = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=start_time)

        if end_time is None:
            end_time = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d 00:00:00')
        else:
            check_time_format(time=end_time)

        if output_dir is None:
            output_dir = os.path.join(ROOT_DIRECTORY, "data")
        if file_name is None:
            file_name = 'mike_rf_obs_{}_{}_{}.txt'.format(start_time, end_time, time_step).replace(" ", "_").replace(":", "-")

        mike_rf_file_path = os.path.join(output_dir, file_name)

        if not os.path.isfile(mike_rf_file_path):
            makedir_if_not_exist_given_filepath(mike_rf_file_path)
            print("{} start preparing mike rainfall input".format(datetime.now()))
            mike_rainfall = prepare_mike_rf_input(start=start_time, end=end_time, step=time_step)
            mike_rainfall.to_csv(mike_rf_file_path, header=True, index=True)
            print("{} completed preparing mike rainfall input".format(datetime.now()))
            print("Mike input rainfall file is available at {}".format(mike_rf_file_path))
        else:
            print('Mike rainfall input file already in path : ', mike_rf_file_path)

    except Exception:
        traceback.print_exc()

