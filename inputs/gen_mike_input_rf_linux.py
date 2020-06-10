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
from db_adapter.curw_sim.timeseries import Timeseries
from db_adapter.constants import COMMON_DATE_TIME_FORMAT

ROOT_DIRECTORY = '/home/uwcc-admin/curw_mike_data_handler'
# ROOT_DIRECTORY = 'D:\curw_mike_data_handlers'
OUTPUT_DIRECTORY = "/mnt/disks/wrf_nfs/mike/inputs"


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


def get_all_obs_rain_hashids_from_curw_sim(pool):

    obs_id_hash_id_mappings = {}

    expected_earliest_obs_end = (datetime.now() - timedelta(days=1)).strftime(COMMON_DATE_TIME_FORMAT)

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "SELECT `id`, `grid_id` FROM `run` where `model`=%s and `obs_end`>=%s;"
            row_count = cursor.execute(sql_statement, ("hechms", expected_earliest_obs_end))
            if row_count > 0:
                results = cursor.fetchall()
                for dict in results:
                    grid_id = dict.get("grid_id")
                    grid_id_parts = grid_id.split("_")
                    obs_id_hash_id_mappings[grid_id_parts[1]] = dict.get("id")
                return obs_id_hash_id_mappings
            else:
                return None
    except Exception as exception:
        traceback.print_exc()
    finally:
        if connection is not None:
            connection.close()


def prepare_mike_rf_input(start, end, coefficients):

    try:

        #### process staton based hybrid timeseries ####
        distinct_obs_ids = coefficients.curw_obs_id.unique()
        hybrid_ts_df = pd.DataFrame()
        hybrid_ts_df['time'] = pd.date_range(start=start, end=end, freq='5min')

        pool = get_Pool(host=con_params.CURW_SIM_HOST, port=con_params.CURW_SIM_PORT, user=con_params.CURW_SIM_USERNAME,
                        password=con_params.CURW_SIM_PASSWORD,
                        db=con_params.CURW_SIM_DATABASE)

        TS = Timeseries(pool)

        obs_id_hash_id_mapping = get_all_obs_rain_hashids_from_curw_sim(pool)

        for obs_id in distinct_obs_ids:
            # taking data from curw_sim database (data prepared based on active stations for hechms)
            ts = TS.get_timeseries(id_=obs_id_hash_id_mapping.get(str(obs_id)), start_date=start, end_date=end)
            ts.insert(0, ['time', obs_id])
            ts_df = list_of_lists_to_df_first_row_as_columns(ts)
            ts_df[obs_id] = ts_df[obs_id].astype('float64')

            hybrid_ts_df = pd.merge(hybrid_ts_df, ts_df, how="left", on='time')

        hybrid_ts_df.set_index('time', inplace=True)
        hybrid_ts_df = hybrid_ts_df.resample('15min', label='right', closed='right').sum()

        # pd.set_option('display.max_rows', hybrid_ts_df.shape[0]+1)
        # pd.set_option('display.max_columns', hybrid_ts_df.shape[1]+1)
        # print(hybrid_ts_df)

        hybrid_ts_df = replace_negative_numbers_with_nan(hybrid_ts_df)

        # print(hybrid_ts_df)
        hybrid_ts_df = replace_nan_with_row_average(hybrid_ts_df)

        # print(hybrid_ts_df)

        #### process mike input ####

        distinct_names = coefficients.name.unique()
        mike_input = pd.DataFrame()
        mike_input_initialized = False

        for name in distinct_names:
            catchment_coefficients = coefficients[coefficients.name == name]
            # print(catchment_coefficients)
            catchment = pd.DataFrame()
            catchment_initialized = False
            for index, row in catchment_coefficients.iterrows():
                # print(index, row['curw_obs_id'], row['coefficient'])
                if not catchment_initialized:
                    catchment = (hybrid_ts_df[row['curw_obs_id']] * row['coefficient']).to_frame(name=row['curw_obs_id'])
                    catchment_initialized = True
                else:
                    new = (hybrid_ts_df[row['curw_obs_id']] * row['coefficient']).to_frame(name=row['curw_obs_id'])
                    catchment = pd.merge(catchment, new, how="left", on='time')

            if not mike_input_initialized:
                mike_input[name] = catchment.sum(axis=1)
                mike_input_initialized = True
            else:
                mike_input = pd.merge(mike_input, (catchment.sum(axis=1)).to_frame(name=name), how="left", on='time')

        mike_input.round(1)
        return mike_input

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
        config = json.loads(open(os.path.join('inputs', 'rain_config.json')).read())

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
            output_dir = os.path.join(OUTPUT_DIRECTORY, (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d_%H-00-00'))
        if file_name is None:
            file_name = 'mike_rf.txt'.format(start_time, end_time)

        mike_rf_file_path = os.path.join(output_dir, file_name)

        if not os.path.isfile(mike_rf_file_path):
            makedir_if_not_exist_given_filepath(mike_rf_file_path)
            print("{} start preparing mike rainfall input".format(datetime.now()))
            coefficients = pd.read_csv(os.path.join('inputs', 'params', 'sb_rf_coefficients.csv'), delimiter=',')
            mike_rainfall = prepare_mike_rf_input(start=start_time, end=end_time, coefficients=coefficients)
            mike_rainfall.to_csv(mike_rf_file_path, header=True, index=True)
            print("{} completed preparing mike rainfall input".format(datetime.now()))
            print("Mike input rainfall file is available at {}".format(mike_rf_file_path))
        else:
            print('Mike rainfall input file already in path : ', mike_rf_file_path)

    except Exception:
        traceback.print_exc()

