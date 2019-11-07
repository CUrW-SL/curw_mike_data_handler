#!/home/uwcc-admin/curw_mike_data_handler/venv/bin/python3

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
from db_adapter.curw_sim.timeseries import Timeseries
from db_adapter.constants import COMMON_DATE_TIME_FORMAT


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
        print("{} not specified in config file.".format(attribute))
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

    distinct_obs_ids = coefficients.curw_obs_id.unique()
    hybrid_ts_df = pd.DataFrame()
    hybrid_ts_df_initialized = False

    try:
        pool = get_Pool(host=CURW_SIM_HOST, port=CURW_SIM_PORT, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                        db=CURW_SIM_DATABASE)
        TS = Timeseries(pool)

        obs_id_hash_id_mapping = get_all_obs_rain_hashids_from_curw_sim(pool)

        for obs_id in distinct_obs_ids:
            print(obs_id)
            ts = TS.get_timeseries(id_=obs_id_hash_id_mapping.get(str(obs_id)), start_date=start, end_date=end)
            ts.insert(0, ['time', obs_id])
            ts_df = list_of_lists_to_df_first_row_as_columns(ts)

            if not hybrid_ts_df_initialized:
                hybrid_ts_df = ts_df
                hybrid_ts_df_initialized = True
            else:
                hybrid_ts_df = pd.merge(hybrid_ts_df, ts_df, how="outer", on='time')

        pd.set_option('display.max_rows', hybrid_ts_df.shape[0]+1)
        print(hybrid_ts_df)
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

        print("started creating rainfall input for mike")
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
        config = json.loads(open('inputs/rain_config.json').read())

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

        coefficients = pd.read_csv('inputs/params/sb_rf_coefficients.csv', delimiter=',')

        prepare_mike_rf_input(start=start_time, end=end_time, coefficients=coefficients)

        # if output_dir is not None and file_name is not None:
        #     mike_rf_file_path = os.path.join(output_dir, file_name)
        # else:
        #     mike_rf_file_path = os.path.join(r"D:\curw_mike_data_handlers",
        #                                   'mike_rf_{}_{}.DAT'.format(start_time, end_time).replace(' ', '_').replace(':', '-'))
        #
        # if not os.path.isfile(mike_rf_file_path):
        #     print("{} start preparing mike rainfall input".format(datetime.now()))
        #
        #     print("{} completed preparing mike rainfall input".format(datetime.now()))
        # else:
        #     print('Mile rainfall input file already in path : ', mike_rf_file_path)

    except Exception:
        traceback.print_exc()

