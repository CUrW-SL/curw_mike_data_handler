#!/home/uwcc-admin/curw_mike_data_handler/venv/bin/python3

import json
import traceback
import sys
import os
from datetime import datetime, timedelta
import re
import getopt
import pandas as pd

from db_adapter.logger import logger
from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.base import get_Pool
from db_adapter.curw_fcst.source import get_source_id, get_source_parameters
from db_adapter.curw_fcst.variable import get_variable_id
from db_adapter.curw_fcst.unit import get_unit_id, UnitType
from db_adapter.curw_fcst.station import get_mike_stations, StationEnum
from db_adapter.curw_fcst.timeseries import Timeseries
from db_adapter.curw_fcst.timeseries import insert_run_metadata

ROOT_DIRECTORY = '/home/uwcc-admin/curw_mike_data_handler'


def read_attribute_from_config_file(attribute, config, compulsory):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :param compulsory: Boolean value: whether the attribute is must present or not in the config file
    :return:
    """
    if attribute in config and (config[attribute]!=""):
        return config[attribute]
    elif compulsory:
        logger.error("{} not specified in config file.".format(attribute))
        exit(1)
    else:
        logger.error("{} not specified in config file.".format(attribute))
        return None


def get_file_last_modified_time(file_path):
    # returns local time (UTC + 5 30)

    modified_time_raw = os.path.getmtime(file_path)
    modified_time_utc = datetime.utcfromtimestamp(modified_time_raw)
    modified_time_SL = modified_time_utc + timedelta(hours=5, minutes=30)

    return modified_time_SL.strftime('%Y-%m-%d %H:%M:%S')


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def save_forecast_timeseries_to_db(pool, output, mike_stations, fgt, tms_meta):
    print('EXTRACT_MIKE_WATERLEVEL:: save_forecast_timeseries >>', tms_meta)

    # {
    #         'tms_id'     : '',
    #         'sim_tag'    : '',
    #         'station_id' : '',
    #         'source_id'  : '',
    #         'unit_id'    : '',
    #         'variable_id': ''
    #         }

    # iterating the stations
    for station in output.columns:
        ts = output[station].reset_index().values.tolist()  # including index

        tms_meta['latitude'] = str(mike_stations.get(station)[1])
        tms_meta['longitude'] = str(mike_stations.get(station)[2])
        tms_meta['station_id'] = mike_stations.get(station)[0]

        try:

            TS = Timeseries(pool=pool)

            tms_id = TS.get_timeseries_id_if_exists(meta_data=tms_meta)

            if tms_id is None:
                tms_id = TS.generate_timeseries_id(meta_data=tms_meta)
                tms_meta['tms_id'] = tms_id
                TS.insert_run(run_meta=tms_meta)
                TS.update_start_date(id_=tms_id, start_date=fgt)

            TS.insert_data(timeseries=ts, tms_id=tms_id, fgt=fgt, upsert=True)
            TS.update_latest_fgt(id_=tms_id, fgt=fgt)

        except Exception:
            logger.error("Exception occurred while pushing data to the curw_fcst database")
            traceback.print_exc()


def usage():
    usageText = """
    ----------------------------------------------------------------------------
    Extract MIKE output waterlevel to the curw_fcst database.
    -----------------------------------------------------------------------------
    
    Usage: ./outputs/extract_water_level.py [-m mike11_XXX] [-t XXX]
    [-d "/mnt/disks/curwsl_nfs/mike/outputs"] [-E]

    -h  --help          Show usage
    -m  --model         MIKE11 model (e.g. mike11_2016).
    -d  --dir           Output directory (e.g. "/mnt/disks/curwsl_nfs/mike/outputs"); 
                        Directory where HYCHAN.OUT and TIMDEP.OUT files located.
    -t  --sim_tag       Simulation tag
    -E  --event_sim     Weather the output should be extracted to event database or not (e.g. -E, --event_sim)
    """
    print(usageText)


if __name__ == "__main__":

    """
    wl_config.json 
    {
      "OUTPUT_FILE": "resmike11_WL.csv",
    
      "utc_offset": "",
    
      "sim_tag": "daily_run",
    
      "model": "MIKE11",
    
      "unit": "m",
      "unit_type": "Instantaneous",
    
      "variable": "WaterLevel"
    }

    """
    set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config.json'))

    try:

        mike_model = None
        output_dir = None
        sim_tag = None
        event_sim = False

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:m:d:t:E",
                                       ["help", "model=", "dir=", "sim_tag=", "event_sim"])
        except getopt.GetoptError:
            usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-m", "--model"):
                mike_model = arg.strip()
            elif opt in ("-d", "--dir"):
                output_dir = arg.strip()
            elif opt in ("-t", "--sim_tag"):
                sim_tag = arg.strip()
            elif opt in ("-E", "--event_sim"):
                event_sim = True

        config = json.loads(open(os.path.join(ROOT_DIRECTORY, 'outputs', 'wl_config.json')).read())

        # flo2D related details
        OUTPUT_FILE = read_attribute_from_config_file('OUTPUT_FILE', config, True)

        if mike_model is None:
            print("Please specify mike model.")
            usage()
            exit(1)
        if output_dir is None:
            print("Please specify mike output directory.")
            usage()
            exit(1)

        if not os.path.isdir(output_dir):
            print("Given output directory doesn't exist")
            exit(1)
        if mike_model not in ("mike11_2016"):
            print("Flo2d model should be \"mike11_2016\" ")
            exit(1)

        # sim tag
        if sim_tag is None:
            sim_tag = read_attribute_from_config_file('sim_tag', config, True)

        # source details
        model = read_attribute_from_config_file('model', config, True)
        version = "_".join(mike_model.split("_")[1:])

        # unit details
        unit = read_attribute_from_config_file('unit', config, True)
        unit_type = UnitType.getType(read_attribute_from_config_file('unit_type', config, True))

        # variable details
        variable = read_attribute_from_config_file('variable', config, True)

        output_file_path = os.path.join(output_dir, OUTPUT_FILE)

        pool = get_Pool(host=con_params.CURW_FCST_HOST, port=con_params.CURW_FCST_PORT, db=con_params.CURW_FCST_DATABASE,
                        user=con_params.CURW_FCST_USERNAME, password=con_params.CURW_FCST_PASSWORD)

        mike_stations = get_mike_stations(pool=pool)

        source_id = get_source_id(pool=pool, model=model, version=version)

        variable_id = get_variable_id(pool=pool, variable=variable)

        unit_id = get_unit_id(pool=pool, unit=unit, unit_type=unit_type)

        tms_meta = {
                'sim_tag'    : sim_tag,
                'model'      : model,
                'version'    : version,
                'variable'   : variable,
                'unit'       : unit,
                'unit_type'  : unit_type.value,
                'source_id'  : source_id,
                'variable_id': variable_id,
                'unit_id'    : unit_id
                }


        # Check output file exists
        if not os.path.exists(output_file_path):
            print('Unable to find file : ', output_file_path)
            traceback.print_exc()
            exit(1)

        fgt = get_file_last_modified_time(output_file_path)

        output_df = pd.read_csv(output_file_path, delimiter=',')
        output_df.set_index('Time Stamp', inplace=True)

        # Push timeseries to database
        save_forecast_timeseries_to_db(pool=pool, output=output_df, mike_stations=mike_stations, fgt=fgt, tms_meta=tms_meta)


        # run_info = json.loads(open(os.path.join(os.path.dirname(output_file_path), "run_meta.json")).read())
        # insert_run_metadata(pool=pool, source_id=source_id, variable_id=variable_id, sim_tag=sim_tag, fgt=fgt,
        #                     metadata=run_info, template_path=None)
    except Exception as e:
        traceback.print_exc()
    finally:
        logger.info("Process finished.")
        print("Process finished.")