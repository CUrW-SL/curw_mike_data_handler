#!/home/uwcc-admin/curw_mike_data_handler/venv/bin/python3
import traceback, os, getopt, sys
import json

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params
from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import add_station, StationEnum
# from db_adapter.constants import CURW_FCST_HOST, CURW_FCST_USERNAME, CURW_FCST_PASSWORD, CURW_FCST_PORT, CURW_FCST_DATABASE

from db_adapter.csv_utils import read_csv

ROOT_DIRECTORY = '/home/uwcc-admin/curw_mike_data_handler'


if __name__=="__main__":

    set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config.json'))

    try:

        ##################################
        # Initialize parameters for MIKE #
        ##################################

        # station details
        mike_stations = read_csv(os.path.join(ROOT_DIRECTORY, 'resources/mike_stations.csv'))

        pool = get_Pool(host=con_params.CURW_FCST_HOST, port=con_params.CURW_FCST_PORT, user=con_params.CURW_FCST_USERNAME, password=con_params.CURW_FCST_PASSWORD,
                db=con_params.CURW_FCST_DATABASE)

        for station in mike_stations:
            id = station[0]
            station_name = station[1]
            lat = station[2]
            lon = station[3]
            add_station(pool=pool, name=station_name,
                    latitude="%.6f" % float(lat),
                    longitude="%.6f" % float(lon),
                    station_type=StationEnum.MIKE11, description="mike_station")

        destroy_Pool(pool=pool)

    except Exception:
        print("Initialization process failed.")
        traceback.print_exc()
    finally:
        print("Initialization process finished.")
