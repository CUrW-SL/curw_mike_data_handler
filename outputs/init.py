import traceback

from db_adapter.logger import logger
from db_adapter.constants import CURW_FCST_DATABASE, CURW_FCST_PORT, CURW_FCST_PASSWORD, CURW_FCST_USERNAME, \
    CURW_FCST_HOST
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import add_station, StationEnum
from db_adapter.constants import CURW_FCST_HOST, CURW_FCST_USERNAME, CURW_FCST_PASSWORD, CURW_FCST_PORT, CURW_FCST_DATABASE
from db_adapter.curw_sim.constants import FLO2D_250,

from db_adapter.csv_utils import read_csv

if __name__=="__main__":

    try:

        ##################################
        # Initialize parameters for MIKE #
        ##################################

        # source details
        MIKE_model = 'MIKE11'
        MIKE_version = ''

        # station details
        mike_stations =



    except Exception:
        logger.info("Initialization process failed.")
        traceback.print_exc()
    finally:
        logger.info("Initialization process finished.")
