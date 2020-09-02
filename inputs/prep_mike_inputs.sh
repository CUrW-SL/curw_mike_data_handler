#!/usr/bin/env bash

echo `date`

echo "Changing into ~/curw_mike_data_handler"
cd /home/uwcc-admin/curw_mike_data_handler
echo "Inside `pwd`"


# If no venv (python3 virtual environment) exists, then create one.
if [ ! -d "venv" ]
then
    echo "Creating venv python3 virtual environment."
    virtualenv -p python3 venv
fi

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate

# Install dependencies using pip.
if [ ! -f "mike_utils.log" ]
then
    echo "Installing PyMySQL"
    pip install PyMySQL
    echo "Installing PyYAML"
    pip install PyYAML
    echo "Installing db adapter"
    pip install git+https://github.com/shadhini/curw_db_adapter.git
    touch mike_utils.log
fi

# prepare mike input rain
echo "Preparing input rain for mike ..."
./inputs/gen_mike_input_rf_linux.py >> inputs/mike_input_rain.log 2>&1

# prepare mike input discharge
echo "Preparing input discharge for mike ..."
./inputs/gen_mike_input_dis_linux.py >> inputs/mike_input_dis.log 2>&1

# prepare mike input tide
echo "Preparing input tide for mike ..."
./inputs/gen_mike_input_tide_linux.py >> inputs/mike_input_tide.log 2>&1

# prepare mike input rainfall raw
echo "Preparing input raw rainfall for mike ..."
./inputs/gen_mike_input_rf_linux_all_stations_raw.py >> inputs/mike_input_rain_raw.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
