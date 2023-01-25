import pathlib
import os
import pytz

API_VERSION = '0'
PACKAGE_PATH = pathlib.Path().resolve()
ROOT_PATH = pathlib.Path.home()

REPORT_FREQ_USER_ACTIVITY = 3600 * 4
DB_UPLOAD_MAX_ROW_NUM = 2048

GAME_RM_NOTIONAL = 100000
ANALYTICS_DECIMALS = 4
PRICE_DECIMAL = 2
TIME_ZONE = pytz.timezone('US/Eastern')