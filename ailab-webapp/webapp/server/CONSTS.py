import pathlib
import os


API_VERSION = '1.11'
PACKAGE_PATH = pathlib.Path().resolve()
ROOT_PATH = pathlib.Path.home()

if os.name == "nt":
    print(os.name)
    TEMP_PATH = PACKAGE_PATH
    EFS_PATH = None
else:
    EFS_PATH = ROOT_PATH.parent.parent.joinpath('mnt', 'efs_mtge')
    # EFS_PATH = ROOT_PATH.parent.parent.joinpath('home', 'gwu')
    TEMP_PATH = EFS_PATH.joinpath('tmp')

RESULT_S3_BUCKET = "fordham"
S3_NPL_OUTDIR = "output"
PRESIGNED_URL_LIFE = 7200
LOOKBACK_MONTHS_FOR_RECENT_DEALS = 4

# ECS settings
ECS_CLUSTER = "FargateCluster"
ECS_REGION = "us-east-1"
ECS_WORKER = "fordham-worker"
ECS_SERVER = "fordham-service"



ACCESS_TOKEN_EXPIRE_MINUTES = 60
ENCRYPT_ALGORITHM = "HS256"
ENCRYPT_SECRET_KEY = "15c517cde7fcbfc6b4272858b78071beea4ae49897067f986fc315dc24065ffa"


REPORT_FREQ_USER_ACTIVITY = 3600 * 4
DB_UPLOAD_MAX_ROW_NUM = 2048
