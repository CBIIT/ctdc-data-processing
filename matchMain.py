import argparse
from datetime import datetime
import os

from bento.common.utils import get_logger, get_log_file, LOG_PREFIX, APP_NAME
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Match_File_Loader'

from bento.common.s3 import S3Bucket
from bento.common.secrets import get_secret
from bento.common.simple_cipher import SimpleCipher
from bento.common.tokens import get_okta_token

from config import Config
from patient import getPatientsFileData, uploadPatientFiles
from treatmentarm import ArmAPI


# Specifying argument parsing from the command line
parser = argparse.ArgumentParser(description='Extract file information from NCI MATCH API, and upload files to CDS bucket')
parser.add_argument("config_file", help="Name of Configuration File to run the File Uploader")
args = parser.parse_args()

LOGER_NAME = 'MATCH File Loader'
MANIFEST_FOLDER = 'Manifests'
os.environ[APP_NAME] = 'MATCH_File_Loader'
log = get_logger(LOGER_NAME)
log_file = get_log_file()


def upload_meta_file(bucket_name, file_path):
    base_name = os.path.basename(file_path)
    s3 = S3Bucket(bucket_name)
    key = f'{MANIFEST_FOLDER}/{base_name}'
    return s3.upload_file(key, file_path)


try:
    config = Config(args.config_file)

    cipher = SimpleCipher(config.cipher_key)

    if config.use_prod:
        log.info('Using Match Production Environment')
    else:
        log.info('Using Match UAT Environment')

    # Read Secrets from AWS Secrets Manager
    secrets = get_secret(config.region, config.secret_name)
    log.info('Secrets Read')
    # Retrieve the Okta Token
    token = get_okta_token(secrets, config.okta_auth_url)
    log.info('Token Obtained')

    myPatientList = []
    # Get the List of Patients for Each Arm
    arm_api = ArmAPI(token, config.match_base_url)
    arm_api.getPatientsByTreatmentArm(config.arms, myPatientList)
    log.info('List of Patients by Arm received')

    # Get the List of S3 Paths for each patient in each Arm
    getPatientsFileData(token, config.match_base_url, myPatientList)
    log.info('Getting S3 Paths for all Patients in each Arm')

    log.info('Uploading Files...')

    manifest_filename = 'tmp/Manifest' + \
        str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + '.tsv'

    log.info('Uploading patient files...')
    uploadPatientFiles(token, config.match_base_url, manifest_filename, myPatientList, config.domain, config.use_prod, cipher, log)
    log.info('Uploading Files Completed!')

    upload_meta_file(config.meta_data_bucket, manifest_filename)
    log.info(f'Manifest file {manifest_filename} has been uploaded to s3://{config.meta_data_bucket}')

    upload_meta_file(config.meta_data_bucket, log_file)
    log.info(f'Log file {log_file} has been uploaded to s3://{config.meta_data_bucket}')

except Exception as e:
    # Print the cause of the exception
    log.exception(e)
