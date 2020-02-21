import argparse
from datetime import datetime

from bento.common.secrets import get_secret
from bento.common.simple_cipher import SimpleCipher
from bento.common.tokens import get_okta_token
from bento.common.utils import get_logger

from config import Config
from patient import getPatientsFileData, getPatientsPreSignedURL, uploadPatientFiles
from treatmentarm import getPatientsByTreatmentArm


# Specifying argument parsing from the command line
parser = argparse.ArgumentParser(description='Extract file information from NCI MATCH API, and upload files to CDS bucket')
parser.add_argument("config_file", help="Name of Configuration File to run the File Uploader")
args = parser.parse_args()

log = get_logger('MATCH File Extractor')
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
    getPatientsByTreatmentArm(config.arms, token, config.match_base_url, myPatientList)
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


except Exception as e:
    # Print the cause of the exception
    log.exception(e)
