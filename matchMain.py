from tokens import get_okta_token
from secrets import get_secret
from treatmentarm import getPatientsByTreatmentArm
from patient import getPatientsFileData, getPatientsPreSignedURL, uploadPatientFiles
from datetime import datetime
import json
import argparse
import jsonpickle
import sys

# Specifying argument parsing from the command line
parser = argparse.ArgumentParser(
    description='Configuration File to run the File Uploader')
parser.add_argument("--file", required=True, type=str,
                    help="Name of Configuration File to run the File Uploader")
args = parser.parse_args()
try:
        # Read the Configuration File
    with open(args.file) as config_file:
        data = json.load(config_file)

    # Read the region
    region = data['region']
    # Get List of Arms
    armIds = data['armIds']
    acls = data['phsIds']
    bucketNames = data["bucketNames"]
    domain = data["domain"]
    useProd = data["useProd"]
    # Error Handling Code
    if((len(armIds)==len(acls)==len(bucketNames)) == False):
        print(f"Number of Arms, ACLs and Bucket Names ")
        
    if((data['useProd']) == 'True'):
        print('Using Match Production Environment')
        # Get the Secret Name
        secret_name = data['secretNameProd']
        # Get Okta Authorization URL
        oktaAuthUrl = data["oktaAuthUrlProd"]
        # Get the Match Treatment Arm Api URL
        matchBaseUrl = data['matchProdBaseUrl']
        # Get the Match Patient Api URL
        matchBaseUrlPatient = data['matchProdBaseUrlPatient']
    else:
        print('Using Match UAT Environment')
        # Get the Secret Name UAT
        secret_name = data['secretName']
        # Get Okta UAT Authorization URL
        oktaAuthUrl = data["oktaAuthUrl"]
        # Get the Match UAT Treatment Arm Api URL
        matchBaseUrl = data['matchUatBaseUrl']
        # Get the Match UAT Patient Api URL
        matchBaseUrlPatient = data['matchUatBaseUrlPatient']

    # Get Projection Query to read List of Files
    fileProjectionQuery = data['fileProjectionQuery']

    # Read Secrets from AWS Secrets Manager
    secrets = get_secret(region, secret_name)
    print('Secrets Read')
    # Retrieve the Okta Token
    token = get_okta_token(secrets, data, oktaAuthUrl)
    print('Token Obtained')

    myPatientList = []
    # Get the List of Patients for Each Arm
    getPatientsByTreatmentArm(
        armIds, token, matchBaseUrl, myPatientList, acls, bucketNames)
    print('List of Patients by Arm received')

    # Get the List of S3 Paths for each patient in each Arm
    getPatientsFileData(token, fileProjectionQuery,
                        matchBaseUrlPatient, myPatientList)
    print('Getting S3 Paths for all Patients in each Arm')

    print('List of File Paths received')
    getPatientsPreSignedURL(token, matchBaseUrlPatient, myPatientList)

    print('PreSigned Urls Generated')

    # Getting Bucket Name to upload files
    bucketName = secrets["S3_DEST_BUCKET_NAME"]

    print('Uploading Files...')

    manifest_filename = 'Manifest' + \
        str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + '.csv'

    uploadPatientFiles(manifest_filename, myPatientList, domain, useProd)
    print('Uploading Files Completed!')
    # Exiting Code
    # print(jsonpickle.encode(myPatientList))
    # sys.exit(0)


except Exception as e:
    # Print the cause of the exception
    print(e)
