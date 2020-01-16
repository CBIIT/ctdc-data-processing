from tokens import get_okta_token
from secrets import get_secret
from treatmentarm import getPatientsByTreatmentArm
from patient import getPatientsFileData, getPatientsPreSignedURL, uploadPatientFiles
from datetime import datetime
import json

# Read the Configuration File
with open('./config/config.json') as config_file:
    data = json.load(config_file)

# Read the region
region = data['region']
# Get List of Arms
armIds = data['armIds']
acls = data['phsIds']
if((data['useProd']) == 'TRUE'):
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
# Get the List of Patients for Each Arm
patientsListbyArm = getPatientsByTreatmentArm(armIds, token, matchBaseUrl)
print('List of Patients by Arm received')

fileListPatientArm = []
i = 0
# Get the List of S3 Paths for each patient in each Arm
while(i < len(armIds)):
    # print(len(patientsListbyArm[i]))
    fileListPatientArm.append(getPatientsFileData(
        patientsListbyArm[i], token, fileProjectionQuery, matchBaseUrlPatient))
    i += 1
print('List of File Paths received')

# getPatientsFileData(patientsListbyArm[0],token,fileProjectionQuery)
# print(len(fileListPatientArm[0][0]))
# signedUrlList=getPatientsPreSignedURL(patientsListbyArm[0][0],fileListPatientArm[0][0],token)

# Get the List of PreSigned URLS for the S3 Paths for each patient in each Arm
i, j = 0, 0
signedUrlList = []
while(i < len(armIds)):
    templist = []
    while (j < len(patientsListbyArm[i])):
        templist.append(getPatientsPreSignedURL(
            patientsListbyArm[i][j], fileListPatientArm[i][j], token, matchBaseUrlPatient))
        j += 1
    signedUrlList.append(templist)
    i += 1
print('PreSigned Urls Generated')
# print(signedUrlList[0][1])
# Generating a Sample Signed URL to test FileName Parsing
signedUrlSample = signedUrlList[0][1][0]
print(signedUrlSample.split("?")[0].split('/')[::-1][0])
i, j = 0, 0
# Getting Bucket Name to upload files
bucketName = secrets["S3_DEST_BUCKET_NAME"]

print('Uploading Files...')

manifest_filename = 'Manifest' + \
    str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + '.csv'
# For all Arms
while(i < len(armIds)):
    # For all Patients per Arm
    while (j < len(patientsListbyArm[i])):
        # Upload file
        uploadPatientFiles(signedUrlList[i][j],
                           acls[i], bucketName, manifest_filename)

print('Uploading Files Completed!')
