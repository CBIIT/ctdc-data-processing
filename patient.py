import requests
import boto3
import csv
import hashlib
import os
import jsonpickle
import json
import random
import string
import sys
from bento.common.utils import get_md5, UUID, get_sha512, get_uuid

# Specifying the Constants for the Manifest File
GUID = 'GUID'
UUID = 'uuid'
TYPE = "type"
ACL = 'acl'
MD5 = 'md5'
SIZE = 'size'
URL = 'url'
FILE_NAME = 'file_name'
FILE_NODE = "file"
DEFAULT_ACL = "['Open']"
FILE_LOC = 'file_locations'
FILE_FORMAT = 'file_format'
FILE_STAT = 'file_status'
FILE_SIZE = "file_size"
FILE_TYPE = "file_type"
MD5SUM = "md5sum"
CASE_ID = "case_id"
FILE_STATUS = "file_status"
MSN = "molecularSequenceNumber"

INDEXD_GUID_PREFIX = 'dg.4DFC/'
MANIFEST_FIELDS = [GUID, UUID, MD5, MD5SUM, TYPE, SIZE,
                   ACL, URL, FILE_LOC, FILE_NAME, FILE_STATUS, FILE_FORMAT, FILE_TYPE, MSN]

# Subpath for PreSigned URL request
DOWNLOAD_URL_SUBPATH = '/download_url'


class Patient(object):
    def __init__(self, patientId, armId, acls, bucket):
        self.patientId = patientId
        self.armId = armId
        self.files = []
        self.acls = acls
        self.bucket = bucket


def getPatientsPreSignedURL(token, matchBaseUrlPatient='', myPatientList=[]):
    """
    This function retrieves the Presigned URL for each file for each
    patient in the list of patient objects provided in myPatientList
    """
    for patient in myPatientList:
        # Create the MATCH Api URL
        url = matchBaseUrlPatient+patient.patientId+DOWNLOAD_URL_SUBPATH
        # For each file in the patient object
        for file in patient.files:
            s3Url = ({"s3_url": file["s3url"]})
            headers = {
                'Authorization': token,
                'Content-Type': 'application/json'
            }
            r = requests.post(url, json=s3Url, headers=headers)
            # Add a dictionary item for the file object
            file['download_url'] = r.json()['download_url']
        # print(jsonpickle.encode(patient))


def getPatientsFileData(token, projection, matchBaseUrlPatient='', myPatientList=[]):
    """
    This function retrieves the relevant files for the list
    of patients specified by myPatientList using the Okta token.
    The projection parameter is used to retrieve only the fields
    of interest. It updates the myPatientList Object with the 
    filepaths for each file for each patient. 
    """

    # Set the Headers
    headers = {'Authorization': token}
    # Iterate to retrive the S3 Paths for all patients
    for patient in myPatientList:
        url = matchBaseUrlPatient+patient.patientId+'?'+'projection='+projection
        r = requests.get(url, headers=headers)
        response = r.json()
        getPatientS3Paths(response, patient)


def getPatientS3Paths(data, patientFromList=None):
    """
    This function iterates through the list of biopsies and filters
    out the sequences with a CONFIRMED variant report status and returns
    the dna, rna bam vcf dnabai and rnabai files.
    """

    fileInfo = {}
    #biopsesLen = len(data['biopsies'])
    for biopsy in data['biopsies']:
        for nextGenerationSequence in biopsy['nextGenerationSequences']:
            status = nextGenerationSequence['status']
            # Look for Biopsies with Confirmed Status
            if(status == 'CONFIRMED'):

                # Checking for each file type presence and adding it to the file object array for each
                # patient
                fileInfo = {}
                if('dnaBamFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'DNABam'
                    fileInfo["molecularSequenceNumber"] = nextGenerationSequence['ionReporterResults']["molecularSequenceNumber"]
                    fileInfo["s3url"] = nextGenerationSequence['ionReporterResults']['dnaBamFilePath']
                    patientFromList.files.append(fileInfo)

                fileInfo = {}
                if('rnaBamFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'RNABam'
                    fileInfo["molecularSequenceNumber"] = nextGenerationSequence['ionReporterResults']["molecularSequenceNumber"]
                    fileInfo["s3url"] = nextGenerationSequence['ionReporterResults']['rnaBamFilePath']
                    patientFromList.files.append(fileInfo)

                fileInfo = {}
                if('vcfFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'VCF'
                    fileInfo["molecularSequenceNumber"] = nextGenerationSequence['ionReporterResults']["molecularSequenceNumber"]
                    fileInfo["s3url"] = nextGenerationSequence['ionReporterResults']['vcfFilePath']
                    patientFromList.files.append(fileInfo)

                fileInfo = {}
                if('dnaBaiFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'DNABai'
                    fileInfo["molecularSequenceNumber"] = nextGenerationSequence['ionReporterResults']["molecularSequenceNumber"]
                    fileInfo["s3url"] = nextGenerationSequence['ionReporterResults']['dnaBaiFilePath']
                    patientFromList.files.append(fileInfo)

                fileInfo = {}
                if('rnaBaiFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'RNABai'
                    fileInfo["molecularSequenceNumber"] = nextGenerationSequence['ionReporterResults']["molecularSequenceNumber"]
                    fileInfo["s3url"] = nextGenerationSequence['ionReporterResults']['rnaBaiFilePath']
                    patientFromList.files.append(fileInfo)


def uploadPatientFiles(manifestpath='', myPatientList=[], domain='', useProd='False'):
    """
    This function uploads a set of files file pointed to from the Presigned 
    urls into a bucket with the specified key name.The manifestpath is where the file final manifest is stored.
    """

    s3_client = boto3.client('s3')

    # Open the IndexD Manifest File
    with open(manifestpath, 'w', newline='\n') as indexd_f:

        manifest_writer = csv.DictWriter(
            indexd_f, delimiter='\t', fieldnames=MANIFEST_FIELDS)

        # Write the Header Row
        manifest_writer.writeheader()
        # Process each Patient in the list
        for patient in myPatientList:
            # Get the name of the bucket
            bucket = patient.bucket
            for fileData in (patient.files):
                # Get the File using the PreSigned URL
                url = fileData['download_url']
                r = requests.get(url, stream=True)
                if (r.status_code >= 400):
                    print(f'Http Error Code {r.status_code}')

                # Get the Filename from the PreSigned URL
                filename = url.split("?")[0].split('/')[::-1][0]

                # Creating a pseudo variable for Non Production Test Data for different File types
                if(useProd == 'False'):
                    if(fileData["type"] == 'DNABam'):
                        filenameToUpload = './samples/SampleFile.bam'
                    if(fileData["type"] == 'RNABam'):
                        filenameToUpload = './samples/Sample2.bam'
                    if(fileData["type"] == 'VCF'):
                        filenameToUpload = './samples/Sample.vcf'
                    if(fileData["type"] == 'DNABai'):
                        filenameToUpload = './samples/SampleFile.bam.bai'
                    if(fileData["type"] == 'RNABai'):
                        filenameToUpload = './samples/Sample2.bam.bai'
                else:
                    # This is Production. Write the file to local disk
                    with open(filename, 'wb') as file:
                        if(r.content is not None):
                            file.write(r.content)
                        # Setting the Pseudo variable's name to the actual filename
                        filenameToUpload = filename

                # Set S3 Key to Match the MATCH MSN/FileName
                msn = fileData["molecularSequenceNumber"]
                s3_key = msn+'/'+filename

                # Upload File to S3
                #bucket.upload_file(Filename=filenameToUpload, Key=s3_key)
                with open(filenameToUpload, 'rb') as file:
                    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=file)

                md5sum = s3_client.head_object(
                    Bucket=bucket, Key=s3_key)['ETag'][1:-1]

                # Calculate MD5 , Size and Bucket Location
                md5 = get_md5(filenameToUpload)

                if(md5sum != md5):
                    print(f"Error Uploading file: {filename}")
                    sys.exit(1)

                print(f'Uploaded file {filenameToUpload}')
                # Get S3 location of the bucket
                s3_location = "s3://{}/{}".format(patient.bucket, s3_key)
                # Get the filesize of the downloaded file from MATCHBOX
                file_size = os.stat(filenameToUpload).st_size
                # Get the File format of the file downloaded
                file_format = (os.path.splitext(filenameToUpload)
                               [1]).split('.')[1].lower()
                # Calculating the SHA512
                currentDirectory = os.getcwd()
                sha512 = get_sha512(currentDirectory+'\\'+filenameToUpload)
                # Delete the file once it has been processed
                print(f'Processed File {filename} ')
                # Extract the ACL from the acls provided
                acl = "[{}]".format(patient.acls)
                # Generate the UUID and GUID using the SHA calculated
                uuid = get_uuid(domain, "file", sha512)
                guid = '{}{}'.format(INDEXD_GUID_PREFIX, uuid)
                # Generate the dictionary to be writtent to the manifest file
                fileInfo = {
                    GUID: guid,
                    UUID: uuid,
                    MD5: md5,
                    MD5SUM: md5,
                    TYPE: FILE_NODE,
                    SIZE: file_size,
                    ACL: acl,
                    URL: s3_location,
                    FILE_LOC: s3_location,
                    FILE_NAME: filename,
                    FILE_STATUS: "uploaded",
                    FILE_FORMAT: file_format,
                    FILE_TYPE: fileData["type"],
                    MSN: msn

                }
                print(jsonpickle.encode(fileInfo))
                # Write the entry into the Manifest file
                manifest_writer.writerow(fileInfo)
                # For Production Data Delete the File after Processing
                if(useProd == True):
                    if os.path.exists(filename):
                        os.remove(filename)
