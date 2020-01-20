import requests
import boto3
import csv
import hashlib
import os
import jsonpickle
import json
import random
import string
from bento.common.utils import get_md5, UUID, get_sha512, get_uuid

# Specifying the Constants for the Manifest File
GUID = 'GUID'
ACL = 'acl'
MD5 = 'md5'
SIZE = 'size'
URL = 'url'
FILE_NAME = 'file_name'
DEFAULT_ACL = "['Open']"
FILE_LOC = 'file_locations'
FILE_FORMAT = 'file_format'
FILE_STAT = 'file_status'
FILE_SIZE = "file_size"
FILE_TYPE = "file_type"
MD5SUM = "md5sum"
CASE_ID = "case_id"
FILE_STATUS = "file_status"

INDEXD_GUID_PREFIX = 'dg.4DFC/'
MANIFEST_FIELDS = [GUID, MD5, MD5SUM, SIZE,
                   ACL, URL, FILE_LOC, FILE_NAME, FILE_STATUS, FILE_FORMAT, FILE_TYPE, CASE_ID]

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
                    fileInfo["s3url"] = nextGenerationSequence['ionReporterResults']['dnaBamFilePath']
                    patientFromList.files.append(fileInfo)

                fileInfo = {}
                if('rnaBamFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'RNABam'
                    fileInfo["s3url"] = nextGenerationSequence['ionReporterResults']['rnaBamFilePath']
                    patientFromList.files.append(fileInfo)

                fileInfo = {}
                if('vcfFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'VCF'
                    fileInfo["s3url"] = nextGenerationSequence['ionReporterResults']['vcfFilePath']
                    patientFromList.files.append(fileInfo)

                fileInfo = {}
                if('dnaBaiFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'DNABai'
                    fileInfo["s3url"] = nextGenerationSequence['ionReporterResults']['dnaBaiFilePath']
                    patientFromList.files.append(fileInfo)

                fileInfo = {}
                if('rnaBaiFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'RNABai'
                    fileInfo["s3url"] = nextGenerationSequence['ionReporterResults']['rnaBaiFilePath']
                    patientFromList.files.append(fileInfo)


def uploadPatientFiles(manifestpath='', myPatientList=[], domain=''):
    """
    This function uploads a set of files file pointed to from the Presigned 
    urls into a bucket with the specified key name.The manifestpath is where the file final manifest is stored.
    """
    session = boto3.Session()
    s3 = session.resource('s3')

    # Open the IndexD Manifest File
    with open(manifestpath, 'w', newline='\n') as indexd_f:

        manifest_writer = csv.DictWriter(
            indexd_f, delimiter='\t', fieldnames=MANIFEST_FIELDS)
        manifest_writer.writeheader()
        for patient in myPatientList:
            bucket = s3.Bucket(patient.bucket)
            for fileData in (patient.files):
                # Get the File using the PreSigned URL
                url = fileData['download_url']
                r = requests.get(url, stream=True)
                if (r.status_code >= 400):
                    print(f'Http Error Code {r.status_code}')

                # Get the Filename from the PreSigned URL
                filename = url.split("?")[0].split('/')[::-1][0]
                filenameToUpload = 'sample.bam'
                s3_key = patient.patientId+'/'+filename
                # Write the file to local disk
                # with open(filename, 'wb') as file:
                #     if(r.content is not None):
                #         file.write(r.content)

                # Upload File to S3
                bucket.upload_file(Filename=filenameToUpload, Key=s3_key)

                # Calculate MD5 , Size and Bucket Location
                md5 = get_md5(filenameToUpload)
                s3_location = "s3://{}/{}".format(patient.bucket, s3_key)
                file_size = os.stat(filenameToUpload).st_size
                file_format = (os.path.splitext(filenameToUpload)
                               [1]).split('.')[1].lower()
                # Calculating the SHA512
                currentDirectory = os.getcwd()
                sha512 = get_sha512(currentDirectory+'\\'+filenameToUpload)
                # Delete the file once it has been processed
                print(f'Processed File {filename} ')

                acl = "[{}]".format(patient.acls)
                guid = '{}{}'.format(INDEXD_GUID_PREFIX,
                                     get_uuid(domain, "file", sha512))
                fileInfo = {
                    GUID: guid,
                    MD5: md5,
                    MD5SUM: md5,
                    SIZE: file_size,
                    ACL: acl,
                    URL: s3_location,
                    FILE_LOC: s3_location,
                    FILE_NAME: filename,
                    FILE_STATUS: "uploaded",
                    FILE_FORMAT: file_format,
                    FILE_TYPE: fileData["type"],
                    CASE_ID: patient.patientId

                }
                print(jsonpickle.encode(fileInfo))
                manifest_writer.writerow(fileInfo)

                # if os.path.exists(filename):
                #     os.remove(filename)
        # Write all the files for this patient

    #     # Write the Header if this is the first time entering the file
    #     if indexd_f.tell() == 0:
    #         manifest_writer.writeheader()

    #     # Generate the GUID
    #     # After Upload, Generate the Path Location
    #     # Write all these values to the Manifest File

        #
