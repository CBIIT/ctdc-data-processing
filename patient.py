import csv
import os
import shutil

import requests

from bento.common.s3 import S3Bucket
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
MSN = "sequencing_assay.molecularSequenceNumber"
PSN = 'patientSequenceNumber'
S3URL = 's3url'

INDEXD_GUID_PREFIX = 'dg.4DFC/'
MANIFEST_FIELDS = [PSN, GUID, UUID, MD5, MD5SUM, TYPE, SIZE,
                   ACL, URL, FILE_LOC, FILE_NAME, FILE_STATUS, FILE_FORMAT, FILE_TYPE, MSN]

FILE_PROPS_PROJECTION = "biopsies.nextGenerationSequences.ionReporterResults.dnaBamFilePath,biopsies.nextGenerationSequences.ionReporterResults.rnaBamFilePath,biopsies.nextGenerationSequences.ionReporterResults.vcfFilePath,biopsies.nextGenerationSequences.ionReporterResults.dnaBaiFilePath,biopsies.nextGenerationSequences.ionReporterResults.rnaBaiFilePath,biopsies.nextGenerationSequences.ionReporterResults.molecularSequenceNumber,biopsies.nextGenerationSequences.status"

# Subpath for PreSigned URL request
DOWNLOAD_API_PATH = '/download_url'
PATIENTS_API_PATH = 'patients'


class Patient(object):
    def __init__(self, patientId, armId, acls, bucket):
        self.patientId = patientId
        self.armId = armId
        self.files = []
        self.acls = acls
        self.bucket = bucket


def get_signed_url(token, match_base_url, patien_id, s3_url):
    url = f'{match_base_url}/{PATIENTS_API_PATH}/{patien_id}{DOWNLOAD_API_PATH}'
    query = ({"s3_url": s3_url})
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json'
    }
    r = requests.post(url, json=query, headers=headers)
    if r.status_code >= 400:
        raise Exception(f'Can NOT retrieve signed URL for {patien_id}: {s3_url}')
    # Add a dictionary item for the file object
    return r.json()['download_url']


def getPatientsPreSignedURL(token, match_base_url, my_patient_list):
    """
    This function retrieves the Presigned URL for each file for each
    patient in the list of patient objects provided in myPatientList
    """
    for patient in my_patient_list:
        # For each file in the patient object
        for file in patient.files:
            # Add a dictionary item for the file object
            file['download_url'] = get_signed_url(token, match_base_url, patient.patientId, file[S3URL])


def getPatientsFileData(token, match_base_url, my_patient_list):
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
    for patient in my_patient_list:
        url = f'{match_base_url}/{PATIENTS_API_PATH}/{patient.patientId}?projection={FILE_PROPS_PROJECTION}'
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
                    fileInfo[S3URL] = nextGenerationSequence['ionReporterResults']['dnaBamFilePath']
                    patientFromList.files.append(fileInfo)

                fileInfo = {}
                if('rnaBamFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'RNABam'
                    fileInfo["molecularSequenceNumber"] = nextGenerationSequence['ionReporterResults']["molecularSequenceNumber"]
                    fileInfo[S3URL] = nextGenerationSequence['ionReporterResults']['rnaBamFilePath']
                    patientFromList.files.append(fileInfo)

                fileInfo = {}
                if('vcfFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'VCF'
                    fileInfo["molecularSequenceNumber"] = nextGenerationSequence['ionReporterResults']["molecularSequenceNumber"]
                    fileInfo[S3URL] = nextGenerationSequence['ionReporterResults']['vcfFilePath']
                    patientFromList.files.append(fileInfo)

                fileInfo = {}
                if('dnaBaiFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'DNABai'
                    fileInfo["molecularSequenceNumber"] = nextGenerationSequence['ionReporterResults']["molecularSequenceNumber"]
                    fileInfo[S3URL] = nextGenerationSequence['ionReporterResults']['dnaBaiFilePath']
                    patientFromList.files.append(fileInfo)

                fileInfo = {}
                if('rnaBaiFilePath' in nextGenerationSequence['ionReporterResults']):
                    fileInfo["type"] = 'RNABai'
                    fileInfo["molecularSequenceNumber"] = nextGenerationSequence['ionReporterResults']["molecularSequenceNumber"]
                    fileInfo[S3URL] = nextGenerationSequence['ionReporterResults']['rnaBaiFilePath']
                    patientFromList.files.append(fileInfo)


def uploadPatientFiles(token, match_base_url, manifestpath, myPatientList, domain, useProd, cipher, log):
    """
    This function uploads a set of files file pointed to from the Pre-signed
    urls into a bucket with the specified key name.The manifestpath is where the file final manifest is stored.
    """

    # Open the IndexD Manifest File
    with open(manifestpath, 'w', newline='\n') as indexd_f:

        manifest_writer = csv.DictWriter(
            indexd_f, delimiter='\t', fieldnames=MANIFEST_FIELDS)

        # Write the Header Row
        manifest_writer.writeheader()
        totalPatients = len(myPatientList)

        if myPatientList:
            bucket = myPatientList[0].bucket
            s3_bucket = S3Bucket(bucket)
        else:
            log.warning('Empty patient list!')
            return

        files_uploaded = 0

        # Process each Patient in the list
        for index, patient in enumerate(myPatientList):
            log.info(f'Uploading Data for Patient {index + 1} of {totalPatients}')

            # Get the name of the bucket
            bucket = patient.bucket
            # Only create a new bucket when bucket name is different from current patient's
            if bucket != s3_bucket.bucket_name:
                s3_bucket = S3Bucket(bucket)

            for fileData in (patient.files):
                # Get the File using the PreSigned URL
                url = get_signed_url(token, match_base_url, patient.patientId, fileData[S3URL])
                # Get the Filename from the PreSigned URL

                # Todo: filename should be extracted from fileData, extracting from signed URL causes files with characters
                #       not compatible with URL to get encoded and won't match orginal file name any more
                filename = url.split("?")[0].split('/')[::-1][0]


                try:
                    if not useProd:
                        # Creating a pseudo variable for Non Production Test Data for different File types
                        # UAT: don't download files (because there are no real files in the given bucket
                        # Upload sample files for every case
                        if(fileData["type"] == 'DNABam'):
                            filenameToUpload = './samples/SampleFile.bam'
                        elif(fileData["type"] == 'RNABam'):
                            filenameToUpload = './samples/Sample2.bam'
                        elif(fileData["type"] == 'VCF'):
                            filenameToUpload = './samples/Sample.vcf'
                        elif(fileData["type"] == 'DNABai'):
                            filenameToUpload = './samples/SampleFile.bam.bai'
                        elif(fileData["type"] == 'RNABai'):
                            filenameToUpload = './samples/Sample2.bam.bai'
                        else:
                            raise Exception(f'Wrong file type: "{fileData["type"]}"')

                        log.info(f'Use sample file {filenameToUpload}')
                    else:
                        log.info(f'Downloading file {filename}')
                        # Production: download real files and upload them
                        with requests.get(url, stream=True) as r:
                            # If Error is found and we are in Prod Print and Exit
                            if r.status_code >= 400:
                                log.error(f'Http Error Code {r.status_code} for file {filename}')
                                continue

                            filenameToUpload = f'tmp/{filename}'
                            # This is Production. Write the file to local disk
                            with open(filenameToUpload, 'wb') as file:
                                shutil.copyfileobj(r.raw, file)
                                # Setting the Pseudo variable's name to the actual filename

                    # Set S3 Key to Match the MATCH MSN/FileName
                    msn = fileData["molecularSequenceNumber"]
                    s3_key = msn+'/'+filename

                    log.info(f'Uploading file {filenameToUpload} to s3://{bucket}/{s3_key}')
                    # Upload File to S3
                    upload_result = s3_bucket.upload_file(s3_key, filenameToUpload)

                    md5sum = upload_result['md5']

                    # Calculate MD5 , Size and Bucket Location
                    md5 = get_md5(filenameToUpload)

                    if(md5sum != md5):
                        log.error(f"Uploading file to s3://{bucket}/{s3_key} FAILED")
                        continue

                    log.info(f"Uploading file to s3://{bucket}/{s3_key} SUCCEEDED")
                    # Get S3 location of the bucket
                    s3_location = "s3://{}/{}".format(patient.bucket, s3_key)
                    # Get the filesize of the downloaded file from MATCHBOX
                    file_size = os.stat(filenameToUpload).st_size
                    # Get the File format of the file downloaded
                    file_format = (os.path.splitext(filenameToUpload)
                                   [1]).split('.')[1].lower()
                    # Calculating the SHA512
                    #currentDirectory = os.getcwd()
                    sha512 = get_sha512(os.path.abspath(filenameToUpload))
                    # Extract the ACL from the acls provided
                    acl = "[{}]".format(patient.acls)
                    # Generate the UUID and GUID using the SHA calculated
                    if useProd:
                        uuid = get_uuid(domain, "file", sha512)
                    else:
                        uuid = get_uuid(domain, "file", s3_location + sha512)
                    guid = '{}{}'.format(INDEXD_GUID_PREFIX, uuid)
                    # Generate the dictionary to be writtent to the manifest file
                    fileInfo = {
                        PSN: cipher.simple_cipher(patient.patientId),
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
                    # Write the entry into the Manifest file
                    manifest_writer.writerow(fileInfo)
                    # For Production Data Delete the File after Processing
                    if useProd:
                        if os.path.exists(filenameToUpload):
                            os.remove(filenameToUpload)
                except Exception as e:
                    log.exception(e)
                else:
                    log.info(f'Process file {filename} Succeeded')
                    files_uploaded += 1
        return files_uploaded


def get_patient_meta_data(token, base_url, patient_id):
    url = f'{base_url}/{PATIENTS_API_PATH}/{patient_id}'
    headers = {'Authorization': token}
    result = requests.get(url, headers=headers)
    if result and result.ok:
        return result.json()
    else:
        return None
