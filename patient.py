import requests
import boto3
import csv
import hashlib
import os
from bento.common.utils import get_md5, UUID

# Specifying the Constants for the Manifest File
GUID = 'GUID'
ACL = 'acl'
MD5 = 'md5'
SIZE = 'size'
URL = 'url'
DEFAULT_ACL = "['Open']"
MANIFEST_FIELDS = [GUID, MD5, SIZE, ACL, URL]

# Subpath for PreSigned URL request
DOWNLOAD_URL_SUBPATH = '/download_url'


def getPatientsPreSignedURL(patientId, s3PathList, token, matchBaseUrlPatient=''):
    """
    This function returns a list of preSigned URLs for the files specified
    in the s3Paths list for the patient specified by patientId
    using the Okta Token specified by token
    """
    url = matchBaseUrlPatient+patientId+DOWNLOAD_URL_SUBPATH
    signedUrlList = []
    for path in s3PathList:
        s3Url = ({"s3_url": path})
        headers = {
            'Authorization': token,
            'Content-Type': 'application/json'
        }
        r = requests.post(url, json=s3Url, headers=headers)
        response = r.json()
        signedUrlList.append(response['download_url'])
    return signedUrlList


def getPatientsFileData(patients, token, projection, matchBaseUrlPatient=''):
    """
    This function retrieves the relevant files for the list
    of patients specified by patients using the Okta token.
    The projection parameter is used to retrieve only the fields
    of interest. It returns a 2D list of s3 paths indexed by
    patient and files. 
    """

    fileList = []
    # Set the Headers
    headers = {'Authorization': token}
    # Iterate to retrive the S3 Paths for all patients
    for patient in patients:
        url = matchBaseUrlPatient+patient+'?'+'projection='+projection
        r = requests.get(url, headers=headers)
        response = r.json()
        pathList = getPatientS3Paths(response)
        fileList.append(pathList)
    return fileList


def getPatientS3Paths(data):
    """
    This function iterates through the list of biopsies and filters
    out the sequences with a CONFIRMED variant report status and returns
    the dna, rna bam and the vcf files.
    """
    s3PathList = []
    #biopsesLen = len(data['biopsies'])
    for biopsy in data['biopsies']:
        for nextGenerationSequence in biopsy['nextGenerationSequences']:
            status = nextGenerationSequence['status']
            if(status == 'CONFIRMED'):
                s3PathList.append(
                    (nextGenerationSequence['ionReporterResults']['dnaBamFilePath']))
                s3PathList.append(
                    (nextGenerationSequence['ionReporterResults']['rnaBamFilePath']))
                s3PathList.append(
                    (nextGenerationSequence['ionReporterResults']['vcfFilePath']))
    return s3PathList


def uploadPatientFiles(urls=[], acl=DEFAULT_ACL, bucket_name='', manifestpath=''):
    """
    This function uploads a set of files file pointed to from the Presigned 
    urls into a bucket with the specified key name.The manifestpath is where the file final manifest is stored.
    """
    session = boto3.Session()
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for url in urls:
        # Get the File using the PreSigned URL
        r = requests.get(url, stream=True)
        # Get the Filename from the PreSigned URL
        filename = url.split("?")[0].split('/')[::-1][0]
        s3_key = filename
        # Write the file to local disk
        with open(filename, 'wb') as file:
            file.write(r.content)

        # Upload File to S3
        bucket.upload_file(Filename=filename, Key=s3_key)

        #print(f'Wrote File {filename} to disk')

        # Calculate MD5 , Size and Bucket Location
        md5 = get_md5(filename)
        s3_location = "s3://{}/{}".format(bucket, s3_key)
        file_size = os.stat(filename).st_size

        # Generate the GUID
        # After Upload, Generate the Path Location
        # Write all these values to the Manifest File

        #
        # Delete the file once it has been processed
        os.remove(filename)
