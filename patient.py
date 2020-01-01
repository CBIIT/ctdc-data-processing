import base64
import json,requests

DOWNLOAD_URL_SUBPATH='/download_url'

with open('config.json') as config_file:
    config = json.load(config_file)
    MATCH_BASEURL_PATIENT=config['matchUatBaseUrlPatient']


def getPatientsPreSignedURL(patientId,s3PathList,token):
    """
    This function returns a list of preSigned URLs for the files specified
    in the s3Paths list for the patient specified by patientId
    using the Okta Token specified by token
    """
    url= MATCH_BASEURL_PATIENT+patientId+DOWNLOAD_URL_SUBPATH
    signedUrlList=[]
    for path in s3PathList:
        s3Url=({"s3_url":path})
        headers={
            'Authorization': token,
            'Content-Type': 'application/json'
        }
        r=requests.post(url,json=s3Url,headers=headers)
        response = r.json()
        signedUrlList.append(response['download_url'])
    return signedUrlList



def getPatientsFileData(patients, token, projection):
    """
    This function retrieves the relevant files for the list
    of patients specified by patients using the Okta token.
    The projection parameter is used to retrieve only the fields
    of interest. It returns a 2D list of s3 paths indexed by
    patient and files.
    """
    #Load the Config File   
    matchBaseUrlPatient = config['matchUatBaseUrlPatient']    
        
    fileList=[]
    #Set the Headers
    headers = {'Authorization': token}
    # Iterate to retrive the S3 Paths for all patients
    for patient in patients:
        url= matchBaseUrlPatient+patient+'?'+'projection='+projection
        r = requests.get(url, headers=headers)
        response= r.json()
        pathList=getPatientS3Paths(response)
        fileList.append(pathList)
    return fileList


def getPatientS3Paths(data):
    """
    This function iterates through the list of biopsies and filters
    out the sequences with a CONFIRMED variant report status and returns
    the dna, rna bam and the vcf files.
    """
    s3PathList=[]
    #biopsesLen = len(data['biopsies'])
    for biopsy in data['biopsies']:
        for nextGenerationSequence in biopsy['nextGenerationSequences']:
            status = nextGenerationSequence['status']
            if(status=='CONFIRMED'):
                s3PathList.append((nextGenerationSequence['ionReporterResults']['dnaBamFilePath']))
                s3PathList.append((nextGenerationSequence['ionReporterResults']['rnaBamFilePath']))
                s3PathList.append((nextGenerationSequence['ionReporterResults']['vcfFilePath']))  
    return s3PathList


    
        
        
        

		

