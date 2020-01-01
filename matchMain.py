from tokens import get_okta_token
from secrets import get_secret
from treatmentarm import getPatientsByTreatmentArm
from patient import getPatientsFileData,getPatientsPreSignedURL
import json

#Read the Configuration File
with open('config.json') as config_file:
    data = json.load(config_file)

#Read the region
region = data['region']
#Get the Secret Name
secret_name = data['secretName']
#Get List of Arms
armIds= data['armIds']
#Get Projection Query to read List of Files
fileProjectionQuery= data['fileProjectionQuery']

#Read Secrets from AWS Secrets Manager
secrets = get_secret(region,secret_name)
print('Secrets Read')
#Retrieve the Okta Token
token= get_okta_token(secrets,data)
print('Token Obtained')
#Get the List of Patients for Each Arm
patientsListbyArm = getPatientsByTreatmentArm(armIds,token)
print('List of Patients by Arm received')

fileListPatientArm=[]
i=0
#Get the List of S3 Paths for each patient in each Arm
while(i<len(armIds)):
    #print(len(patientsListbyArm[i]))
    fileListPatientArm.append(getPatientsFileData(patientsListbyArm[i],token,fileProjectionQuery))
    i+=1
print('List of File Paths received')

#getPatientsFileData(patientsListbyArm[0],token,fileProjectionQuery)
#print(len(fileListPatientArm[0][0]))
#signedUrlList=getPatientsPreSignedURL(patientsListbyArm[0][0],fileListPatientArm[0][0],token)

#Get the List of PreSigned URLS for the S3 Paths for each patient in each Arm
i,j=0,0
signedUrlList=[]
while(i<len(armIds)):
    templist=[]
    while (j<len(patientsListbyArm[i])):
        templist.append(getPatientsPreSignedURL(patientsListbyArm[i][j],fileListPatientArm[i][j],token))
        j+=1
    signedUrlList.append(templist)
    i+=1 
print('PreSigned Urls Generated')
#print(signedUrlList[0][0])
print(signedUrlList[0][1])
#print ("El secreto completo en JSON es ",secrets)
#print(get_okta_token(secrets,data))
