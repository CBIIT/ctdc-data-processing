import base64
import json,requests

def getPatientsByTreatmentArm(arms=[],token=''):
    """
    This function gets a list of patients for each arm
    specified by the list of Patient Arms in arms. The
    token is the Okta token required for access to the Match 
    Environment
    """
    #Load the Config File
    with open('config.json') as config_file:
        data = json.load(config_file)
    #Get the Base URL    
    matchBaseUrl = data.get('matchUatBaseUrl')
    #Set the Headers
    headers = {'Authorization': token}
    patientList=[]
    #Retrieve the Patient List for each Arm
    for arm in (arms):
        url=matchBaseUrl+arm
        r = requests.get(url, headers=headers)
        response= r.json()
        plist=[]
        for items in (response):
            plist.append(items['patientSequenceNumber'])
        
        patientList.append(plist) 
    return patientList
    
    
