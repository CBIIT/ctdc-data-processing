import requests
from patient import Patient


def getPatientsByTreatmentArm(arms=[], token='', matchBaseUrl='', patientInputList=[], acls=[], bucketNames=[]):
    """
    This function gets a list of patients for each arm
    specified by the list of Patient Arms in arms. The
    token is the Okta token required for access to the Match
    Environment
    """
    print(matchBaseUrl)
    # Set the Headers
    headers = {'Authorization': token}

    # Retrieve the Patient List for each Arm
    for index, arm in enumerate(arms):
        url = matchBaseUrl+arm
        r = requests.get(url, headers=headers)
        response = r.json()

        for items in (response):

            patientInputList.append(
                Patient(items['patientSequenceNumber'], arm, acls[index], bucketNames[index]))
