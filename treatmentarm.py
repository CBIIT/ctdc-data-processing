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

def get_assignment_status_outcome_for_arm(arm_id='', token='', matchArmUrl=''):
    """
    This function gets a dict for assignmentStatusOutcome info for all patients an arm
    Key is patient's patientSequenceNumber, value is assignmentStatusOutcome
    The token is the Okta token required for access to the Match
    Environment
    """
    # Set the Headers
    headers = {'Authorization': token}
    arm_url = matchArmUrl + arm_id
    arm_result = requests.get(arm_url, headers=headers)
    arms = arm_result.json()
    patients = {}
    arm_version = ''
    for arm in arms:
        current_version = arm.get('version')
        if arm_version == '' or current_version > arm_version:
            for patient in arm.get('summaryReport', {}).get('assignmentRecords', []):
                patients[patient.get('patientSequenceNumber')] = patient.get('assignmentStatusOutcome')

    return patients


def get_patient_ids_by_treatment_arm(arm, token='', matchBaseUrl=''):
    """
    This function gets a list of patients for arm
    specified by the ID. The token is the Okta token
    required for access to the Match Environment
    """
    print(matchBaseUrl)
    # Set the Headers
    headers = {'Authorization': token}
    patient_list = []
    # Retrieve the Patient List for each Arm
    url = matchBaseUrl + arm
    r = requests.get(url, headers=headers)
    response = r.json()
    for items in (response):
        patient_list.append(items['patientSequenceNumber'])

    return patient_list
