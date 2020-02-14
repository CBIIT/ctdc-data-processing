import requests
from patient import Patient

ARM_API_PATH = 'treatment_arms'

def getPatientsByTreatmentArm(arms=[], token='', matchArmUrl='', patientInputList=[]):
    """
    This function gets a list of patients for each arm
    specified by the list of Patient Arms in arms. The
    token is the Okta token required for access to the Match
    Environment
    """

    # Retrieve the Patient List for each Arm
    for arm in arms:
        patients = get_patients_for_arm(arm.arm_id, token, matchArmUrl)

        for patient_id in patients:
            patientInputList.append(Patient(patient_id, arm.arm_id, arm.phs_id, arm.bucket_name))

def get_patients_for_arm(arm_id='', token='', match_base_url=''):
    """
    This function gets a dict for assignmentStatusOutcome info for all patients an arm
    Key is patient's patientSequenceNumber, value is assignmentStatusOutcome
    The token is the Okta token required for access to the Match
    Environment
    """
    # Set the Headers
    headers = {'Authorization': token}
    arm_url = f'{match_base_url}/{ARM_API_PATH}/{arm_id}'
    arm_result = requests.get(arm_url, headers=headers)
    arms = arm_result.json()
    patients = {}
    arm_version = ''
    for arm in arms:
        current_version = arm.get('version')
        if arm_version == '' or current_version > arm_version:
            for patient in arm.get('summaryReport', {}).get('assignmentRecords', []):
                slot = int(patient.get('slot', -1))
                if slot > 0:
                    patients[patient.get('patientSequenceNumber')] = patient.get('assignmentStatusOutcome')

    return patients

