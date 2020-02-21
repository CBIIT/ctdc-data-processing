import requests
from patient import Patient

ARM_API_PATH = 'treatment_arms'

class ArmAPI:
    def __init__(self, token, base_url):
        assert token
        assert base_url
        self.token = token
        self.base_url = base_url

    def getPatientsByTreatmentArm(self, arms, patient_list):
        """
        This function gets a list of patients for each arm
        specified by the list of Patient Arms in arms. The
        token is the Okta token required for access to the Match
        Environment
        """

        # Retrieve the Patient List for each Arm
        assert isinstance(arms, list)
        assert isinstance(patient_list, list)
        for arm in arms:
            patients = self.get_patients_for_arm(arm.arm_id)

            for patient_id in patients:
                patient_list.append(Patient(patient_id, arm.arm_id, arm.phs_id, arm.bucket_name))

    def get_patients_for_arm(self, arm_id):
        """
        This function gets a dict for assignmentStatusOutcome info for all patients an arm
        Key is patient's patientSequenceNumber, value is assignmentStatusOutcome
        The token is the Okta token required for access to the Match
        Environment
        """
        assert isinstance(arm_id, str)
        # Set the Headers
        headers = {'Authorization': self.token}
        arm_url = f'{self.base_url}/{ARM_API_PATH}/{arm_id}'
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

