import requests
from patient import Patient
from bento.common.utils import get_logger


class ArmAPI:
    ARM_API_PATH = 'treatment_arms'
    VERSION = 'version'

    def __init__(self, token, base_url):
        if not token:
            raise ValueError('Token is not valid!')
        if not base_url:
            raise ValueError('base_url is not valid!')
        self.log = get_logger('Arm API')
        self.token = token
        self.base_url = base_url
        self.headers = {'Authorization': self.token}


    def _get_arm_url(self, arm_id):
        return f'{self.base_url}/{self.ARM_API_PATH}/{arm_id}'


    def _retrieve_arm_info(self, arm_id):
        '''
        Retrieve arm information from Match API, return latest version of the arm

        :param arm_id:
        :return: dict contains information of latest version of the arm
        '''
        arm_url = self._get_arm_url(arm_id)
        arm_result = requests.get(arm_url, headers=self.headers)


        arms = arm_result.json()
        arm_info = None
        for arm in arms:
            current_version = arm.get(self.VERSION)
            if not arm_info or current_version > arm_info[self.VERSION]:
                arm_info = arm

        return arm_info


    def getPatientsByTreatmentArm(self, arms, patient_list):
        """
        This function gets a list of patients for each arm
        specified by the list of Patient Arms in arms. The
        token is the Okta token required for access to the Match
        Environment
        """

        # Retrieve the Patient List for each Arm
        if not isinstance(arms, list):
            raise TypeError('Arms is not a list!')
        if not isinstance(patient_list, list):
            raise TypeError('Patient_lis is not a list!')
        for arm in arms:
            patients = self.get_patients_for_arm(arm.arm_id)

            for patient_id in patients:
                patient_list.append(Patient(patient_id, arm.arm_id, arm.phs_id, arm.bucket_name))

    @staticmethod
    def get_ctdc_arm_id(arm_id):
        '''
        Return CTDC simple version of arm_id, for EAY131-Z1D, return Z1D

        :param arm_id:
        :return:
        '''

        return arm_id.split('-')[-1]

    def get_arm_node(self, arm):
        """
        Retrieves information from API and returns a node for given arm_id

        :param arm_id: arm object with type Arm
        :return:
        """
        arm_id = arm.arm_id
        if not isinstance(arm_id, str):
            raise TypeError(f'arm_id "{arm_id}" is not a string!')
        arm_info = self._retrieve_arm_info(arm_id)
        obj = {}
        obj['arm_id'] = arm.ctdcArmId
        obj['clinical_trial.clinical_trial_id'] = arm.clinicalTrialID
        # obj['arm_id'] = self.get_ctdc_arm_id(arm_id)
        obj['arm_target'] = arm_info['gene']
        drugs = arm_info['treatmentArmDrugs']
        if len(drugs) == 1:
            obj['arm_drug'] = drugs[0]['name']
        else:
            raise Exception(f'Arm {arm_id} has {len(drugs)} drugs!')
        obj['pubmed_id'] = arm.pubmed_id

        return obj


    def get_patients_for_arm(self, arm_id):
        """
        This function gets a dict for assignmentStatusOutcome info for all patients an arm
        Key is patient's patientSequenceNumber, value is assignmentStatusOutcome
        The token is the Okta token required for access to the Match
        Environment
        """
        if not isinstance(arm_id, str):
            raise TypeError(f'arm_id "{arm_id}" is not a string!')
        patients = {}
        arm = self._retrieve_arm_info(arm_id)
        for patient in arm.get('summaryReport', {}).get('assignmentRecords', []):
            slot = int(patient.get('slot', -1))
            if slot > 0:
                patients[patient.get('patientSequenceNumber')] = patient.get('assignmentStatusOutcome')

        return patients

