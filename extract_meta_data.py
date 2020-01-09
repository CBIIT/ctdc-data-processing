import csv
import os

from tokens import get_okta_token
from secrets import get_secret
from treatmentarm import getPatientsByTreatmentArm
from meta_data import get_patient_meta_data
from config import Config
from bento.common.utils import get_logger


CONFIG_FILE_ENVVAR = 'DATA_PROC_CONFIG_FILE'
JOIN = 'join'
ARRAY = 'array'
FIELD = 'field'
FIRST_ENTRY = 'first_entry'
SUB_FIELD = 'sub_field'
DRUGS = 'drugs'
DRUG_NAME = 'name'
DELIMITER = ', '
HARD_CODED_FUNC = 'hard_coded'
ARM_ID = 'arm_id'

class MetaData:
    def __init__(self, config):
        self.log = get_logger('Meta Data')
        self.config = config
        self.fields = {}

    @staticmethod
    def get_prior_drugs(desc):
        drugs = []
        for drug in desc:
            drugs.append(drug[DRUGS][0][DRUG_NAME])
        return DELIMITER.join(drugs)

    def get_fields(self, props):
        fields = [k for k in props.keys()]
        fields.append(ARM_ID)
        return fields

    @staticmethod
    def join_field_in_objects(objects, field, delimiter):
        return delimiter.join([obj[field] for obj in objects])

    def extract_case(self, data):
        obj = {ARM_ID: data[ARM_ID]}
        obj['patientSequenceNumber'] = data['patientSequenceNumber']
        obj['gender'] = data['gender']
        obj['races'] = DELIMITER.join(data['races'])
        obj['ethnicity'] = data['ethnicity']
        obj['currentPatientStatus'] = data['currentPatientStatus']
        obj['currentStepNumber'] = data['currentStepNumber']
        if len(data['diseases']) == 1:
            obj['diseases'] = data['diseases'][0].get('ctepTerm', '')
            obj['ctepCategory'] = data['diseases'][0].get('ctepCategory', '')
            obj['ctepSubCategory'] = data['diseases'][0].get('ctepSubCategory', '')
            obj['disease_id'] = data['diseases'][0].get('_id', '')
        else:
            self.log.warning('wrong number of diseases!')

        obj['priorDrugs'] = self.get_prior_drugs(data['priorDrugs'])
        return [obj]

    def extract_specimen(self, data):
        objs = []
        for biopsy in data['biopsies']:
            obj = {}
            obj['patientSequenceNumber'] = data['patientSequenceNumber']
            obj['biopsySequenceNumber'] = biopsy['biopsySequenceNumber']
            objs.append(obj)
        return objs


    def process_nodes(self):
        self.nodes = {}
        self.nodes['case'] = []
        self.fields['case'] = [
            ARM_ID,
            'patientSequenceNumber',
            'gender',
            'races',
            'ethnicity',
            'currentPatientStatus',
            'currentStepNumber',
            'diseases',
            'ctepCategory',
            'ctepSubCategory',
            'disease_id',
            'priorDrugs'
        ]

        self.nodes['specimen'] = []
        self.fields['specimen'] = [
            'patientSequenceNumber',
            'biopsySequenceNumber'
        ]

        for data in self.patient_data:
            self.nodes['case'].extend(self.extract_case(data))
            self.nodes['specimen'].extend(self.extract_specimen(data))

    def write_files(self):
        nodes = ['case', 'specimen']
        for node in nodes:
            file_name = 'tmp/{}.csv'.format(node)
            with open(file_name, 'w') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=self.fields[node])
                writer.writeheader()

                for obj in self.nodes[node]:
                    writer.writerow(obj)


    def extract(self):
        # Read Secrets from AWS Secrets Manager
        secrets = get_secret(self.config.region, self.config.secret_name)
        self.log.info('Secrets Read')
        # Retrieve the Okta Token
        token = get_okta_token(secrets, self.config.data, self.config.oktaAuthUrl)
        self.log.info('Token Obtained')
        # Get the List of Patients for Each Arm
        self.patient_data = []
        for armID in self.config.armIds:
            patientsListbyArm = getPatientsByTreatmentArm([armID], token, self.config.matchBaseUrl)
            self.log.info('List of Patients by Arm received')
            for patients in patientsListbyArm:
                for p in patients:
                    data = get_patient_meta_data(token, self.config.matchBaseUrlPatient, p)
                    data[ARM_ID] = armID
                    self.patient_data.append(data)

        self.process_nodes()
        self.write_files()


if __name__ == '__main__':
    config_file = os.environ.get(CONFIG_FILE_ENVVAR, 'config/config.json')
    config = Config(config_file)

    meta_data = MetaData(config)
    meta_data.extract()