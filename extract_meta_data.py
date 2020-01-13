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
        obj['patientSequenceNumber'] = data.get('patientSequenceNumber')
        obj['gender'] = data.get('gender')
        obj['races'] = DELIMITER.join(data['races'])
        obj['ethnicity'] = data.get('ethnicity')
        obj['currentPatientStatus'] = data.get('currentPatientStatus')
        obj['currentStepNumber'] = data.get('currentStepNumber')
        diseases = data.get('diseases', [])
        if len(diseases) == 1:
            obj['diseases'] = diseases[0].get('ctepTerm', '')
            obj['ctepCategory'] = diseases[0].get('ctepCategory', '')
            obj['ctepSubCategory'] = diseases[0].get('ctepSubCategory', '')
            obj['disease_id'] = diseases[0].get('_id', '')
        else:
            self.log.warning('wrong number of diseases!')

        obj['priorDrugs'] = self.get_prior_drugs(data.get('priorDrugs', []))
        return [obj]

    def extract_specimen(self, data):
        objs = []
        for biopsy in data.get('biopsies', []):
            obj = {}
            obj['patientSequenceNumber'] = data.get('patientSequenceNumber')
            obj['biopsySequenceNumber'] = biopsy.get('biopsySequenceNumber')
            objs.append(obj)
        return objs

    def extract_nucleic_acid(self, data):
        objs = []
        for biopsy in data.get('biopsies', []):
            biopsy_sequence_num = biopsy.get('biopsySequenceNumber')
            for message in biopsy.get('mdAndersonMessages', []):
                if 'molecularSequenceNumber' in message:
                    obj = {'biopsySequenceNumber': biopsy_sequence_num}
                    obj['molecularSequenceNumber'] = message.get('molecularSequenceNumber')
                    obj['dnaConcentration'] = message.get('dnaConcentration')
                    obj['dnaVolume'] = message.get('dnaVolume')
                    objs.append(obj)
                else:
                    self.log.info('mdAndersonMessages does NOT have a molecularSequenceNumber, it is not a nucleic_acid')
        return objs

    def extract_ihc_assay_report(self, data):
        objs = []
        # There are almost identical information in "patient_assignments/*/assayMessages"
        for biopsy in data.get('biopsies', []):
            biopsy_sequence_num = biopsy.get('biopsySequenceNumber')
            for message in biopsy.get('assayMessages', []):
                if 'result' in message:
                    obj = {'biopsySequenceNumber': biopsy_sequence_num}
                    obj['result'] = message.get('result')
                    obj['biomarker'] = message.get('biomarker')
                    objs.append(obj)
                else:
                    self.log.info('assayMessages does NOT have a result, it is not a ihc_assay_report')
        return objs

    def fill_in_variant_obj(self, obj, report):
        obj['identifier'] = report.get('identifier')
        obj['chromosome'] = report.get('chromosome')
        obj['exon'] = report.get('exon')
        obj['position'] = report.get('position')
        obj['reference'] = report.get('reference')
        obj['alternative'] = report.get('alternative')
        obj['alleleFrequency'] = report.get('alleleFrequency')
        obj['transcript'] = report.get('transcript')
        obj['hgvs'] = report['hgvs'] if 'hgvs' in report else report.get('genomicHgvs')
        obj['oncominevariantclass'] = report.get('oncominevariantclass')
        obj['function'] = report.get('function')
        obj['protein'] = report.get('protein')

    def extract_variants(self, data):
        snv_variants = []
        delins_variants = []
        indel_variants = []
        copy_number_variants = []
        gene_fusion_variants = []
        for biopsy in data['biopsies']:
            biopsy_sequence_num = biopsy['biopsySequenceNumber']
            for sequence in biopsy.get('nextGenerationSequences', []):
                status = sequence['status']
                if status == 'CONFIRMED':
                    variant_report = sequence.get('ionReporterResults', {}).get('variantReport', {})
                    for report in variant_report.get('singleNucleotideVariants', []):
                        obj = {}
                        self.fill_in_variant_obj(obj, report)
                        snv_variants.append(obj)
                    for report in variant_report.get('indels', []):
                        obj = {}
                        self.fill_in_variant_obj(obj, report)
                        indel_variants.append(obj)
                    # TODO: delins_variant doesn't exists in variant report
                    if 'delins_variant' in variant_report:
                        self.log.warning('delins_variant actually exists!')
                        for report in variant_report.get('delins_variant', []):
                            obj = {}
                            self.fill_in_variant_obj(obj, report)
                            delins_variants.append(obj)

                    for report in variant_report.get('copyNumberVariants', []):
                        obj = {}
                        obj['identifier'] = report.get('identifier')
                        obj['gene'] = report.get('gene')
                        obj['chromosome'] = report.get('chromosome')
                        obj['copyNumber'] = report.get('copyNumber')
                        obj['confidenceInterval5percent'] = report.get('confidenceInterval5percent')
                        obj['confidenceInterval95percent'] = report.get('confidenceInterval95percent')
                        obj['oncominevariantclass'] = report.get('oncominevariantclass')
                        obj['cancerGeneType'] = report.get('cancerGeneType')

                        copy_number_variants.append(obj)

                    for report in variant_report.get('unifiedGeneFusions', []):
                        obj = {}
                        obj['identifier'] = report.get('identifier')
                        obj['partnerGene'] = report.get('partnerGene')
                        obj['partnerReadCount'] = report.get('partnerReadCount')
                        obj['driverGene'] = report.get('driverGene')
                        obj['driverReadCount'] = report.get('driverReadCount')
                        obj['oncominevariantclass'] = report.get('oncominevariantclass')

                        gene_fusion_variants.append(obj)

                else:
                    self.log.info('Sequence skipped, status: {}'.format(status))
        return (snv_variants, delins_variants, indel_variants, copy_number_variants, gene_fusion_variants)

    def write_files(self):
        for node in self.nodes.keys():
            file_name = 'tmp/{}.csv'.format(node)
            with open(file_name, 'w') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=self.fields[node])
                writer.writeheader()

                for obj in self.nodes[node]:
                    writer.writerow(obj)


    def prepare(self):
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

        self.nodes['nucleic_acid'] = []
        self.fields['nucleic_acid'] = [
            'biopsySequenceNumber',
            'molecularSequenceNumber',
            'dnaConcentration',
            'dnaVolume'
        ]

        self.nodes['ihc_assay_report'] = []
        self.fields['ihc_assay_report'] = [
            'biopsySequenceNumber',
            'biomarker',
            'result'
        ]

        self.nodes['snv_variant'] = []
        self.fields['snv_variant'] = [
            "identifier",
            "gene",
            "chromosome",
            "exon",
            "position",
            "reference",
            "alternative",
            "alleleFrequency",
            "transcript",
            "hgvs",
            "oncominevariantclass",
            "function",
            "protein"
        ]


        self.nodes['delins_variant'] = []
        self.fields['delins_variant'] = [
            "identifier",
            "gene",
            "chromosome",
            "exon",
            "position",
            "reference",
            "alternative",
            "alleleFrequency",
            "transcript",
            "hgvs",
            "oncominevariantclass",
            "function",
            "protein"
        ]

        self.nodes['indel_variant'] = []
        self.fields['indel_variant'] = [
            "identifier",
            "gene",
            "chromosome",
            "exon",
            "position",
            "reference",
            "alternative",
            "alleleFrequency",
            "transcript",
            "hgvs",
            "oncominevariantclass",
            "function",
            "protein"
        ]

        self.nodes['copy_number_variant'] = []
        self.fields['copy_number_variant'] = [
            "identifier",
            "gene",
            "chromosome",
            "copyNumber",
            "confidenceInterval5percent",
            "confidenceInterval95percent",
            "oncominevariantclass",
            "cancerGeneType"
        ]

        self.nodes['gene_fusion_variant'] = []
        self.fields['gene_fusion_variant'] = [
            "identifier",
            "partnerGene",
            "partnerReadCount",
            "driverGene",
            "driverReadCount",
            "oncominevariantclass"
        ]

    def extract(self):
        self.prepare()
        # Read Secrets from AWS Secrets Manager
        secrets = get_secret(self.config.region, self.config.secret_name)
        self.log.info('Secrets Read')
        # Retrieve the Okta Token
        token = get_okta_token(secrets, self.config.data, self.config.oktaAuthUrl)
        self.log.info('Token Obtained')
        # Get the List of Patients for Each Arm
        for armID in self.config.armIds:
            patientsListbyArm = getPatientsByTreatmentArm([armID], token, self.config.matchBaseUrl)
            self.log.info('List of Patients by Arm received')
            for patients in patientsListbyArm:
                for p in patients:
                    data = get_patient_meta_data(token, self.config.matchBaseUrlPatient, p)
                    data[ARM_ID] = armID
                    self.nodes['case'].extend(self.extract_case(data))
                    self.nodes['specimen'].extend(self.extract_specimen(data))
                    self.nodes['nucleic_acid'].extend(self.extract_nucleic_acid(data))
                    self.nodes['ihc_assay_report'].extend(self.extract_ihc_assay_report(data))
                    (snv_variants, delins_variants, indel_variants, copy_number_variants, gene_fusion_variants) \
                    = self.extract_variants(data)
                    self.nodes['snv_variant'].extend(snv_variants)
                    self.nodes['delins_variant'].extend(delins_variants)
                    self.nodes['indel_variant'].extend(indel_variants)
                    self.nodes['copy_number_variant'].extend(copy_number_variants)
                    self.nodes['gene_fusion_variant'].extend(gene_fusion_variants)

        self.write_files()


if __name__ == '__main__':
    config_file = os.environ.get(CONFIG_FILE_ENVVAR, 'config/config.json')
    config = Config(config_file)

    meta_data = MetaData(config)
    meta_data.extract()