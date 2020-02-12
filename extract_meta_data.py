import argparse
import csv
import os
import re

from bento.common.tokens import get_okta_token

from bento.common.utils import get_logger
from config import Config
from meta_data import get_patient_meta_data
from bento.common.secrets import get_secret
from treatmentarm import get_patients_for_arm
from bento.common.s3 import S3Bucket
from bento.common.simple_cipher import SimpleCipher

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
        assert isinstance(config, Config)
        self.config = config
        self.fields = {}
        self.bucket = S3Bucket(self.config.metaDataBucket)
        self.cipher = SimpleCipher(self.config.cipher_key)

    @staticmethod
    def get_prior_drugs(desc):
        drugs = []
        if isinstance(desc, list):
            for drug in desc:
                drugs.append(drug[DRUGS][0][DRUG_NAME])
        return DELIMITER.join(drugs)

    @staticmethod
    def get_fields(self, props):
        fields = [k for k in props.keys()]
        fields.append(ARM_ID)
        return fields

    @staticmethod
    def join_field_in_objects(objects, field, delimiter):
        return delimiter.join([obj[field] for obj in objects])


    def extract_case(self, data):
        obj = {
            'type': 'case',
            'arm.arm_id': data[ARM_ID]
        }
        obj['patientSequenceNumber'] = self.cipher.simple_cipher(data.get('patientSequenceNumber'))
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
            self.log.error('wrong number ({}) of diseases!'.format(len(diseases)))

        obj['priorDrugs'] = self.get_prior_drugs(data.get('priorDrugs', []))
        return [obj]

    def extract_specimen_n_nucleic_acid(self, data):
        necleic_acids = []
        speicmens = []
        for biopsy in data.get('biopsies', []):
            for message in biopsy.get('mdAndersonMessages', []):
                type = message.get('message')
                status = message.get('status', '')
                if status == 'REJECTED':
                    # Ignore rejected nucleic_acid or specimen
                    self.log.info('Ignore nucleic_acid/specimen with "REJECTED" status')
                    continue
                if type == 'NUCLEIC_ACID_SENDOUT':
                    obj = {
                        'type': 'nucleic_acid',
                        'specimen.biopsySequenceNumber': message.get('biopsySequenceNumber')
                    }
                    obj['molecularSequenceNumber'] = message.get('molecularSequenceNumber')
                    obj['dnaConcentration'] = message.get('dnaConcentration')
                    obj['dnaVolume'] = message.get('dnaVolume')
                    necleic_acids.append(obj)
                elif type == 'SPECIMEN_RECEIVED':
                    obj = {
                        'type': 'specimen',
                        'biopsySequenceNumber': message.get('biopsySequenceNumber')
                    }
                    obj['case.patientSequenceNumber'] = self.cipher.simple_cipher(message.get('patientSequenceNumber'))
                    speicmens.append(obj)
                else:
                    self.log.debug('mdAndersonMessages is not a nucleic_acid or specimen')
        return (necleic_acids, speicmens)

    def extract_ihc_assay_report(self, data):
        objs = []
        # There are almost identical information in "patient_assignments/*/assayMessages"
        for biopsy in data.get('biopsies', []):
            for message in biopsy.get('assayMessages', []):
                if 'result' not in message:
                    self.log.debug('IHC report without result, ignored!')
                    continue
                if 'reportedDate' not in message:
                    self.log.warning('IHC report missing result date!')

                obj = {
                    'type': 'ihc_assay_report',
                    'specimen.biopsySequenceNumber': message.get('biopsySequenceNumber')
                }
                obj['result'] = message.get('result', 'UNKNOWN')
                obj['biomarker'] = message.get('biomarker')
                objs.append(obj)
        return objs

    def extract_variant_report_n_sequencing_assay(self, data):
        variant_reports = []
        sequencing_assays = []
        for biopsy in data['biopsies']:
            for sequence in biopsy.get('nextGenerationSequences', []):
                if sequence.get('status') == 'CONFIRMED':
                    ion_result = sequence.get('ionReporterResults', {})
                    copy_number_rpt = ion_result.get('copyNumberReport', {})
                    variant_rpt = { 'type': 'variant_report' }
                    variant_rpt['sequencing_assay.molecularSequenceNumber'] = copy_number_rpt.get('molecularSequenceNumber', ion_result.get('molecularSequenceNumber'))
                    variant_rpt['jobName'] = ion_result.get('jobName')
                    variant_rpt['mapd'] = copy_number_rpt.get('mapd')
                    variant_rpt['cellularity'] = copy_number_rpt.get('cellularity')
                    variant_rpt['tvc_version'] = copy_number_rpt.get('tvc_version')
                    variant_reports.append(variant_rpt)

                    control_panel = ion_result.get('oncomineControlPanel', {})
                    assay = { 'type': 'sequencing_assay' }
                    molecular_sn = control_panel.get('molecularSequenceNumber')
                    if not molecular_sn:
                        molecular_sn = ion_result.get('molecularSequenceNumber')
                    assay['nucleic_acid.molecularSequenceNumber'] = molecular_sn
                    qc_rsts = []
                    for gene, value in control_panel.get('genes', {}).items():
                        qc_rsts.append('{}: {}'.format(gene, value))
                    assay['qc_result'] = '; '.join(qc_rsts)
                    sequencing_assays.append(assay)
        return (variant_reports, sequencing_assays)

    def fill_in_variant_obj(self, obj, report):
        obj['identifier'] = report.get('identifier')
        obj['gene'] = report.get('gene')
        obj['chromosome'] = report.get('chromosome')
        obj['exon'] = report.get('exon')
        obj['position'] = report.get('position')
        obj['reference'] = report.get('reference')
        obj['alternative'] = report.get('alternative')
        obj['transcript'] = report.get('transcript')
        obj['hgvs'] = report.get('hgvs', '')
        obj['oncominevariantclass'] = report.get('oncominevariantclass')
        obj['function'] = report.get('function')
        obj['protein'] = report.get('protein')

    @staticmethod
    def is_del_ins_variant(obj):
        hgvs = obj.get('hgvs', '')
        return re.search(r'del[ATGCN]*ins[ATGCN]+$', hgvs)

    def extract_variants(self, data):
        snv_variants = []
        delins_variants = []
        indel_variants = []
        copy_number_variants = []
        gene_fusion_variants = []
        for biopsy in data['biopsies']:
            for sequence in biopsy.get('nextGenerationSequences', []):
                status = sequence['status']
                if status == 'CONFIRMED':
                    variant_report = sequence.get('ionReporterResults', {}).get('variantReport', {})
                    job_name = sequence.get('ionReporterResults', {}).get('jobName')
                    for report in variant_report.get('singleNucleotideVariants', []):
                        obj = {
                            'type': 'snv_variant',
                            'variant_report.jobName': job_name
                        }
                        self.fill_in_variant_obj(obj, report)
                        if self.is_del_ins_variant(obj):
                            self.log.info(f'Found delins variant in a snv invariant! hgvs: {obj["hgvs"]}')
                            obj['type'] = 'delins_variant'
                            obj['delins_variant_of$alleleFrequency'] = report.get('alleleFrequency')
                            delins_variants.append(obj)
                        else:
                            obj['snv_variant_of$alleleFrequency'] = report.get('alleleFrequency')
                            snv_variants.append(obj)
                    for report in variant_report.get('indels', []):
                        obj = {
                            'type': 'indel_variant',
                            'variant_report.jobName': job_name
                        }
                        self.fill_in_variant_obj(obj, report)
                        if self.is_del_ins_variant(obj):
                            self.log.info(f'Found delins variant in an indel invariant! hgvs: {obj["hgvs"]}')
                            obj['type'] = 'delins_variant'
                            obj['delins_variant_of$alleleFrequency'] = report.get('alleleFrequency')
                            delins_variants.append(obj)
                        else:
                            obj['indel_variant_of$alleleFrequency'] = report.get('alleleFrequency')
                            indel_variants.append(obj)

                    for report in variant_report.get('copyNumberVariants', []):
                        obj = {
                            'type': 'copy_number_variant',
                            'variant_report.jobName': job_name
                        }
                        obj['identifier'] = report.get('identifier')
                        obj['gene'] = report.get('gene')
                        obj['chromosome'] = report.get('chromosome')
                        obj['copy_number_variant_of$copyNumber'] = report.get('copyNumber')
                        obj['copy_number_variant_of$confidenceInterval5percent'] = report.get('confidenceInterval5percent')
                        obj['copy_number_variant_of$confidenceInterval95percent'] = report.get('confidenceInterval95percent')
                        obj['oncominevariantclass'] = report.get('oncominevariantclass')
                        obj['cancerGeneType'] = report.get('cancerGeneType')

                        copy_number_variants.append(obj)

                    for report in variant_report.get('unifiedGeneFusions', []):
                        obj = {
                            'type': 'gene_fusion_variant',
                            'variant_report.jobName': job_name
                        }
                        obj['identifier'] = report.get('identifier')
                        obj['partnerGene'] = report.get('partnerGene')
                        obj['gene_fusion_variant_of$partnerReadCount'] = report.get('partnerReadCount')
                        obj['driverGene'] = report.get('driverGene')
                        obj['gene_fusion_variant_of$driverReadCount'] = report.get('driverReadCount')
                        obj['oncominevariantclass'] = report.get('oncominevariantclass')

                        gene_fusion_variants.append(obj)

                else:
                    self.log.info('Sequence skipped, status: {}'.format(status))
        return (snv_variants, delins_variants, indel_variants, copy_number_variants, gene_fusion_variants)

    def extract_assignment_report(self, data):
        objs = []
        for assignment in data.get('patientAssignments', []):
            if assignment.get('patientAssignmentStatus') != 'NO_ARM_ASSIGNED':
                treatment_arm = assignment.get('treatmentArm')
                if treatment_arm:
                    arm_id = treatment_arm.get('treatmentArmId')
                    if arm_id:
                        if arm_id != data['arm_id']:
                            self.log.warning(f"Patient {data.get('patientSequenceNumber')} was assigned " +
                                             f"to another arm: {arm_id}, this assignment is ignored!")
                            continue
                        obj = {
                            'type': 'assignment_report',
                            'arm.arm_id': arm_id
                        }
                        obj['assignmentStatusOutcome'] = data.get('assignmentStatusOutcome')
                        obj['patientSequenceNumber'] = self.cipher.simple_cipher(data.get('patientSequenceNumber'))
                        obj['stepNumber'] = assignment.get('stepNumber')
                        for assignment_message in assignment.get('patientAssignmentMessages', []):
                            obj['stepNumber'] = assignment_message.get('stepNumber', obj['stepNumber'])

                        arms = assignment.get('patientAssignmentLogic', [])
                        for arm in arms:
                            if arm['treatmentArmId'] == obj['arm.arm_id']:
                                obj['patientAssignmentLogic'] = arm.get('reason')

                        # jobName seems not really belongs to assignment_report
                        biopsy_sn = assignment.get('biopsySequenceNumber')
                        obj['specimen.biopsySequenceNumber'] = biopsy_sn
                        for biopsy in data.get('biopsies', []):
                            if biopsy_sn == biopsy.get('biopsySequenceNumber'):
                                for sequence in biopsy.get('nextGenerationSequences'):
                                    if sequence.get('status') == 'CONFIRMED':
                                        obj['jobName'] = sequence.get('ionReporterResults', {}).get('jobName')
                                        obj['variant_report.jobName'] = obj['jobName']
                        objs.append(obj)
                    else:
                        self.log.error('Assignment with arm but no arm_id!')
                else:
                    self.log.warning('Assignment without treatmentArm!')
        return objs


    def get_s3_key(self, file_path):
        file_name = os.path.basename(file_path)
        path = os.path.join(self.config.metaDataPath, file_name)
        return  path

    def upload_files(self, file_list):
        assert isinstance(file_list, list)
        for file in file_list:
            key = self.get_s3_key(file)
            self.log.info('Uploading {} to s3://{}{}'.format(file, self.bucket.bucket_name, key))
            obj = self.bucket.upload_file(key, file)
            if not obj:
                self.log.error('Upload {} FAILED'.format(file, self.bucket.bucket_name, key))
            else:
                if obj.get('skipped'):
                    self.log.info('File skipped.')
                else:
                    self.log.info('File uploaded.')

    def write_files(self):
        file_list = []
        for node in self.nodes.keys():
            file_name = 'tmp/{}.csv'.format(node)
            self.log.info('Writing {} data into "{}"'.format(node, file_name))
            with open(file_name, 'w') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=self.fields[node])
                writer.writeheader()

                for obj in self.nodes[node]:
                    writer.writerow(obj)
                file_list.append(file_name)
        return file_list


    def prepare(self):
        self.nodes = {}
        self.nodes['case'] = []
        self.fields['case'] = [
            'type',
            'arm.arm_id',
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
            'type',
            'case.patientSequenceNumber',
            'biopsySequenceNumber'
        ]

        self.nodes['nucleic_acid'] = []
        self.fields['nucleic_acid'] = [
            'type',
            'specimen.biopsySequenceNumber',
            'molecularSequenceNumber',
            'dnaConcentration',
            'dnaVolume'
        ]

        self.nodes['ihc_assay_report'] = []
        self.fields['ihc_assay_report'] = [
            'type',
            'specimen.biopsySequenceNumber',
            'biomarker',
            'result'
        ]

        self.nodes['snv_variant'] = []
        self.fields['snv_variant'] = [
            'type',
            "variant_report.jobName",
            "identifier",
            "gene",
            "chromosome",
            "exon",
            "position",
            "reference",
            "alternative",
            "snv_variant_of$alleleFrequency",
            "transcript",
            "hgvs",
            "oncominevariantclass",
            "function",
            "protein"
        ]


        self.nodes['delins_variant'] = []
        self.fields['delins_variant'] = [
            'type',
            "variant_report.jobName",
            "identifier",
            "gene",
            "chromosome",
            "exon",
            "position",
            "reference",
            "alternative",
            "delins_variant_of$alleleFrequency",
            "transcript",
            "hgvs",
            "oncominevariantclass",
            "function",
            "protein"
        ]

        self.nodes['indel_variant'] = []
        self.fields['indel_variant'] = [
            'type',
            "variant_report.jobName",
            "identifier",
            "gene",
            "chromosome",
            "exon",
            "position",
            "reference",
            "alternative",
            "indel_variant_of$alleleFrequency",
            "transcript",
            "hgvs",
            "oncominevariantclass",
            "function",
            "protein"
        ]

        self.nodes['copy_number_variant'] = []
        self.fields['copy_number_variant'] = [
            'type',
            "variant_report.jobName",
            "identifier",
            "gene",
            "chromosome",
            "copy_number_variant_of$copyNumber",
            "copy_number_variant_of$confidenceInterval5percent",
            "copy_number_variant_of$confidenceInterval95percent",
            "oncominevariantclass",
            "cancerGeneType"
        ]

        self.nodes['gene_fusion_variant'] = []
        self.fields['gene_fusion_variant'] = [
            'type',
            "variant_report.jobName",
            "identifier",
            "partnerGene",
            "gene_fusion_variant_of$partnerReadCount",
            "driverGene",
            "gene_fusion_variant_of$driverReadCount",
            "oncominevariantclass"
        ]

        self.nodes['assignment_report'] = []
        self.fields['assignment_report'] = [
            'type',
            'arm.arm_id',
            "patientSequenceNumber",
            "specimen.biopsySequenceNumber",
            "variant_report.jobName",
            "jobName",
            "stepNumber",
            "patientAssignmentLogic",
            "assignmentStatusOutcome"
        ]

        self.nodes['variant_report'] = []
        self.fields['variant_report'] = [
            'type',
            'sequencing_assay.molecularSequenceNumber',
            "jobName",
            "mapd",
            "cellularity",
            "tvc_version"
        ]

        self.nodes['sequencing_assay'] = []
        self.fields['sequencing_assay'] = [
            'type',
            'nucleic_acid.molecularSequenceNumber',
            "qc_result"
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
            patients = get_patients_for_arm(armID, token, self.config.matchArmUrl)
            self.log.info('List of Patients by Arm received')
            for patient_id, outcome in patients.items():
                data = get_patient_meta_data(token, self.config.matchBaseUrlPatient, patient_id)
                data[ARM_ID] = armID
                data['assignmentStatusOutcome'] = outcome
                self.nodes['case'].extend(self.extract_case(data))
                nucleic_acid_reports, speicimens = self.extract_specimen_n_nucleic_acid(data)
                self.nodes['specimen'].extend(speicimens)
                self.nodes['nucleic_acid'].extend(nucleic_acid_reports)
                self.nodes['ihc_assay_report'].extend(self.extract_ihc_assay_report(data))
                variant_reports, sequencing_assays = self.extract_variant_report_n_sequencing_assay(data)
                self.nodes['variant_report'].extend(variant_reports)
                self.nodes['sequencing_assay'].extend(sequencing_assays)
                (snv_variants, delins_variants, indel_variants, copy_number_variants, gene_fusion_variants) \
                = self.extract_variants(data)
                self.nodes['snv_variant'].extend(snv_variants)
                self.nodes['delins_variant'].extend(delins_variants)
                self.nodes['indel_variant'].extend(indel_variants)
                self.nodes['copy_number_variant'].extend(copy_number_variants)
                self.nodes['gene_fusion_variant'].extend(gene_fusion_variants)
                self.nodes['assignment_report'].extend(self.extract_assignment_report(data))

        file_list = self.write_files()
        self.upload_files(file_list)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract meta data from NCI MATCH API')
    parser.add_argument("config_file", help="Name of Configuration File to run the File Uploader")
    args = parser.parse_args()

    config = Config(args.config_file)

    meta_data = MetaData(config)
    meta_data.extract()