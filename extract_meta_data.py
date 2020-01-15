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
            obj['diseases'] = diseases[0].get('shortName', '')
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
                if type == 'NUCLEIC_ACID_SENDOUT':
                    obj = {'biopsySequenceNumber': message.get('biopsySequenceNumber')}
                    obj['molecularSequenceNumber'] = message.get('molecularSequenceNumber')
                    obj['dnaConcentration'] = message.get('dnaConcentration')
                    obj['dnaVolume'] = message.get('dnaVolume')
                    necleic_acids.append(obj)
                elif type == 'SPECIMEN_RECEIVED':
                    obj = {'biopsySequenceNumber': message.get('biopsySequenceNumber')}
                    obj['patientSequenceNumber'] = message.get('patientSequenceNumber')
                    speicmens.append(obj)
                else:
                    self.log.debug('mdAndersonMessages is not a nucleic_acid or specimen')
        return (necleic_acids, speicmens)

    def extract_ihc_assay_report(self, data):
        objs = []
        # There are almost identical information in "patient_assignments/*/assayMessages"
        for biopsy in data.get('biopsies', []):
            for message in biopsy.get('assayMessages', []):
                obj = {'biopsySequenceNumber': message.get('biopsySequenceNumber')}
                obj['result'] = message.get('result')
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
                    control_panel = ion_result.get('oncomineControlPanel', {})
                    copy_number_rpt = ion_result.get('copyNumberReport', {})
                    variant_rpt = {}
                    variant_rpt['molecularSequenceNumber'] = ion_result.get('molecularSequenceNumber')
                    variant_rpt['biopsySequenceNumber'] = copy_number_rpt.get('biopsySequenceNumber')
                    variant_rpt['jobName'] = ion_result.get('jobName')
                    variant_rpt['mapd'] = copy_number_rpt.get('mapd')
                    variant_rpt['cellularity'] = copy_number_rpt.get('cellularity')
                    variant_rpt['tvc_version'] = control_panel.get('tvc_version')
                    variant_reports.append(variant_rpt)

                    assay = {}
                    assay['molecularSequenceNumber'] = ion_result.get('molecularSequenceNumber')
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
        obj['alleleFrequency'] = report.get('alleleFrequency')
        obj['transcript'] = report.get('transcript')
        obj['hgvs'] = report.get('hgvs')
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
            for sequence in biopsy.get('nextGenerationSequences', []):
                status = sequence['status']
                if status == 'CONFIRMED':
                    variant_report = sequence.get('ionReporterResults', {}).get('variantReport', {})
                    molecular_sn = sequence.get('ionReporterResults', {}).get('molecularSequenceNumber')
                    for report in variant_report.get('singleNucleotideVariants', []):
                        obj = {'molecularSequenceNumber': molecular_sn}
                        self.fill_in_variant_obj(obj, report)
                        snv_variants.append(obj)
                    for report in variant_report.get('indels', []):
                        obj = {'molecularSequenceNumber': molecular_sn}
                        self.fill_in_variant_obj(obj, report)
                        indel_variants.append(obj)
                    # TODO: delins_variant doesn't exists in variant report
                    if 'delins_variant' in variant_report:
                        self.log.warning('delins_variant actually exists!')
                        for report in variant_report.get('delins_variant', []):
                            obj = {'molecularSequenceNumber': molecular_sn}
                            self.fill_in_variant_obj(obj, report)
                            delins_variants.append(obj)

                    for report in variant_report.get('copyNumberVariants', []):
                        obj = {'molecularSequenceNumber': molecular_sn}
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
                        obj = {'molecularSequenceNumber': molecular_sn}
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

    def extract_assignment_report(self, data):
        objs = []
        for assignment in data.get('patientAssignments', []):
            if assignment.get('patientAssignmentStatus') != 'NO_ARM_ASSIGNED':
                treatment_arm = assignment.get('treatmentArm')
                if treatment_arm:
                    arm_id = treatment_arm.get('treatmentArmId')
                    if arm_id:
                        obj = {ARM_ID: arm_id}
                        obj['patientSequenceNumber'] = data.get('patientSequenceNumber')
                        # Todo: assignmentStatusOutcome seems not exactly the value we want
                        obj['assignmentStatusOutcome'] = data.get('currentPatientStatus')
                        obj['stepNumber'] = assignment.get('stepNumber')

                        arms = assignment.get('patientAssignmentLogic', [])
                        for arm in arms:
                            if arm['treatmentArmId'] == obj[ARM_ID]:
                                obj['patientAssignmentLogic'] = arm.get('reason')

                        # jobName seems not really belongs to assignment_report
                        biopsy_sn = assignment.get('biopsySequenceNumber')
                        obj['biopsySequenceNumber'] = biopsy_sn
                        for biopsy in data.get('biopsies', []):
                            if biopsy_sn == biopsy.get('biopsySequenceNumber'):
                                for sequence in biopsy.get('nextGenerationSequences'):
                                    if sequence.get('status') == 'CONFIRMED':
                                        obj['jobName'] = sequence.get('ionReporterResults', {}).get('jobName')
                        objs.append(obj)
                    else:
                        self.log.error('Assignment with arm but no arm_id!')
                else:
                    self.log.warning('Assignment without treatmentArm!')
        return objs


    def write_files(self):
        for node in self.nodes.keys():
            file_name = 'tmp/{}.csv'.format(node)
            self.log.info('Writing {} data into "{}"'.format(node, file_name))
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
            "molecularSequenceNumber",
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
            "molecularSequenceNumber",
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
            "molecularSequenceNumber",
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
            "molecularSequenceNumber",
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
            "molecularSequenceNumber",
            "identifier",
            "partnerGene",
            "partnerReadCount",
            "driverGene",
            "driverReadCount",
            "oncominevariantclass"
        ]

        self.nodes['assignment_report'] = []
        self.fields['assignment_report'] = [
            ARM_ID,
            "patientSequenceNumber",
            "biopsySequenceNumber",
            "jobName",
            "stepNumber",
            "patientAssignmentLogic",
            "assignmentStatusOutcome"
        ]

        self.nodes['variant_report'] = []
        self.fields['variant_report'] = [
            "biopsySequenceNumber",
            'molecularSequenceNumber',
            "jobName",
            "mapd",
            "cellularity",
            "tvc_version"
        ]

        self.nodes['sequencing_assay'] = []
        self.fields['sequencing_assay'] = [
            'molecularSequenceNumber',
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
            patientsListbyArm = getPatientsByTreatmentArm([armID], token, self.config.matchBaseUrl)
            self.log.info('List of Patients by Arm received')
            for patients in patientsListbyArm:
                for p in patients:
                    data = get_patient_meta_data(token, self.config.matchBaseUrlPatient, p)
                    data[ARM_ID] = armID
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

        self.write_files()


if __name__ == '__main__':
    config_file = os.environ.get(CONFIG_FILE_ENVVAR, 'config/config.json')
    config = Config(config_file)

    meta_data = MetaData(config)
    meta_data.extract()