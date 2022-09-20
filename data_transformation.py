from bento.common.utils import get_logger
from transformation_script.arm import arm_transformation
from transformation_script.assignment_report_file import assignment_report_transformation
from transformation_script.case import case_transformation
from transformation_script.copy_number_variant import copy_number_variant_transformation
from transformation_script.gene_fusion_variant import gene_fusion_variant_transformation
from transformation_script.ihc_assay_report import ihc_assay_report_transformation
from transformation_script.indel_variant import indel_variant_transformation
from transformation_script.nucleic_acid import nucleic_acid_transformation
from transformation_script.sequencing_assay import sequencing_assay_transformation
from transformation_script.snv_variant import snv_variant_transformation
from transformation_script.specimen import specimen_transformation
from transformation_script.variant_report import variant_report_transformation
import json
import argparse
import os


parser = argparse.ArgumentParser(description='Data Transformation')
parser.add_argument("config_file", help="Name of Configuration File to run the Data Transformation")
args = parser.parse_args()

with open(args.config_file) as config_file:
    data = json.load(config_file)
input_folder = data['input_folder']
output_folder = data['output_folder']
input_files = data['input_files']


LOGER_NAME = 'CTDC Data Transformation'
log = get_logger(LOGER_NAME)

arm_transformation(os.path.join(input_folder, input_files['arm']), log, input_files, output_folder)
assignment_report_transformation(os.path.join(input_folder, input_files["assignment_report"]), log, input_files, output_folder)
case_transformation(os.path.join(input_folder, input_files['case']), log, input_files, output_folder)
copy_number_variant_transformation(os.path.join(input_folder, input_files['copy_number_variant']), log, input_files, output_folder)
gene_fusion_variant_transformation(os.path.join(input_folder, input_files['gene_fusion_variant']), log, input_files, output_folder)
ihc_assay_report_transformation(os.path.join(input_folder, input_files['ihc_assay_report']), log, input_files, output_folder)
indel_variant_transformation(os.path.join(input_folder, input_files['indel_variant']), log, input_files, output_folder)
nucleic_acid_transformation(os.path.join(input_folder, input_files['nucleic_acid']), log, input_files, output_folder)
sequencing_assay_transformation(os.path.join(input_folder, input_files['sequencing_assay']), log, input_files, output_folder)
snv_variant_transformation(os.path.join(input_folder, input_files['snv_variant']), log, input_files, output_folder)
specimen_transformation(os.path.join(input_folder, input_files['specimen']), log, input_files, output_folder)
variant_report_transformation(os.path.join(input_folder, input_files['variant_report']), log, input_files, output_folder)