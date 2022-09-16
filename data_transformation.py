from bento.common.utils import get_logger
from transformation_script.arm import arm_transformation
from transformation_script.assignment_report import assignment_report_transformation
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
from config import Config
import argparse


parser = argparse.ArgumentParser(description='Data Transformation')
parser.add_argument("config_file", help="Name of Configuration File to run the Data Transformation")
args = parser.parse_args()

config = Config(args.config_file)
input_folder = config.input_folder
input_files = config.input_files

LOGER_NAME = 'CTDC Data Transformation'
log = get_logger(LOGER_NAME)

arm_transformation(input_folder + input_files['arm'], log, config)
assignment_report_transformation(input_folder + input_files["assignment_report"], log,config)
case_transformation(input_folder + input_files['case'], log, config)
copy_number_variant_transformation(input_folder + input_files['copy_number_variant'], log, config)
gene_fusion_variant_transformation(input_folder + input_files['gene_fusion_variant'], log, config)
ihc_assay_report_transformation(input_folder + input_files['ihc_assay_report'], log, config)
indel_variant_transformation(input_folder + input_files['indel_variant'], log, config)
nucleic_acid_transformation(input_folder + input_files['nucleic_acid'], log, config)
sequencing_assay_transformation(input_folder + input_files['sequencing_assay'], log, config)
snv_variant_transformation(input_folder + input_files['snv_variant'], log, config)
specimen_transformation(input_folder + input_files['specimen'], log, config)
variant_report_transformation('transformation_script/raw/variant_report.csv', log, config)