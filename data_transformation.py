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

LOGER_NAME = 'CTDC Data Transformation'
log = get_logger(LOGER_NAME)
arm_transformation('transformation_script/raw/arm.csv', log)
assignment_report_transformation('transformation_script/raw/assignment_report_file.csv', log)
case_transformation('transformation_script/raw/case.csv', log)
copy_number_variant_transformation('transformation_script/raw/copy_number_variant.csv', log)
gene_fusion_variant_transformation('transformation_script/raw/gene_fusion_variant.csv', log)
ihc_assay_report_transformation('transformation_script/raw/ihc_assay_report.csv', log)
indel_variant_transformation('transformation_script/raw/indel_variant.csv', log)
nucleic_acid_transformation('transformation_script/raw/nucleic_acid.csv', log)
sequencing_assay_transformation('transformation_script/raw/sequencing_assay.csv', log)
snv_variant_transformation('transformation_script/raw/snv_variant.csv', log)
specimen_transformation('transformation_script/raw/specimen.csv', log)
variant_report_transformation('transformation_script/raw/variant_report.csv', log)