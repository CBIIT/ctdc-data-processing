import pandas as pd
import hashlib
from transformation_script.property_function import rename_properties


def copy_number_variant_transformation(copy_number_variant_file_name, log):
    log.info('Transforming copy_number_variant.csv')
    copy_number_variant_df = pd.read_csv(copy_number_variant_file_name)
    copy_number_variant_df['show_node'] = [True] * len(copy_number_variant_df)
    variant_report_id = []
    variant_id = []
    #new_copyNumber = []
    #new_confidenceInterval5percent = []
    #new_confidenceInterval95percent = []
    for index in range(len(copy_number_variant_df)):
        variant_report_id.append('CTDC-VAR-REP-' + str(copy_number_variant_df['variant_report.jobName'].iloc[index]))
        variant_id_value = str(copy_number_variant_df['identifier'].iloc[index])
        variant_id_md5 = str(hashlib.md5(variant_id_value.encode('utf-8')).hexdigest())
        variant_id.append('CTDC-VAR-' + variant_id_md5)
        #new_copyNumber.append(round(copy_number_variant_df['copy_number_variant_of$copyNumber'].iloc[index], 1))
        #new_confidenceInterval5percent.append(round(copy_number_variant_df['copy_number_variant_of$confidenceInterval5percent'].iloc[index], 1))
        #new_confidenceInterval95percent.append(round(copy_number_variant_df['copy_number_variant_of$confidenceInterval95percent'].iloc[index], 1))
    copy_number_variant_df['variant_report.jobName'] = variant_report_id
    copy_number_variant_df['variant_id'] = variant_id
    #copy_number_variant_df['copy_number_variant_of$copyNumber'] = new_copyNumber
    #copy_number_variant_df['copy_number_variant_of$confidenceInterval5percent'] = new_confidenceInterval5percent
    #copy_number_variant_df['copy_number_variant_of$confidenceInterval95percent'] = new_confidenceInterval95percent
    property = [
        {'old':'variant_report.jobName', 'new':'variant_report.variant_report_id'},
        {'old':'identifier', 'new':'external_variant_id'},
        {'old':'oncominevariantclass', 'new':'oncomine_variant_class'},
        {'old':'cancerGeneType', 'new':'tumor_suppressor'},
        {'old':'copy_number_variant_of$confidenceInterval5percent', 'new':'copy_number_variant_of$copy_number_ci_5'},
        {'old':'copy_number_variant_of$confidenceInterval95percent', 'new':'copy_number_variant_of$copy_number_ci_95'},
        {'old':'copy_number_variant_of$copyNumber', 'new':'copy_number_variant_of$copy_number'}
    ]
    copy_number_variant_df = rename_properties(copy_number_variant_df, property)
    copy_number_variant_df = copy_number_variant_df.reindex(columns=['type', 'show_node', 'variant_report.variant_report_id', 'variant_id', 'external_variant_id',
        'gene', 'chromosome', 'copy_number_variant_of$copy_number', 'copy_number_variant_of$copy_number_ci_5', 'copy_number_variant_of$copy_number_ci_95', 'oncomine_variant_class', 'tumor_suppressor'])
    copy_number_variant_df.to_csv('transformation_script/copy_number_variant.tsv', sep = "\t", index = False)

