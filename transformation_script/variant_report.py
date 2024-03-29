import pandas as pd
from transformation_script.property_function import rename_properties, remove_trailing_zero
import os

def variant_report_transformation(variant_report_file_name, log, input_files, output_folder):
    log.info('Transforming variant_report.csv')
    variant_report_df = pd.read_csv(variant_report_file_name)
    sequencing_assay_id = []
    variant_report_id = []
    #mapd = []
    variant_report_df['reference_genome'] = ['GRCh37'] * len(variant_report_df)
    variant_report_df['show_node'] = ['TRUE'] * len(variant_report_df)
    for index in range(len(variant_report_df)):
        sequencing_assay_id.append('CTDC-SEQ-' + variant_report_df['sequencing_assay.molecularSequenceNumber'].iloc[index])
        variant_report_id.append('CTDC-VAR-REP-' + variant_report_df['jobName'].iloc[index])
        #mapd.append(round(variant_report_df['mapd'].iloc[index], 1))
    variant_report_df['sequencing_assay.molecularSequenceNumber'] = sequencing_assay_id
    variant_report_df['variant_report_id'] = variant_report_id
    #variant_report_df['mapd']= mapd
    property = [
        {'old':'sequencing_assay.molecularSequenceNumber', 'new':'sequencing_assay.sequencing_assay_id'},
        {'old':'jobName', 'new':'analysis_id'},
        {'old':'tvc_version', 'new':'torrent_variant_caller_version'}
    ]
    variant_report_df = rename_properties(variant_report_df, property)
    variant_report_df = variant_report_df.reindex(columns=['type', 'show_node', 'sequencing_assay.sequencing_assay_id', 'variant_report_id',
        'analysis_id', 'mapd', 'cellularity', 'torrent_variant_caller_version', 'reference_genome'])
    variant_report_df['cellularity'] = remove_trailing_zero(variant_report_df['cellularity'])

    input_file_name = os.path.splitext(input_files['variant_report'])[0]
    output_file = os.path.join(output_folder, input_file_name + ".tsv")
    variant_report_df.to_csv(output_file, sep = "\t", index = False)

