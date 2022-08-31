import pandas as pd
from transformation_script.property_function import rename_properties

def variant_report_transformation(variant_report_file_name, log):
    log.info('Transforming variant_report.csv')
    variant_report_df = pd.read_csv(variant_report_file_name)
    sequencing_assay_id = []
    variant_report_id = []
    mapd = []
    cellularity = []
    variant_report_df['reference_genome'] = ['GRCh37'] * len(variant_report_df)
    variant_report_df['show_node'] = [True] * len(variant_report_df)
    for index in range(len(variant_report_df)):
        sequencing_assay_id.append('CTDC-SEQ-' + variant_report_df['sequencing_assay.molecularSequenceNumber'].iloc[index])
        variant_report_id.append('CTDC-VAR-REP-' + variant_report_df['jobName'].iloc[index])
        mapd.append(round(variant_report_df['mapd'].iloc[index], 1))
        if pd.isna(variant_report_df['cellularity'].iloc[index]):
            cellularity.append(variant_report_df['cellularity'].iloc[index])
        else:
            cellularity.append(int(variant_report_df['cellularity'].iloc[index]))
    variant_report_df['sequencing_assay.molecularSequenceNumber'] = sequencing_assay_id
    variant_report_df['variant_report_id'] = variant_report_id
    variant_report_df['mapd']= mapd
    variant_report_df['cellularity']= cellularity
    property = [
        {'old':'sequencing_assay.molecularSequenceNumber', 'new':'sequencing_assay.sequencing_assay_id'},
        {'old':'jobName', 'new':'analysis_id'},
        {'old':'tvc_version', 'new':'torrent_variant_caller_version'}
    ]
    variant_report_df = rename_properties(variant_report_df, property)
    variant_report_df.to_csv('transformation_script/variant_report.tsv', sep = "\t", index = False)

