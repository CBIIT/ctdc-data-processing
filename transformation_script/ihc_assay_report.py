import pandas as pd
from transformation_script.property_function import rename_properties
import os

def ihc_assay_report_transformation(ihc_assay_report_file_name, log, input_files, output_folder):
    log.info('Transforming ihc_assay_report.csv')
    ihc_assay_report_df = pd.read_csv(ihc_assay_report_file_name)
    ihc_assay_report_df['show_node'] = ['TRUE'] * len(ihc_assay_report_df)
    specimen_id = []
    ihc_assay_id = []
    ihc_test_gene = []
    ihc_test_result = []
    for index in range(len(ihc_assay_report_df)):
        biomarker = str(ihc_assay_report_df['biomarker'].iloc[index])
        new_biomarker = biomarker[3:len(biomarker)-1]
        biopsySequenceNumber = str(ihc_assay_report_df['specimen.biopsySequenceNumber'].iloc[index])
        specimen_id.append('CTDC-SP-' + biopsySequenceNumber)
        ihc_assay_id.append('CTDC-ASSAY-' + biopsySequenceNumber + new_biomarker)
        ihc_test_gene.append(new_biomarker)
        if ihc_assay_report_df['result'].iloc[index] == 'NEGATIVE':
            ihc_test_result.append('LOST')
        elif ihc_assay_report_df['result'].iloc[index] == 'POSITIVE':
            ihc_test_result.append('EXPRESSED')
        else:
            ihc_test_result.append(ihc_assay_report_df['result'].iloc[index])

    ihc_assay_report_df['specimen.biopsySequenceNumber'] = specimen_id
    ihc_assay_report_df['biomarker'] = ihc_test_gene
    ihc_assay_report_df['result'] = ihc_test_result
    ihc_assay_report_df['ihc_assay_id'] = ihc_assay_id
    property = [
        {'old':'specimen.biopsySequenceNumber', 'new':'specimen.specimen_id'},
        {'old':'biomarker', 'new':'ihc_test_gene'},
        {'old':'result', 'new':'ihc_test_result'}
    ]
    ihc_assay_report_df = rename_properties(ihc_assay_report_df, property)
    ihc_assay_report_df = ihc_assay_report_df.reindex(columns=['type', 'show_node', 'specimen.specimen_id', 'ihc_assay_id', 'ihc_test_gene', 'ihc_test_result'])

    input_file_name = os.path.splitext(input_files['ihc_assay_report'])[0]
    output_file = os.path.join(output_folder, input_file_name + ".tsv")
    ihc_assay_report_df.to_csv(output_file, sep = "\t", index = False)


