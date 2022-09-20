import pandas as pd
from transformation_script.property_function import rename_properties
import os

def specimen_transformation(specimen_file_name, log, input_files, output_folder):
    log.info('Transforming specimen.csv')
    specimen_df = pd.read_csv(specimen_file_name)
    case_id = []
    specimen_id = []
    specimen_df['show_node'] = ['TRUE'] * len(specimen_df)
    specimen_df['specimen_type'] = ['Tumor'] * len(specimen_df)
    for index in range(len(specimen_df)):
        case_id.append('CTDC-' + str(specimen_df['case.patientSequenceNumber'].iloc[index]))
        specimen_id.append('CTDC-SP-' + specimen_df['biopsySequenceNumber'].iloc[index])
    specimen_df['case.patientSequenceNumber'] = case_id
    specimen_df['specimen_id'] = specimen_id
    property = [
        {'old':'case.patientSequenceNumber', 'new':'case.case_id'},
        {'old':'biopsySequenceNumber', 'new':'biopsy_sequence_number'}
    ]
    specimen_df = rename_properties(specimen_df, property)
    specimen_df = specimen_df.reindex(columns=['type', 'show_node', 'case.case_id', 'specimen_id', 'biopsy_sequence_number', 'specimen_type'])

    input_file_name = os.path.splitext(input_files['specimen'])[0]
    output_file = os.path.join(output_folder, input_file_name + ".tsv")
    specimen_df.to_csv(output_file, sep = "\t", index = False)

