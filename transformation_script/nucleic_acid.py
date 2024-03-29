import pandas as pd
from transformation_script.property_function import rename_properties, remove_trailing_zero
import os

def nucleic_acid_transformation(nucleic_acid_file_name, log, input_files, output_folder):
    log.info('Transforming nucleic_acid.csv')
    nucleic_acid_df = pd.read_csv(nucleic_acid_file_name)
    nucleic_acid_df['show_node'] = ['TRUE'] * len(nucleic_acid_df)
    nucleic_acid_df['nucleic_acid_type'] = ['Pooled DNA/cDNA'] * len(nucleic_acid_df)
    specimen_id = []
    aliquot_id = []
    for index in range(len(nucleic_acid_df)):
        specimen_id.append('CTDC-SP-' + nucleic_acid_df['specimen.biopsySequenceNumber'].iloc[index])
        aliquot_id.append('CTDC-NA-' + nucleic_acid_df['molecularSequenceNumber'].iloc[index])
    nucleic_acid_df['specimen.biopsySequenceNumber'] = specimen_id
    nucleic_acid_df['aliquot_id'] = aliquot_id

    property = [
        {'old':'specimen.biopsySequenceNumber', 'new':'specimen.specimen_id'},
        {'old':'molecularSequenceNumber', 'new':'molecular_sequence_number'},
        {'old':'dnaConcentration', 'new':'nucleic_acid_concentration'},
        {'old':'dnaVolume', 'new':'nucleic_acid_volume'}
    ]
    nucleic_acid_df = rename_properties(nucleic_acid_df, property)
    nucleic_acid_df = nucleic_acid_df.reindex(columns=['type', 'show_node', 'specimen.specimen_id', 'aliquot_id', 'molecular_sequence_number',
        'nucleic_acid_concentration', 'nucleic_acid_volume', 'nucleic_acid_type'])

    nucleic_acid_df['nucleic_acid_volume'] = remove_trailing_zero(nucleic_acid_df['nucleic_acid_volume'])
    nucleic_acid_df['nucleic_acid_concentration'] = remove_trailing_zero(nucleic_acid_df['nucleic_acid_concentration'])

    input_file_name = os.path.splitext(input_files['nucleic_acid'])[0]
    output_file = os.path.join(output_folder, input_file_name + ".tsv")
    nucleic_acid_df.to_csv(output_file, sep = "\t", index = False)

