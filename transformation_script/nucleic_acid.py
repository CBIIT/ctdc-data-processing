import pandas as pd
from transformation_script.property_function import rename_properties

def nucleic_acid_transformation(nucleic_acid_file_name, log):
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
    nucleic_acid_df.to_csv('transformation_script/nucleic_acid.tsv', sep = "\t", index = False)

