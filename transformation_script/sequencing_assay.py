import pandas as pd
from transformation_script.property_function import rename_properties

def sequencing_assay_transformation(sequencing_assay_file_name, log, config):
    log.info('Transofrming sequencing_assay.csv')
    sequencing_assay_df = pd.read_csv(sequencing_assay_file_name)
    sequencing_assay_df['show_node'] = ['TRUE'] * len(sequencing_assay_df)
    sequencing_assay_df['platform'] = ['Ion Torrent'] * len(sequencing_assay_df)
    sequencing_assay_df['experimental_method'] = ['Targeted NGS'] * len(sequencing_assay_df)
    sequencing_assay_id = []
    aliquot_id = []
    for index in range(len(sequencing_assay_df)):
        aliquot_id.append('CTDC-NA-' + sequencing_assay_df['nucleic_acid.molecularSequenceNumber'].iloc[index])
        sequencing_assay_id.append('CTDC-SEQ-' + sequencing_assay_df['nucleic_acid.molecularSequenceNumber'].iloc[index])
    sequencing_assay_df['nucleic_acid.molecularSequenceNumber'] = aliquot_id
    sequencing_assay_df['sequencing_assay_id'] = sequencing_assay_id
    property = [
        {'old':'nucleic_acid.molecularSequenceNumber', 'new':'nucleic_acid.aliquot_id'}
    ]
    sequencing_assay_df = rename_properties(sequencing_assay_df, property)
    sequencing_assay_df = rename_properties(sequencing_assay_df, property)
    sequencing_assay_df = sequencing_assay_df.reindex(columns=['type', 'show_node', 'nucleic_acid.aliquot_id', 'sequencing_assay_id', 'qc_result', 'platform', 'experimental_method'])

    input_file_list = config.input_files['sequencing_assay'].split('.')
    output_file = config.output_folder + input_file_list[0] + ".tsv"
    sequencing_assay_df.to_csv(output_file, sep = "\t", index = False)

