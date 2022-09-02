import pandas as pd
import hashlib
from transformation_script.property_function import rename_properties, remove_nan

def indel_variant_transformation(indel_variant_file_name, log):
    log.info('Transforming indel_variant.csv')
    indel_variant_df = pd.read_csv(indel_variant_file_name)
    variant_report_id = []
    variant_id = []
    #alleleFrequency = []
    indel_variant_df['reference'] = remove_nan(indel_variant_df['reference'])
    indel_variant_df['alternative'] = remove_nan(indel_variant_df['alternative'])
    indel_variant_df['show_node'] = [True] * len(indel_variant_df)
    for index in range(len(indel_variant_df)):
        variant_report_id.append('CTDC-VAR-REP-' + str(indel_variant_df['variant_report.jobName'].iloc[index]))
        variant_id_value = indel_variant_df['chromosome'].iloc[index] + str(indel_variant_df['position'].iloc[index]) + str(indel_variant_df['reference'].iloc[index]) + str(indel_variant_df['alternative'].iloc[index]) + indel_variant_df['hgvs'].iloc[index]
        varidant_id_md5 = str(hashlib.md5(variant_id_value.encode('utf-8')).hexdigest())
        variant_id.append('CTDC-VAR-' + varidant_id_md5)
        #alleleFrequency.append(round(indel_variant_df['indel_variant_of$alleleFrequency'].iloc[index], 1))

    indel_variant_df['variant_report.jobName'] = variant_report_id
    indel_variant_df['variant_id'] = variant_id
    #indel_variant_df['indel_variant_of$alleleFrequency'] = alleleFrequency
    indel_variant_df['exon'] = indel_variant_df['exon'].astype('Int32')

    property = [
        {'old':'variant_report.jobName', 'new':'variant_report.variant_report_id'},
        {'old':'indel_variant_of$alleleFrequency', 'new':'indel_variant_of$allele_frequency'},
        {'old':'identifier', 'new':'external_variant_id'},
        {'old':'transcript', 'new':'transcript_id'},
        {'old':'hgvs', 'new':'transcript_hgvs'},
        {'old':'genomicHgvs', 'new':'genomic_hgvs'},
        {'old':'function', 'new':'variant_classification'},
        {'old':'oncominevariantclass', 'new':'oncomine_variant_class'},
        {'old':'protein', 'new':'amino_acid_change'}
    ]
    indel_variant_df = rename_properties(indel_variant_df, property)
    indel_variant_df.to_csv('transformation_script/indel_variant.tsv', sep = "\t", index = False)

