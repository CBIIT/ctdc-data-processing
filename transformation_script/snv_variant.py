import pandas as pd
import hashlib
from transformation_script.property_function import rename_properties, remove_nan, remove_trailing_zero

def snv_variant_transformation(snv_variant_file_name, log):
    log.info('Transforming snv_variant.csv')
    snv_variant_df = pd.read_csv(snv_variant_file_name)
    variant_report_id = []
    variant_id = []
    #alleleFrequency = []
    snv_variant_df['reference'] = remove_nan(snv_variant_df['reference'])
    snv_variant_df['alternative'] = remove_nan(snv_variant_df['alternative'])
    snv_variant_df['transcript'] = remove_nan(snv_variant_df['transcript'])
    snv_variant_df['protein'] = remove_nan(snv_variant_df['protein'])
    snv_variant_df['function'] = remove_nan(snv_variant_df['function'])
    snv_variant_df['show_node'] = ['TRUE'] * len(snv_variant_df)
    for index in range(len(snv_variant_df)):
        variant_report_id.append('CTDC-VAR-REP-' + str(snv_variant_df['variant_report.jobName'].iloc[index]))
        variant_id_value = snv_variant_df['chromosome'].iloc[index] + str(snv_variant_df['position'].iloc[index]) + str(snv_variant_df['reference'].iloc[index]) + str(snv_variant_df['alternative'].iloc[index]) + snv_variant_df['hgvs'].iloc[index]
        varidant_id_md5 = str(hashlib.md5(variant_id_value.encode('utf-8')).hexdigest())
        variant_id.append('CTDC-VAR-' + varidant_id_md5)
        #alleleFrequency.append(round(snv_variant_df['snv_variant_of$alleleFrequency'].iloc[index], 1))

    snv_variant_df['variant_report.jobName'] = variant_report_id
    snv_variant_df['variant_id'] = variant_id
    #snv_variant_df['snv_variant_of$alleleFrequency'] = alleleFrequency
    #snv_variant_df['exon'] = remove_nan(snv_variant_df['exon'])

    property = [
        {'old':'variant_report.jobName', 'new':'variant_report.variant_report_id'},
        {'old':'snv_variant_of$alleleFrequency', 'new':'snv_variant_of$allele_frequency'},
        {'old':'identifier', 'new':'external_variant_id'},
        {'old':'transcript', 'new':'transcript_id'},
        {'old':'hgvs', 'new':'transcript_hgvs'},
        {'old':'genomicHgvs', 'new':'genomic_hgvs'},
        {'old':'function', 'new':'variant_classification'},
        {'old':'oncominevariantclass', 'new':'oncomine_variant_class'},
        {'old':'protein', 'new':'amino_acid_change'}
    ]
    snv_variant_df = rename_properties(snv_variant_df, property)
    snv_variant_df['exon'] = remove_trailing_zero(snv_variant_df['exon'])
    snv_variant_df = snv_variant_df.reindex(columns=['type', 'show_node', 'variant_report.variant_report_id', 'variant_id', 'external_variant_id', 'gene', 'chromosome', 'exon',
        'position', 'reference', 'alternative', 'snv_variant_of$allele_frequency', 'transcript_id', 'transcript_hgvs', 'oncomine_variant_class', 'variant_classification', 'amino_acid_change', 'genomic_hgvs'])
    snv_variant_df.to_csv('transformation_script/snv.tsv', sep = "\t", index = False)

