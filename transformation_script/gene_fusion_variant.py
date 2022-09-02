import pandas as pd
from transformation_script.property_function import rename_properties
import hashlib


def gene_fusion_variant_transformation(gene_fusion_variant_file_name, log):
    log.info('Transforming gene_fusion_variant.csv')
    gene_fusion_variant_df = pd.read_csv(gene_fusion_variant_file_name)
    gene_fusion_variant_df['show_node'] = [True] * len(gene_fusion_variant_df)
    variant_report_id = []
    variant_id = []
    for index in range(len(gene_fusion_variant_df)):
        variant_report_id.append('CTDC-VAR-REP-' + str(gene_fusion_variant_df['variant_report.jobName'].iloc[index]))
        variant_id_value = gene_fusion_variant_df['partnerGene'].iloc[index] + gene_fusion_variant_df['driverGene'].iloc[index] + gene_fusion_variant_df['identifier'].iloc[index]
        varidant_id_md5 = str(hashlib.md5(variant_id_value.encode('utf-8')).hexdigest())
        variant_id.append('CTDC-VAR-' + varidant_id_md5)

    gene_fusion_variant_df['variant_report.jobName'] = variant_report_id
    gene_fusion_variant_df['variant_id'] = variant_id
    property = [
        {'old':'variant_report.jobName', 'new':'variant_report.variant_report_id'},
        {'old':'identifier', 'new':'external_variant_id'},
        {'old':'partnerGene', 'new':'gene1'},
        {'old':'gene_fusion_variant_of$partnerReadCount', 'new':'gene_fusion_variant_of$gene1_read_count'},
        {'old':'driverGene', 'new':'gene2'},
        {'old':'gene_fusion_variant_of$driverReadCount', 'new':'gene_fusion_variant_of$gene2_read_count'},
        {'old':'oncominevariantclass', 'new':'oncomine_variant_class'}
    ]
    gene_fusion_variant_df = rename_properties(gene_fusion_variant_df, property)
    gene_fusion_variant_df =gene_fusion_variant_df.reindex(columns=['type', 'show_node', 'variant_report.variant_report_id', 'variant_id', 'external_variant_id',
        'gene1', 'gene_fusion_variant_of$gene1_read_count', 'gene2', 'gene_fusion_variant_of$gene2_read_count', 'oncomine_variant_class'])
    gene_fusion_variant_df.to_csv('transformation_script/gene_fusion_variant.tsv', sep = "\t", index = False)

