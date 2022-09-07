import pandas as pd
from transformation_script.property_function import add_properties
#from bento.common.utils import get_logger



def arm_transformation(arm_file_name, log):
    #log = get_logger('Arm Transformation')
    log.info('Transforming arm.csv')
    arm_df = pd.read_csv(arm_file_name)
    arm_columns = list(arm_df.columns)
    new_arm_id = []
    for arm_id in arm_df['arm_id']:
        arm_id_list = arm_id.split('-')
        new_arm_id.append(arm_id_list[1])
    arm_df['arm_id'] = new_arm_id
    clinical_trial_id = 'clinical_trial.clinical_trial_id'
    if not clinical_trial_id in arm_columns:
        arm_df['clinical_trial.clinical_trial_id'] = ['NCT02465060'] * len(arm_df)
    props = [
        {'new_property':'type', 'new_value':['arm'] * len(arm_df)},
        {'new_property':'show_node', 'new_value':['TRUE'] * len(arm_df)}
    ]
    arm_df = add_properties(arm_df, props)
    arm_df = arm_df.reindex(columns=['show_node', 'type', 'clinical_trial.clinical_trial_id', 'arm_id', 'arm_target', 'arm_drug', 'pubmed_id'])
    arm_df.to_csv('transformation_script/arm.tsv', sep = "\t", index = False)


