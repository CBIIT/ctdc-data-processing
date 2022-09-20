import pandas as pd
import os

def assignment_report_transformation(assignment_file_name, log, input_files, output_folder):
    log.info('Transforming assignment_report.csv')
    new_arm_id = []
    assignment_df = pd.read_csv(assignment_file_name)
    for arm_id in assignment_df['arm.arm_id']:
        arm_id_list = arm_id.split('-')
        new_arm_id.append(arm_id_list[1])
    assignment_df['arm.arm_id'] = new_arm_id
    assignment_df = assignment_df.drop(columns = ['acl'])
    assignment_df = assignment_df.reindex(columns=['type', 'show_node', 'arm.arm_id', 'file_description', 'file_format',
        'file_name', 'file_size', 'file_type', 'md5sum'])

    input_file_name = os.path.splitext(input_files['assignment_report'])[0]
    output_file = os.path.join(output_folder, input_file_name + ".tsv")
    assignment_df.to_csv(output_file, sep = "\t", index = False)

