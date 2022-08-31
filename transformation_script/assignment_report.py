import pandas as pd

def assignment_report_transformation(assignment_file_name, log):
    log.info('Transforming assignment_report.csv')
    new_arm_id = []
    assignment_df = pd.read_csv(assignment_file_name)
    for arm_id in assignment_df['arm.arm_id']:
        arm_id_list = arm_id.split('-')
        new_arm_id.append(arm_id_list[1])
    assignment_df['arm.arm_id'] = new_arm_id
    assignment_df = assignment_df.drop(columns = ['acl'])
    assignment_df.to_csv('transformation_script/assignment_report2.tsv', sep = "\t", index = False)

