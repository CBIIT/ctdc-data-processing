import pandas as pd
from transformation_script.property_function import rename_properties, remove_nan

def case_transformation(case_file_name, log):
    log.info('Transforming case.csv')
    case_df = pd.read_csv(case_file_name)
    case_df['diseases'] = remove_nan(case_df['diseases'])
    case_df['ctepCategory'] = remove_nan(case_df['ctepCategory'])
    new_arm_id = []
    case_id = []
    races = []
    gender = []
    ethnicity = []
    for index in range(len(case_df)):
        arm_id_list = case_df['arm.arm_id'].iloc[index].split('-')
        gender.append(case_df['gender'].iloc[index].lower().title())
        race_list = case_df['races'].iloc[index].split('_')
        new_race = ''
        if case_df['races'].iloc[index] == 'AMERICAN_INDIAN_OR_ALASKA_NATIVE, WHITE':
            races.append('Multirace')
        else:
            for race_value in race_list:
                if race_value == 'OR' or race_value == 'REPORTED':
                    if new_race != '':
                        new_race = new_race + ' ' + race_value.lower()
                    else:
                        new_race = race_value.lower()
                else:
                    if new_race != '':
                        new_race = new_race + ' ' + race_value.lower().title()
                    else:
                        new_race = race_value.lower().title()
            races.append(new_race)
        ethnicity_list = case_df['ethnicity'].iloc[index].split('_')
        new_ethnicity = ''
        for ethnicity_value in ethnicity_list:
            if ethnicity_value == 'OR' or ethnicity_value == 'REPORTED':
                if new_ethnicity != '':
                    new_ethnicity = new_ethnicity + ' ' + ethnicity_value.lower()
                else:
                    new_ethnicity = ethnicity_value.lower()
            else:
                if new_ethnicity != '':
                    new_ethnicity = new_ethnicity + ' ' + ethnicity_value.lower().title()
                else:
                    new_ethnicity = ethnicity_value.lower().title()
        ethnicity.append(new_ethnicity)
        new_arm_id.append(arm_id_list[1])
        case_id.append('CTDC-' + str(case_df['patientSequenceNumber'].iloc[index]))
        index += 1
    case_df = case_df.drop(columns = ['PSN'])
    case_df['arm.arm_id'] = new_arm_id
    case_df['races'] = races
    case_df['gender'] = gender
    case_df['ethnicity'] = ethnicity
    case_df['show_node'] = ['TRUE'] * len(case_df)
    case_df['case_id'] = case_id
    property = [
        {'old':'patientSequenceNumber', 'new':'source_id'},
        {'old':'races', 'new':'race'},
        {'old':'currentPatientStatus', 'new':'patient_status'},
        {'old':'currentStepNumber', 'new':'current_step'},
        {'old':'diseases', 'new':'disease'},
        {'old':'ctepCategory', 'new':'ctep_category'},
        {'old':'ctepSubCategory', 'new':'ctep_subcategory'},
        {'old':'disease_id', 'new':'meddra_code'},
        {'old':'priorDrugs', 'new':'prior_drugs'}
    ]
    case_df = rename_properties(case_df, property)
    case_df = case_df.reindex(columns=['type', 'show_node', 'arm.arm_id', 'case_id', 'source_id',
        'gender','race', 'ethnicity', 'patient_status', 'current_step', 'disease', 'ctep_category', 'ctep_subcategory', 'meddra_code', 'prior_drugs'])
    case_df['meddra_code'] = case_df['meddra_code'].astype('Int32')
    case_df.to_csv('transformation_script/case.tsv', sep = "\t", index = False)

