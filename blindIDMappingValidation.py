import argparse
import pandas as pd
from bs4 import BeautifulSoup
from bento.common.utils import get_logger

def convert_to_xlsx(blindID_file):
    blindID_file_name = blindID_file.split('.')
    with open(blindID_file) as xml_file:
        soup = BeautifulSoup(xml_file.read(), 'xml')
        new_blindID_file_name = blindID_file_name[0] + '.xlsx'
        writer = pd.ExcelWriter(new_blindID_file_name)
        for sheet in soup.findAll('Worksheet'):
            sheet_as_list = []
            for row in sheet.findAll('Row'):
                sheet_as_list.append([cell.Data.text if cell.Data else '' for cell in row.findAll('Cell')])
            pd.DataFrame(sheet_as_list).to_excel(writer, sheet_name=sheet.attrs['ss:Name'], index=False, header=False)

        writer.save()
    return new_blindID_file_name

LOGER_NAME = 'Blind ID Mapping Validation'
log = get_logger(LOGER_NAME)
parser = argparse.ArgumentParser(description='Blind ID mapping validation')
parser.add_argument("psn_file", help="Name of PSN File to run the blind ID mapping validation")
parser.add_argument("blindID_file", help="Name of PSN File to run the blind ID mapping validation")
args = parser.parse_args()
#config = Config(args.config_file)

#Converting the Blind ID file to xlsx
log.info('Converting the blind ID file to xlsx')
new_blindID_file_name = convert_to_xlsx(args.blindID_file)

#Read in data for matching validation
blindID_file = new_blindID_file_name
psn_file = args.psn_file
patient_id_df = pd.read_excel(blindID_file)
psn_df = pd.read_csv(psn_file)

#Validation
line_num = 1
validation_success = True
for value in psn_df['PSN'].tolist():
    line_num += 1
    occurrence = patient_id_df['Case ID'].tolist().count(value)
    if occurrence == 0:
        log.error('Line {}: The PSN value {} does not have an entry in the mapping file'.format(line_num, value))
        validation_success = False
    elif occurrence > 1:
        log.error('Line {}: The PSN value {} has more than one entry in the mapping file'.format(line_num, value))
        validation_success = False


if validation_success:
    log.info('Validation success')
else:
    log.info('Validation fail')