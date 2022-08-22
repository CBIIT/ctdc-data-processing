import pandas as pd
from bs4 import BeautifulSoup

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