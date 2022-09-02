import pandas as pd
import glob


for file in glob.glob("*.tsv"):
    print(file)
    file_list = file.split('.')
    file_location = 'transformed/' + file_list[0] + '.txt'
    csvdata = pd.read_csv(file_location, delimiter = "\t")
    #cols = list(csvdata.columns.values)
    #print(cols)
    csvdata_old = pd.read_csv(file, sep = "\t")
    #csvdata = csvdata.reindex(sorted(csvdata.columns), axis=1)
    #csvdata_old = csvdata_old.reindex(sorted(csvdata_old.columns), axis=1)
    print(csvdata.equals(csvdata_old))