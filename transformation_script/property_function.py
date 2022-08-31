import pandas as pd

def rename_properties(df, property):
    for x in range(0, len(property)):
        df = df.rename(columns={property[x]['old']: property[x]['new']})
    return df

def add_properties(df, props):
    for property in props:
        df[property['new_property']] = property['new_value']
    return df

def remove_nan(value_list):
    new_value_list = []
    for value in value_list:
        if pd.isna(value):
            new_value_list.append('-')
        else:
            new_value_list.append(value)
    return new_value_list