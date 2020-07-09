import pandas as pd
import numpy as np

'''
Usage of the script: analysing differences between two dfs
'''

def compare_dfs(df1, df2):
    print(f'df1 shape: {df1.shape}, \ndf2 shape: {df2.shape}')
    dfs_dictionary = {'DF1': df1, 'DF2': df2}
    df = pd.concat(dfs_dictionary)
    #df = df.fillna('unknown')
    df = df.drop_duplicates(keep=False)
    print(df.loc['DF1'])
    print(df.loc['DF2'])

def compare_dfs_value_changes(df1, df2):
    #for dfs equal in length

    df1.fillna('unknown', inplace=True)
    df2.fillna('unknown', inplace=True)
    print(f'df1 shape: {df1.shape}, \ndf2 shape: {df2.shape}')
    ne_stacked = (df1 != df2).stack()
    changed = ne_stacked[ne_stacked]
    changed.index.names = ['id', 'col']

    difference_locations = np.where(df1 != df2)
    changed_from = df1.values[difference_locations]
    changed_to = df2.values[difference_locations]
    df = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=changed.index)

    print(df)
    print(df.shape)

def print_df_containing_list(df, columnname, list):
    print(df.loc[df[columnname].isin(list)])