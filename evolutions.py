import os
import pandas as pd
from db_connection import db_connection
from datetime import datetime

def unwanted_copies(path):
    os.chdir(path)
    table_name = 'gw_burntarea_effis.rob_ba_evo_test'
    df, gdf = db_connection(table_name, use='r')
    print('The number of evolutions to check is {}\n'.format(gdf.shape[0]))
    print(type(df['initialdate'][0]))
    df['initialdate'] = df['initialdate'].apply(lambda x:x.date())

    # nduplicates: dataframe containing the count of the evolutions with the same ba_id, initialdate, finaldate, area_ha
    nduplicates = df.groupby(['ba_id', 'initialdate', 'finaldate', 'area_ha'])['id'].nunique().sort_values(
        ascending=False)
    nduplicates = pd.DataFrame(nduplicates)
    # nduplicates = gdf.groupby(['ba_id', 'initialdate', 'finaldate', 'area_ha'])['id'].nunique().sort_values(
    #     'ba_id')
    nduplicates.to_csv('duplicated_evolutions_number.csv')
    nduplicates = pd.read_csv('duplicated_evolutions_number.csv')

    # duplicates: dataframe containing the id of the evolutions to keep
    evo = df.groupby(['ba_id', 'initialdate', 'finaldate', 'area_ha'])['id'].first().sort_values(
        ascending=False)
    evo = pd.DataFrame(evo)
    # duplicates = gdf.groupby(['ba_id', 'initialdate', 'finaldate', 'area_ha'])['id'].first().sort_values(
    #     'ba_id')
    evo.to_csv('correct_evolutions.csv')
    evo = pd.read_csv('correct_evolutions.csv')
    evo_merge = evo.merge(nduplicates[['ba_id','id']], how='inner', left_on='ba_id', right_on='ba_id')
    evo_merge.to_csv('evo_merge.csv')
    evo_merge = pd.read_csv('evo_merge.csv')
    evo_merge = evo_merge.rename(columns={"id_x":"evo_id", "id_y":"count"})
    id_redundant = set(df.id) - set(evo_merge.evo_id)
    sql_list = '('
    for i in id_redundant:
        sql_list = sql_list + str(i) + ', '
    sql_list = sql_list[0:-2] +')'
    print(sql_list)

    if gdf.shape[0]-evo.shape[0] != 0:
        print('The number of cleaned up evolutions is {}. {} duplicates will be eliminated.\n'.format(len(set(evo_merge.evo_id)), len(id_redundant)))
        writing_table_name = 'gw_burntarea_effis.rob_ba_evo_test'
        sql_query = 'DELETE FROM {} WHERE id IN {}'.format(writing_table_name, sql_list)
        print(sql_query)
        db_connection(writing_table_name, use='w', statement=sql_query)
    else:
        print('No update needed on the evolutions table')
    return 0