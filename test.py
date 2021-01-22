import os

import pandas as pd

path = 'C:\\Users\piermaio\Documents\gisdata\jrc\weekly_videoconference\\'
os.chdir(path)
evo_merge = pd.read_csv('evo_merge.csv')
evo_merge = evo_merge.rename(columns={"id_x":"evo_id", "id_y":"count"})
id_reduntant = set(evo_merge.evo_id)
sql_query = '('
for i in id_reduntant:
    sql_query = sql_query + str(i) + ', '
sql_query = sql_query[0:-2] +')'
print(sql_query)


# test change