#
# Mapping controls for the dates reported in the burnt areas:
# Final date => Initial date

#
import os

def ba_dates_check(path, gdf_ba): # df_sql based on the
    df = gdf_ba
    print(df.columns)
    os.chdir(path)
    df['difference'] = df['finaldate'] - df['initialdate']
    df['difference'] = df['difference'].apply(lambda x: x.days)
    # wrong_dates = df[(df['finaldate'] < df['initialdate'])]
    wrong_dates = df.loc[df['difference'] < -1]
    with open('wrong_dates.txt', 'w+') as f:
        for i in wrong_dates['id']:
            f.write('{}\n'.format(int(i)))
    wrong_dates.to_csv('wrong_dates.csv')
    return 0