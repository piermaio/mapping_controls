#
# Mapping controls for the locations reported in the burnt areas:
# place, province != N.A.
#

import os
from db_connection import db_connection

def location_defined(path):
    os.chdir(path)
    table_name = 'gw_burntarea_effis.ba_oracle_compat_year'
    df, gdf = db_connection(table_name, use='r')
    not_defined = df[(df['place_name'] == 'N.A.') or (df['PROVINCE'] == 'N.A.')]
    not_defined.to_csv('locations.csv')
    return 0