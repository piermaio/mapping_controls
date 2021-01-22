#
# Mapping controls for the publication of the record
#

import os

def publication_check(path, gdf_ba):
    os.chdir(path)
    df = gdf_ba
    unpublished = df.loc[df['public'] == False]
    with open('publication_check.txt', 'w+') as f:
        for i in unpublished['id']:
            f.write('{}\n'.format(int(i)))
    return 0