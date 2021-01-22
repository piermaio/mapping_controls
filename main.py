#
# Main of mapping controls
#
import db_connection
import ba_dates
import evolutions
import province_place

def main():
    path = 'C:\\Users\piermaio\Documents\gisdata\jrc\weekly_videoconference\\'  # PM local output path
    # df_sql, gdf_sql, tab_nations, gdf_ba, gdf_evo = db_connection.db_connection()
    # gdf_evo = db_connection.db_connection()
    # ba_dates.ba_dates_check(path, gdf_ba)
    evolutions.unwanted_copies(path)
    # province_place.location_defined(path)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
