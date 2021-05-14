#
# PM this script provides a list of non-wildland potentially wrong burnt areas
#

import os
import pandas as pd
import psycopg2
import datetime as dt
import numpy as np
import sqlalchemy
from rasterstats import zonal_stats
import geopandas as gpd
import csv


def db_connection():
	try:
		connection = psycopg2.connect(user = "pieralberto",
									  password = "4q2WK4MZMs",
									  host = "db1.wild-fire.eu",
									  port = "5432",
									  database = "e1gwis")

		cursor = connection.cursor()
		# Print PostgreSQL Connection properties
		print(connection.get_dsn_parameters(), "\n")

		# Print PostgreSQL version
		cursor.execute("SELECT version();")
		record = cursor.fetchone()
		print("You are connected to - ", record, "\n")
		cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
		# print(cursor.fetchall())
		# temp = cursor.fetchall()

		engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
			user = "pieralberto",
			password = "4q2WK4MZMs",
			host = "db1.wild-fire.eu",
			port = "5432",
			database = "e1gwis",
		)
		# print('creating sqlalchemy engine')
		engine = sqlalchemy.create_engine(engine_string)
		# read a table from database into pandas dataframe, replace "tablename" with your table name
		global df_sql
		df_sql = pd.read_sql_query('select * from gw_burntarea_effis.ba_oracle_export_year', con = engine)
		nat2k_year = pd.read_sql_query('select sum(clc.natura2k*ba.area_ha) as '
									   'tot_nat2k_ha from gw_burntarea_effis.ba_final as ba, '
									   'gw_burntarea_effis.ba_stats_clc as clc where ba.id=clc.ba_id and '
									   'ba.initialdate>=\'2020-01-01\' and clc.natura2k is not null;', con = engine)
		nat2kweek = pd.read_sql_query('select sum(clc.natura2k*ba.area_ha) as tot_nat2k_ha from '
									  'gw_burntarea_effis.ba_final as ba, gw_burntarea_effis.ba_stats_clc '
									  'as clc where ba.id=clc.ba_id and ba.initialdate>=(now()-interval \'7 days\')::date '
									  'and clc.natura2k is not null;', con = engine)
		print("df_sql successfully downloaded")
		global gdf_sql
		gdf_sql = gpd.read_postgis('select * from gw_burntarea_effis.ba_oracle_export_year', con=engine)
		print("gdf_sql successfully downloaded")
		global gdf_ba
		gdf_ba = gpd.read_postgis('select * from gw_burntarea_effis.ba_year', con=engine)
		print("gdf_sql successfully downloaded")
		global tab_nations  # Table associating the
		tab_nations = pd.read_sql_query('select * from "burnt_areas"."Tab_Elenco_Nazioni"', con=engine)
		print("tab_nations successfully downloaded")
	except (Exception, psycopg2.Error) as error:
		print("Error while connecting to PostgreSQL", error)
	finally:
		# closing database connection.
		if (connection):
			cursor.close()
			connection.close()
			print("PostgreSQL connection now closed")
	return df_sql, nat2k_year, nat2kweek, gdf_sql, tab_nations, gdf_ba

def corine_stats(burnt_areas, corine, dict_path, tab_nations):
	ba = burnt_areas
	# os.chdir('C:\\Users\\piermaio\\Documents\\gisdata\\jrc\\richieste\\20210505_boca_biella')
	# ba = gpd.read_file('final_output_dissolve_2005_LAEA.shp')
	print(ba.shape)
	# ba = ba.merge(tab_nations, how='inner', left_on='COUNTRY', right_on='NUTS0_CODE')
	print('--- Columns of burnt areas ---')
	print(ba.columns)
	ba = ba.sort_values('id')
	os.chdir(dict_path)
	f = csv.reader(open('corine_key.csv'))
	corine_dict = {}
	for row in f:
		corine_dict[row[0]] = row[1]
	corine_dict['1'] = corine_dict.pop('ï»¿1')
	corine_dict = {int(k): v for k, v in corine_dict.items()}
	# ba_stats = zonal_stats(ba, corine, categorical=True, category_map=corine_dict)
	ba_raw = zonal_stats(ba, corine, categorical=True)
	df_ba_raw = pd.DataFrame.from_dict(ba_raw)
	df_ba_raw = df_ba_raw.rename(columns=corine_dict)
	df_ba_raw = df_ba_raw.T
	df_ba_raw.to_csv('corine_ba_raw.csv')
	df_mapped = df_ba_raw.groupby(df_ba_raw.index).sum()
	df_mapped = df_mapped.T
	# df_mapped.to_csv('corine_ba_raw2.csv')
	# PM

	#df = pd.DataFrame.from_dict(ba_stats)
	df = df_mapped
	l = ba['id'].to_list()
	a = ba['area_ha'].to_list()
	df = df.assign(id=l, area=a)
	df.to_csv('__df_test.csv')
	cols = df.columns[:-2]
	print(cols)
	df[cols] = df[cols].div(df[cols].sum(axis=1), axis=0).multiply(100)
	df.to_csv('__df_test2.csv')
	not_natural = df[(df['Broad Leaved (clc 311)']==0)
					 & (df['Other Natural (clc 321,322, 331-423)']==0)
					 & (df['Transitional (clc 324)']==0)
					 & (df['Other (clc 255, 511-995)']==0) # or df['Other (clc 255, 511-995)']==None)
					 & (df['Coniferous (clc 312)']==0)
					 & (df['Sclerophyllous (clc 323)']==0)
					 & (df['Mixed Forest (clc 313)']==0)]
	with open('list_non_natural.txt', 'w+') as f:
		for i in not_natural['id']:
			f.write('{}\n'.format(int(i)))
	df.to_csv('corine_df.csv')

def main():
	path = 'C:\\Users\piermaio\Documents\gisdata\jrc\weekly_videoconference\\' #PM local output path
	df_sql, nat2k_year, nat2k_week, gdf_sql, tab_nations, gdf_ba = db_connection()
	current_year = dt.datetime.today().year
	years = np.arange(2008,2021)  # right limit excluded
	corine_path = 'C:\\Users\\piermaio\\Documents\\gisdata\\jrc\\BAmapping\\Corine\\raster\\Corine_globcover_MA_TU_ukraine.tif'  # PM local path to the corine geotif
	dict_path = 'C:\\Users\\piermaio\\Documents\\gisdata\\jrc\\weekly_videoconference' # PM local path to the corine key
	corine_stats(gdf_ba, corine_path, dict_path, tab_nations)


if __name__ == "__main__":
	main()
