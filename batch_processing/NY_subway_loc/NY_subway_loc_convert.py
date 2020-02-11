### Convert new york subway station locations to longitude and latitude

import pandas as pd
import re
import warnings
warnings.filterwarnings('ignore')

df = pd.read_csv('NY_subway_station_loc_raw.csv')

stations = df[['OBJECTID','NAME','the_geom']]
stations.rename(columns = {'OBJECTID':'object_id', 'NAME':'station_name'}, inplace = True)

stations['longitude'] = stations['the_geom'].str.extract('(-\d+.\d+)',expand=True)
stations['latitude'] = stations['the_geom'].str.extract('\s(\d+.\d+)',expand=True)
stations.drop(columns='the_geom', inplace=True)

stations.to_csv('NY_subway_station_loc.csv',index = False, header = True)

