# # pyshp 2.1.0 project description: https://pypi.org/project/pyshp/
# get_ipython().system('pip install pyshp')
# # pyproj 2.4.2
# get_ipython().system('pip install pyproj')

import pandas as pd
import numpy as np
import shapefile

sf = shapefile.Reader("taxi_zones/taxi_zones.shp")
fields_name = [field[0] for field in sf.fields[1:]]
shp_dic = dict(zip(fields_name, list(range(len(fields_name)))))
attributes = sf.records()
shp_attr = [dict(zip(fields_name, attr)) for attr in attributes]

def get_lat_lon(sf):
    content = []
    for sr in sf.shapeRecords():
        shape = sr.shape
        rec = sr.record
        loc_id = rec[shp_dic['location_i']]
        
        x = (shape.bbox[0]+shape.bbox[2])/2
        y = (shape.bbox[1]+shape.bbox[3])/2
        
        content.append((loc_id, x, y))
    return pd.DataFrame(content, columns=["location_i", "longitude", "latitude"])

df_loc = pd.DataFrame(shp_attr).join(get_lat_lon(sf).set_index("location_i"), on="location_i")
df_loc = df_loc[['location_i','longitude','latitude','borough','zone','shape_area','shape_leng']]
df_loc.head()

df_loc.to_csv('taxi_locID_lon_lat.csv', index = False, header = True)



