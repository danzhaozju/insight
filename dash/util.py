import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd

# year_month_options
years = range(2013,2020)
year_month_options = {}
for year in years:
    if year == 2013:
        year_month_options[year] = range(8,13)
    elif year == 2019:
        year_month_options[year] = range(1,7)
    else:
        year_month_options[year] = range(1,13)

# Month dictionary that converts month number to month name
month_dict={ 1: "January",
       2: "February",
       3: "March",
       4: "April",
       5: "May",
       6: "June",
       7: "July",
       8: "August",
       9: "September",
       10: "October",
       11: "November",
       12: "December"
}

# Subway station names
subway_stations = pd.read_csv("../batch_processing/NY_subway_loc/NY_subway_station_loc.csv")
subway_stations_names = subway_stations['station_name'].unique()

# Distance range
distance_range = range(1,11)

# Functions
def generate_table(dataframe, max_rows=100):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +
        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )
