import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd

# Create year_month_options
years = range(2013,2020)
year_month_options = {}
for year in years:
    if year == 2013:
        year_month_options[year] = list(range(8,13))
    elif year in range(2014,2019):
        year_month_options[year] = list(range(1,13))
    elif year == 2019:
        year_month_options[year] = list(range(1,7))

# Functions
def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +
        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )
