import dash
import dash_core_components as dcc
import dash_html_components as html
# pip install psycopg2-binary
import psycopg2
import pandas as pd
from dash.dependencies import Input, Output
from util import generate_table, year_month_options, month_dict, subway_stations_names, distance_range
from config import host, port, dbname, user, password

# Connect to PostgreSQL database
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
cur = conn.cursor()

cur.execute('CREATE TABLE EMPLOYEE (\
         FIRST_NAME  CHAR(20) NOT NULL,\
         LAST_NAME  CHAR(20),\
         AGE INT,  \
         SEX CHAR(1),\
         INCOME FLOAT )')
conn.commit()

# Create the dataframe bike
# bike = pd.read_sql_query("SELECT * FROM bike LIMIT 3;", conn)
yellow = pd.read_sql_query("SELECT * FROM yellow LIMIT 3;", conn)
green = pd.read_sql_query("SELECT * FROM green LIMIT 3;", conn)

bike = pd.read_sql_query("\
    SELECT * \
    FROM bike \
    WHERE year = 2013\
    AND month = 8\
    AND start_station = '6th Ave'\
    AND avg_duration < 15\
    ORDER BY count DESC\
    LIMIT 20;", conn)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

app.layout = html.Div(children=[
    html.H1(
        children = 'First & Last Mile',
        style = {
            'textAlign': 'center'
        }),

    html.H5(
        children = 'Popular Origin & Destination Detection Platform',
        style = {
            'textAlign': 'center'
        }),

    html.Div([
        html.Div([
            html.Label('Year'),
            dcc.Dropdown(
                id = 'year-dropdown',
                options = [{'label':k, 'value':k} for k in year_month_options.keys()],
                value = 2019)
            ], className = "six columns"),

        html.Div([
            html.Label('Month'),
            dcc.Dropdown(id='month-dropdown')
            ], className = "six columns")
        ]),

    html.Div([
        html.Div([
            html.Label('Subway Station'),
            dcc.Dropdown(
            id = 'station-dropdown',
            options = [{'label':k, 'value':k} for k in subway_stations_names],
            value = subway_stations_names[0])
            ], className = "six columns"),

        html.Div([
            html.Label('Max Searching Distance'),
            dcc.Dropdown(
            id = 'distance-dropdown',
            options = [{'label':k, 'value':k} for k in distance_range],
            value = distance_range[0])
            ], className = "six columns")
        ]),

    html.Hr(),

    html.H5(id='display-selected-input', 
        style = {
            'textAlign': 'center'
        }),

    generate_table(bike),
    generate_table(yellow),
    generate_table(green)
])

@app.callback(
    Output('month-dropdown', 'options'),
    [Input('year-dropdown', 'value')])
def set_month_options(selected_year):
    return [{'label': month_dict[i], 'value': i} for i in year_month_options[selected_year]]

@app.callback(
    Output('month-dropdown', 'value'),
    [Input('month-dropdown', 'options')])
def set_month_value(available_options):
    return available_options[0]['value']

@app.callback(
    Output('display-selected-input', 'children'),
    [Input('year-dropdown', 'value'),
     Input('month-dropdown', 'value'),
     Input('station-dropdown', 'value'),
     Input('distance-dropdown', 'value')])
def set_display_children(year, month, station, distance):
    return 'Top 10 Destinations within {} Miles of {} Subway Station in {} {}:'\
    .format(distance, station, month_dict[month], year)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)