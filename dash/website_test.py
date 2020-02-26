import dash
import dash_core_components as dcc
import dash_html_components as html
# pip install psycopg2-binary
import psycopg2
import pandas as pd
import dash_table
from dash.dependencies import Input, Output
from util import generate_table, year_month_options, month_dict, subway_stations_names, distance_range, vehicles
from plotly import express as px
from dash.exceptions import PreventUpdate

# Connect to PostgreSQL database
host = "ec2-34-222-184-155.us-west-2.compute.amazonaws.com"
port = "5432"
dbname = "insight"
user = "dan"
password = "zhaodan"
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
cur = conn.cursor()

def generate_trips(year, month, station, distance, vehicle):
    if vehicle == 'bike':
        trips = pd.read_sql_query("\
            SELECT year, month, start_station, end_latitude, end_longitude, count, end_geohash\
                ,avg_distance, avg_duration\
            FROM "+ vehicle +" \
            WHERE year = "+ str(year) +"\
            AND month = "+ str(month) +"\
            AND start_station = '"+ station +"'\
            AND avg_distance <= "+ str(distance) +"\
            ORDER BY count DESC;", conn)
        trips['index'] = trips.index
        trips = trips[['index', 'end_latitude', 'end_longitude', 'count','end_geohash',
            'avg_distance', 'avg_duration']]
    else:
        trips = pd.read_sql_query("\
            SELECT year, month, start_station, end_latitude, end_longitude, count, end_geohash\
                ,avg_distance, avg_duration, avg_passengers, avg_cost\
            FROM "+ vehicle +" \
            WHERE year = "+ str(year) +"\
            AND month = "+ str(month) +"\
            AND start_station = '"+ station +"'\
            AND avg_distance <= "+ str(distance) +"\
            ORDER BY count DESC;", conn)
        trips['index'] = trips.index
        trips = trips[['index', 'end_latitude', 'end_longitude', 'count','end_geohash',
            'avg_distance', 'avg_duration','avg_passengers','avg_cost']]    
    return trips

px.set_mapbox_access_token(open(".mapbox_token").read())

def generate_map(trips):
    fig = px.scatter_mapbox(trips, lat = "end_latitude", lon = "end_longitude", 
                        color = "count", size = "count",
                        size_max=15, zoom=11)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return fig

original_trips = generate_trips(2019, 1, 'Astor Pl', 1, 'bike')
original_fig = generate_map(original_trips)


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
            html.Label('Origin/Destination'),
            dcc.Dropdown(
                id = 'origin_destination_dropdown',
                options = [{'label': k, 'value': k} for k in ['Origin', 'Destination']],
                value = 'Origin')
            ], className = "four columns"),

        html.Div([
            html.Label('Year'),
            dcc.Dropdown(
                id = 'year-dropdown',
                options = [{'label':k, 'value':k} for k in year_month_options.keys()],
                value = 2019)
            ], className = "four columns"),

        html.Div([
            html.Label('Month'),
            dcc.Dropdown(id='month-dropdown')
            ], className = "four columns")
        ]),

    html.Div([
        html.Div([
            html.Label('Available Vehicles'),
            dcc.Dropdown(
                id = 'vehicle-dropdown',
                options = [{'label':vehicles[k], 'value':k} for k in vehicles.keys()],
                value = 'bike')
            ], className = "four columns"),

        html.Div([
            html.Label('Subway Station'),
            dcc.Dropdown(
            id = 'station-dropdown',
            options = [{'label':k, 'value':k} for k in subway_stations_names],
            value = subway_stations_names[0])
            ], className = "four columns"),

        html.Div([
            html.Label('Max Searching Distance'),
            dcc.Dropdown(
            id = 'distance-dropdown',
            options = [{'label':k, 'value':k} for k in distance_range],
            value = 1)
            ], className = "four columns")
        ]),

    html.Div([
        html.Button('Submit', id='button', style = {'width': '30%'})
        ], style={'textAlign':'center'}), 

    html.H5(id='display-selected-input', 
        children='Top Origins of Citi Bike Trips within 1 Miles of Astor Pl Subway Station in January 2019:',
        style = {
            'textAlign': 'center'
        }),

    dcc.Graph(
        id = "map",
        figure = original_fig),

    dash_table.DataTable(
        id = 'table',
        columns = [{"name": i, "id": i} for i in original_trips.columns],
        data = original_trips.to_dict('records'),
        page_current = 0,
        page_size = 10,
        page_action = 'custom',
        sort_action='custom',
        sort_mode='single',
        sort_by=[]
        )
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
     Input('distance-dropdown', 'value'),
     Input('vehicle-dropdown', 'value'),
     Input('button', 'n_clicks'),
     Input('origin_destination_dropdown', 'value')])
def set_display_children(year, month, station, distance, vehicle, n_clicks, orig_dest):
    if n_clicks is None:
        raise PreventUpdate
    else:
        return 'Top {}s of {} Trips within {} Miles of {} Subway Station in {} {}:'\
        .format(orig_dest, vehicles[vehicle], distance, station, month_dict[month], year)

@app.callback(
    [Output('map', 'figure'),
    Output('table', 'data'),
    Output('table', 'columns')],
    [Input('year-dropdown', 'value'),
     Input('month-dropdown', 'value'),
     Input('station-dropdown', 'value'),
     Input('distance-dropdown', 'value'),
     Input('vehicle-dropdown', 'value'),
     Input('button', 'n_clicks'),
     Input('table','page_current'),
     Input('table','page_size'),
     Input('table', 'sort_by')])
def set_map_display(year, month, station, distance, vehicle, n_clicks, page_current, page_size, sort_by):
    if n_clicks is None:
        raise PreventUpdate
    else:
        trips = generate_trips(year, month, station, distance, vehicle)
        if len(sort_by):
            trips = trips.sort_values(
                sort_by[0]['column_id'],
                ascending=sort_by[0]['direction'] == 'asc',
                inplace=False)
        trips_small = trips.iloc[page_current*page_size:(page_current+ 1)*page_size]
        data_dct = trips_small.to_dict('records')
        cols = [{"name": i, "id": i} for i in trips.columns]
        return generate_map(trips), data_dct, cols

if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)