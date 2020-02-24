import dash
import dash_core_components as dcc
import dash_html_components as html
# pip install psycopg2-binary
import psycopg2
import pandas as pd
from dash.dependencies import Input, Output
from util import generate_table, year_month_options, month_dict, subway_stations_names, distance_range
from config import host, port, dbname, user, password
from plotly import express as px

# Connect to PostgreSQL database
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
cur = conn.cursor()
# Get access to mapbox
px.set_mapbox_access_token(open(".mapbox_token").read())

def generate_trips(year, month, station, distance, vehicle):
    trips = pd.read_sql_query("\
        SELECT year, month, start_station, latitude, longitude, end_latitude, end_longitude, count, avg_distance\
        FROM "+ vehicle +" \
        WHERE year = "+ str(year) +"\
        AND month = "+ str(month) +"\
        AND start_station = '"+ station +"'\
        AND avg_distance <= "+ str(distance) +"\
        ORDER BY count DESC;", conn)
    return trips

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
            html.Label('Available Vehicles'),
            dcc.Dropdown(
                id = 'vehicle-dropdown',
                options = [{'label':vehicles[k], 'value':k} for k in vehicles.keys()],
                value = 'bike')
            ], className = "six columns"),

        html.Div([
            html.Label('Year'),
            dcc.Dropdown(
                id = 'year-dropdown',
                options = [{'label':k, 'value':k} for k in year_month_options.keys()],
                value = 2019)
            ], className = "three columns"),

        html.Div([
            html.Label('Month'),
            dcc.Dropdown(id='month-dropdown')
            ], className = "three columns")
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

    html.Div([
        html.Button('Submit', id='button', style = {'width': '30%'})
        ], style={'textAlign':'center'}), 

    html.H5(id='display-selected-input', 
        style = {
            'textAlign': 'center'
        }),

    dcc.Graph(
        id = "map",
        figure = original_fig),

    generate_table(original_trips)
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
     Input('button', 'n_clicks')])
def set_display_children(year, month, station, distance, vehicle, n_clicks):
    if n_clicks is None:
        raise PreventUpdate
    else:
        return 'Top 10 Destinations of {} Trips within {} Miles of {} Subway Station in {} {}:'\
        .format(vehicles[vehicle], distance, station, month_dict[month], year)

@app.callback(
    Output('map', 'figure'),
    [Input('year-dropdown', 'value'),
     Input('month-dropdown', 'value'),
     Input('station-dropdown', 'value'),
     Input('distance-dropdown', 'value'),
     Input('vehicle-dropdown', 'value'),
     Input('button', 'n_clicks')])
def set_map_display(year, month, station, distance, vehicle, n_clicks):
    if n_clicks is None:
        raise PreventUpdate
    else:
        trips = generate_trips(year, month, station, distance, vehicle)
        return generate_map(trips)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)