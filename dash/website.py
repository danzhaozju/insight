import dash
import dash_core_components as dcc
import dash_html_components as html
# pip install psycopg2-binary
import psycopg2
import pandas as pd
from dash.dependencies import Input, Output
from util import generate_table, year_month_options
#from config import host, port, dbname, user, password

# Connect to PostgreSQL database
host = "10.0.0.11"
port = "5432"
dbname = "insight"
user = "dan"
password = "zhaodan"
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
cur = conn.cursor()

# Create the dataframe bike
bike = pd.read_sql_query("SELECT * FROM bike LIMIT 3;", conn)


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
            html.H5('Year'),
            dcc.Dropdown(
                id = 'year-dropdown',
                options = [{'label':k, 'value':k} for k in year_month_options.keys()],
                value = 2019)
            ], className = "six columns"),

        html.Div([
            html.H5('Month'),
            dcc.Dropdown(id='month-dropdown')
            ], className = "six columns")
        ]),

    html.Div(id='display-selected-year-month')
])

@app.callback(
    Output('month-dropdown', 'options'),
    [Input('year-dropdown', 'value')])
def set_month_options(selected_year):
    return [{'label': i, 'value': i} for i in year_month_options[selected_year]]

@app.callback(
    Output('month-dropdown', 'value'),
    [Input('month-dropdown', 'options')])
def set_month_value(available_options):
    return available_options[0]['value']

@app.callback(
    Output('display-selected-year-month', 'children'),
    [Input('year-dropdown', 'value'),
     Input('month-dropdown', 'value')])
def set_display_children(selected_year, selected_month):
    return 'You have selected the year ({}) and the month ({}).'.format(selected_year, selected_month)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
