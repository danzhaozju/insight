import dash
import dash_core_components as dcc
import dash_html_components as html
# pip install psycopg2-binary
import psycopg2
import pandas as pd
from dash.dependencies import Input, Output
#from config import host, port, dbname, user, password

host = "10.0.0.11"
port = "5432"
dbname = "insight"
user = "dan"
password = "zhaodan"
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
cur = conn.cursor()

print("start UI")

bike = pd.read_sql_query("SELECT * FROM bike LIMIT 3;", conn)


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),
])

if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
