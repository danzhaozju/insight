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

def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )

bike = pd.read_sql_query("SELECT * FROM bike LIMIT 3;", conn)


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

app.layout = html.Div(children=[
	html.H1(
        children = 'First & Last Mile',
        style = {
            'textAlign': 'center'
        }
    ),
    html.H2(
        children = 'Input:'
    ),

    html.Hr(),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),
    generate_table(bike)
])

if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
