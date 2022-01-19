import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
from dash import dash_table
from helper import plot_game
from pymongo import MongoClient
from datetime import date


client = MongoClient('localhost', 27017)
db = client['NBA']

app = dash.Dash(__name__)

app.layout = html.Div([

    dcc.DatePickerSingle(
        id='date-picker',
    ),

    html.Div(id='mongo-datatable', children=[]),

    dcc.Dropdown(
            options=[],
            value=[],
            multi=True,
            id='game-picker'
        ),

    html.Div(id='mongo-Graph', children=[]),

    # activated once/week or when page refreshed
    dcc.Interval(id='interval_db', interval=86400000 * 7, n_intervals=0),

    html.Div(id="show-graphs", children=[]),
    html.Div(id="placeholder")

])

@app.callback(Output('mongo-Graph', 'children'),
              [Input('game-picker', 'value')])
def create_graph(value):
    print(value)
    return_graphs = []
    for i in range(len(value)):
        game_id = value[i]
        df = pd.DataFrame.from_records(db.historical_pbp_modelled.find({'GAME_ID':game_id}))
        print(df.shape)
        df = df.drop('_id', axis = 1)
        fig = plot_game(df)
        return_graphs.append(dcc.Graph(id=f'example_graph_{i}',figure = fig))

    return return_graphs

@app.callback(Output('game-picker', 'options'), 
              [Input("mongo-datatable", "children")])
def fill_game_select(children):
    data = children[0]['props']['data']

    options = []
    for i in range(len(data)):
        option = {'label': data[i]['MATCHUP'], 'value':data[i]['GAME_ID']}
        options.append(option)

    return options


@app.callback(Output('mongo-datatable', 'children'),
              [Input('date-picker', 'date')])
def populate_datatable(date):
    if date:
        # Convert the Collection (table) date to a pandas DataFrame
        df = pd.DataFrame.from_records(db.game_log.find({'GAME_DATE': date}))
        #Drop the _id column generated automatically by Mongo
        # df = df.drop('_id', axis = 1)

        return [
            dash_table.DataTable(
                id='my-table',
                columns=[{
                    'name': x,
                    'id': x,
                } for x in df.drop('_id', axis = 1).columns],
                data=df.drop('_id', axis = 1).to_dict('records'),
                editable=True,
                row_deletable=True,
                filter_action="native",
                filter_options={"case": "sensitive"},
                sort_action="native",  # give user capability to sort columns
                sort_mode="single",  # sort across 'multi' or 'single' columns
                page_current=0,  # page number that user is on
                style_cell={'textAlign': 'left', 'minWidth': '100px',
                            'width': '100px', 'maxWidth': '100px'},
            )
        ]
    else:
        return [html.H1(
            children='Please Insert Date'
        )]



if __name__ == '__main__':
    app.run_server(debug=True)