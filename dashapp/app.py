import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
from dash import dash_table
from helper import plot_game
import boto3
from boto3.dynamodb.conditions import Key

dynamoDB = boto3.resource('dynamodb', region_name = 'us-east-2')
game_log_db = dynamoDB.Table('game_log')
historical_pbp_modelled_db = dynamoDB.Table('historical_pbp_modelled')

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

    return_graphs = []
    for i in range(len(value)):
        game_id = value[i]
        df = pd.DataFrame(historical_pbp_modelled_db.query(KeyConditionExpression=Key('GAME_ID').eq(game_id))['Items'])

        game_info = pd.DataFrame(game_log_db.query(
            IndexName="GameIdIndex",
            KeyConditionExpression=Key('GAME_ID').eq(game_id))['Items'])

        fig = plot_game(df, game_info)
        return_graphs.append(dcc.Graph(id=f'example_graph_{i}',figure = fig))

    return return_graphs

@app.callback(Output('game-picker', 'options'), 
              [Input("mongo-datatable", "children")])
def fill_game_select(children):

    if 'data' in children[0]['props']:
        data = children[0]['props']['data']

        options = []
        for i in range(len(data)):
            option = {'label': data[i]['MATCHUP'], 'value':data[i]['GAME_ID']}
            options.append(option)

        return options
    else:
        return []


@app.callback(Output('mongo-datatable', 'children'),
              [Input('date-picker', 'date')])
def populate_datatable(date):
    if date:
        data = game_log_db.query(KeyConditionExpression=Key('GAME_DATE').eq(date))['Items']
        df = pd.DataFrame.from_records(data)

        return [
            dash_table.DataTable(
                id='my-table',
                columns=[{
                    'name': x,
                    'id': x,
                } for x in df.columns],
                data=df.to_dict('records'),
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