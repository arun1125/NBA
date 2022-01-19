import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def plot_game(df):
    # Create traces
    fig = go.Figure()
    fig.add_trace(go.Scatter(x = df['time_remaining'][::-1], y=df['preds_w_elo'],
                        mode='lines', name='win prob with elo'))

    fig.add_trace(go.Scatter(x = df['time_remaining'][::-1], y=df['preds_wO_elo'],
                        mode='lines', name='win prob'))

    fig.add_vline(x=720, line_width=3, line_dash="dash", line_color="green", name='Q1')
    fig.add_vline(x=1440, line_width=3, line_dash="dash", line_color="green", name='Q1')
    fig.add_vline(x=2160, line_width=3, line_dash="dash", line_color="green", name='Q1')



    return fig