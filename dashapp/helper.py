import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def plot_game(df, game_info):
    # Create traces
    fig = go.Figure()

    fig.add_vline(x=720, line_width=3, line_dash="dash", line_color="green", name='Q2', annotation_text='Q2')
    fig.add_vline(x=1440, line_width=3, line_dash="dash", line_color="green", name='Q3', annotation_text='Q3')
    fig.add_vline(x=2160, line_width=3, line_dash="dash", line_color="green", name='Q4', annotation_text='Q4')

    if df['OT_ind'].nunique() > 1:
        for ot in df['OT_ind'].unique():
            temp = df[df['OT_ind'] == ot]
            if ot == 0:
                t = 0
                name = 'regulation'
            else:
                name = f'Over Time {ot}'
                t = 2880 + (ot - 1)*300
                fig.add_vline(x=t, line_width=3, 
                        line_dash="dash", line_color="green", 
                        name=f'OT_{ot}', annotation_text=f'OT_{ot}')
                
                
            fig.add_trace(go.Scatter(x = t+temp['time_remaining'][::-1], y=temp['preds_w_elo'],
                                    mode='lines', name=f'Win prob for {name}'))

            fig.add_trace(go.Scatter(x = t+temp['time_remaining'][::-1], y=temp['preds_wO_elo'],
                                    mode='lines', name=f'Win prob using elo for {name}'))
    else:
        fig.add_trace(go.Scatter(x = df['time_remaining'][::-1], y=df['preds_w_elo'],
                        mode='lines', name='Win prob using elo'))
                        
        fig.add_trace(go.Scatter(x = df['time_remaining'][::-1], y=df['preds_wO_elo'],
                        mode='lines', name='Win prob'))

    fig.update_layout(title=f"{game_info['Home'].iloc[0]} win probability against{game_info['Away'].iloc[0]}",
                   xaxis_title='Time Remaining',
                   yaxis_title='Probability')


    return fig