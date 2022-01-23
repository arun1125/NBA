from src.database import db
import pandas as pd
import numpy as np
from nba_api.stats.endpoints import playbyplayv2, leaguegamelog, leaguegamefinder
from src.historical_pbp.helper import feature_engineer
from src.utilities import read_url_to_csv, elo_url
from time import sleep
import tensorflow as tf

def update_game_log():
    print('getting nba_game log')
    gl = pd.DataFrame.from_records(db.game_log.find())
    lgl = leaguegamelog.LeagueGameLog().get_data_frames()[0].astype({'GAME_ID':int})

    games_to_update = set(lgl['GAME_ID']).difference(set(gl['GAME_ID']))
    
    if games_to_update:
        print(f'there are {len(games_to_update)} games to update')
        games_to_update_df = lgl[(lgl['GAME_ID'].isin(games_to_update))&\
            (~lgl['MATCHUP'].str.contains('@'))]
        
        games_to_update_df[['Home', 'Away']] = games_to_update_df['MATCHUP'].str.split('vs.', expand=True)
        games_to_update_df.loc[:, 'Home'] = games_to_update_df['Home'].str.strip()
        games_to_update_df.loc[:, 'home_team_win'] = games_to_update_df['WL'].replace({'W':1, 'L':0})
        games_to_update_df = games_to_update_df.dropna(subset=['home_team_win'])
        print('finished preprocessing the game log')

        elo = read_url_to_csv(elo_url)
        elo = elo[elo['date'] > '2012-01-01']

        elo.loc[:, 'elo_difference'] = np.abs(elo['elo1_pre'] - elo['elo2_pre'])

        elo = elo[['date', 'team1', 'elo1_pre', 'elo2_pre', 'elo_difference']]

        elo['team1'] = elo['team1'].replace({'BRK':'BKN',
                                            'PHO':'PHX',
                                            'CHO':'CHA',})

        print('preprocessed elo')

        df = games_to_update_df.merge(elo, 
                             left_on=['GAME_DATE', 'Home'], 
                             right_on = ['date', 'team1'])[['GAME_ID', 'elo1_pre', 'elo2_pre', 'home_team_win', 'MATCHUP']]
        
        print('starting to get play by play for remaining games')
        pbps = []
        for g_id in df['GAME_ID'].unique():
            pbps.append(playbyplayv2.PlayByPlayV2(f'00{g_id}').get_data_frames()[0])
            sleep(0.5)
        if len(pbps) == 0:
            return
        print('merge all play by plays into one game log')
        pbp_df = pd.concat(pbps).astype({'GAME_ID':int})
        print('create our features for each game')
        pbp_df_prepped = pbp_df.groupby('GAME_ID').apply(feature_engineer).reset_index(drop=True)

        print('merge our game log with elo and feature engineered play by play data')
        final_df = pbp_df_prepped.merge(df, on=['GAME_ID'])

        wEloCols = ['home_poss', 'diff', 'time_remaining', 'OT_ind', 'elo1_pre', 'elo2_pre']
        wOEloCols = ['home_poss', 'diff', 'time_remaining', 'OT_ind']

        print('run model to get predictions')
        model = tf.keras.models.load_model('./app/src/Models/TF_model_w_elo.h5')
        model_wO_elo = tf.keras.models.load_model('./app/src/Models/TF_model_wO_elo.h5')

        final_df.loc[:, 'preds_w_elo'] = model.predict_on_batch(final_df[wEloCols])
        final_df.loc[:, 'preds_wO_elo'] = model_wO_elo.predict_on_batch(final_df[wOEloCols])

        # db.historical_pbp_modelled.insert_many(final_df.to_dict('records'))
        # db.game_log.insert_many(games_to_update_df.to_dict('records'))
        # db.historical_pbp.insert_many(pbp_df.to_dict('records'))

        print('Games Updated!')

    else:
        print('Games didnt Update')

        