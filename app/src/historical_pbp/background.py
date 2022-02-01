import pandas as pd
import numpy as np
from nba_api.stats.endpoints import playbyplayv2, leaguegamelog, leaguegamefinder
from src.historical_pbp.helper import feature_engineer
from src.utilities import read_url_to_csv, elo_url
from time import sleep
import tensorflow as tf
import json
import os
from decimal import Decimal
from src.database import historical_pbp_modelled_db, game_log_db
import boto3

def load_all_game_log():
    response = game_log_db.scan()
    result = response['Items']

    while 'LastEvaluatedKey' in response:
        response = game_log_db.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        result.extend(response['Items'])
    return pd.DataFrame(result)

def process_new_games(new_games):
    new_games[['Home', 'Away']] = new_games['MATCHUP'].str.split('vs.', expand=True)
    new_games.loc[:, 'Home'] = new_games['Home'].str.strip()
    new_games.loc[:, 'home_team_win'] = new_games['WL'].replace({'W':1, 'L':0})
    new_games = new_games.dropna(subset=['home_team_win'])
    return new_games

def preprocess_elo(elo_df):
    elo_df = elo_df[elo_df['date'] > '2012-01-01']
    elo_df.loc[:, 'elo_difference'] = np.abs(elo_df['elo1_pre'] - elo_df['elo2_pre'])
    elo_df = elo_df[['date', 'team1', 'elo1_pre', 'elo2_pre', 'elo_difference']]
    elo_df['team1'] = elo_df['team1'].replace({'BRK':'BKN', 'PHO':'PHX', 'CHO':'CHA',})
    return elo_df

def upload_data_to_dynamoDB(df, db, check_col):
    df_dict = df[~df[check_col].isna()].to_dict('records')
    df_json = [json.loads(json.dumps(item), parse_float=Decimal) for item in df_dict]
    with db.batch_writer() as batch:
        for i in range(len(df_json)):
            batch.put_item(Item = df_json[i])

    print('uploaded data!')

def load_models():
    client = boto3.client('s3')
    
    client.download_file('arun-nba','Models/TF_model_wO_elo.h5','model_wO_elo.h5')
    client.download_file('arun-nba','Models/TF_model_w_elo.h5','model_w_elo.h5')
    
    model_wO_elo = tf.keras.models.load_model('model_wO_elo.h5')
    model_w_elo = tf.keras.models.load_model('model_w_elo.h5')

    return model_w_elo, model_wO_elo
    
def del_models():
    if os.path.exists('model_wO_elo.h5'):
        os.remove('model_wO_elo.h5')

    if os.path.exists('model_w_elo.h5'):
        os.remove('model_w_elo.h5')

    print('models deleted!')


def update_game_log():
    print('getting nba_game log')
    gl = load_all_game_log()
    lgl = leaguegamelog.LeagueGameLog().get_data_frames()[0].astype({'GAME_ID':int})

    games_to_update = set(lgl['GAME_ID']).difference(set(gl['GAME_ID']))
    
    if games_to_update:
        print(f'there are {len(games_to_update)} games to update')

        games_to_update_df = lgl[(lgl['GAME_ID'].isin(games_to_update))&\
            (~lgl['MATCHUP'].str.contains('@'))]
        
        games_to_update_df = process_new_games(games_to_update_df)
        print('finished preprocessing the game log')

        elo = read_url_to_csv(elo_url)
        elo = preprocess_elo(elo)
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
        pbp_df_prepped['PLAY_NUMBER'] = pbp_df_prepped.groupby('GAME_ID').cumcount()

        print('merge our game log with elo and feature engineered play by play data')
        final_df = pbp_df_prepped.merge(df, on=['GAME_ID'])

        wEloCols = ['home_poss', 'diff', 'time_remaining', 'OT_ind', 'elo1_pre', 'elo2_pre']
        wOEloCols = ['home_poss', 'diff', 'time_remaining', 'OT_ind']

        print('run model to get predictions')
        # model = tf.keras.models.load_model('./app/src/Models/TF_model_w_elo.h5')
        # model_wO_elo = tf.keras.models.load_model('./app/src/Models/TF_model_wO_elo.h5')
        model_w_elo, model_wO_elo = load_models()

        final_df.loc[:, 'preds_w_elo'] = model_w_elo.predict_on_batch(final_df[wEloCols])
        final_df.loc[:, 'preds_wO_elo'] = model_wO_elo.predict_on_batch(final_df[wOEloCols])

        # upload_data_to_dynamoDB(games_to_update_df, game_log_db, 'home_team_win')
        # upload_data_to_dynamoDB(final_df, historical_pbp_modelled_db, 'home_team_win')
        
        print('Games Updated!')
        del_models()

    else:
        print('Games didnt Update')

        