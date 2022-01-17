from datetime import datetime
from nba_api.stats.endpoints import playbyplayv2


def str_to_time(str1):
    time_ = datetime.strptime(str1, "%M:%S")
    return time_.second + time_.minute*60


def home_poss(d):
    if (d['home_true'] == 1) & (d['visitor_true']==0):
        return 1
    elif (d['home_true'] == 0) & (d['visitor_true']==1):
        return 0
    else:
        if d['block'] or d['steal']:
            return 1
        else:
            return 0
        
def find_seconds_left(x):
    if x == 1:
        return 3*720
    elif x == 2: 
        return 2*720
    elif x == 3:
        return 720
    else:
        return 0
    
def load_game(game_id):
    pbp = playbyplayv2.PlayByPlayV2(game_id).get_data_frames()[0]
    home_team_name = pbp['PLAYER1_TEAM_ABBREVIATION'].dropna().iloc[0]
    return pbp, home_team_name

def feature_engineer(pbp):
    pbp[['home_true', 'visitor_true']] = pbp[['HOMEDESCRIPTION','VISITORDESCRIPTION']].notnull().astype(int)
    pbp['block'] = pbp['HOMEDESCRIPTION'].str.contains("BLOCK").fillna(False)
    pbp['steal'] = pbp['HOMEDESCRIPTION'].str.contains("STEAL").fillna(False)

    pbp['home_poss'] = pbp.apply(home_poss, axis = 1)
    pbp['diff'] = pbp['SCOREMARGIN'].ffill().fillna(0).replace({'TIE':0}).astype(int)
    pbp['OT_ind'] = (pbp['PERIOD']-4).clip(lower=0)

    pbp['seconds'] = pbp['PCTIMESTRING'].apply(str_to_time)
    pbp['seconds_left_in_game_from_quarter'] = pbp['PERIOD'].apply(find_seconds_left)
    pbp['time_remaining'] = pbp['seconds'] + pbp['seconds_left_in_game_from_quarter']

    game = pbp[['home_poss', 'diff', 'time_remaining', 'OT_ind']]
    
    return game