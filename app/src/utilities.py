import pandas as pd
import requests
import io

def read_url_to_csv(url):
    r = requests.get(url)
    data = io.StringIO(r.text)
    df = pd.read_csv(data, sep=",")
    return df

elo_url = 'https://projects.fivethirtyeight.com/nba-model/nba_elo.csv'

latest_raptor_url = 'https://projects.fivethirtyeight.com/nba-model/2022/latest_RAPTOR_by_team.csv'