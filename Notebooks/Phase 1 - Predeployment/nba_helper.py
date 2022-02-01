import sqlite3 as sq
import pandas as pd
import requests
import io


connection = sq.connect("../Data/basketball.sqlite")

def read_data(table, con=connection):
    return pd.read_sql(f"""SELECT * FROM {table}""", con)


all_tables = pd.read_sql("""SELECT *
                        FROM sqlite_master
                        WHERE type='table';""", connection)


def read_url_to_csv(url):
    r = requests.get(url)
    data = io.StringIO(r.text)
    df = pd.read_csv(data, sep=",")
    return df

elo_url = 'https://projects.fivethirtyeight.com/nba-model/nba_elo.csv'

latest_raptor_url = 'https://projects.fivethirtyeight.com/nba-model/2022/latest_RAPTOR_by_team.csv'


def upload_table(df, table_name, table_path = '../Data/NBA.sqlite', if_exists = 'append'):
    conn = sq.connect(table_path)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS NBA")

    df.to_sql(table_name,
             conn,
             if_exists=if_exists,
             index=False)
    conn.commit()
    conn.close()