from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
# from NBA.app.src.historical_pbp.background import del_models
from src.historical_pbp.background import update_game_log, load_models, del_models
from src.database import historical_pbp_modelled_db, game_log_db
from boto3.dynamodb.conditions import Key
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from pytz import utc
from time import sleep


router = APIRouter(
    prefix="/historical_pbp",
    tags=["historical_pbp"],
    responses={
        404:{"description":"Not found"},
        200:{"description":"data retrieved"}
        }
)


scheduler = BackgroundScheduler()
scheduler.configure(timezone=utc)

def myfunc():
    update_game_log()

job = scheduler.add_job(myfunc, 'interval', minutes = 1, id='update_game_log')
scheduler.start()
# scheduler.shutdown()




@router.get("/")
async def read_root():
    load_models()
    print('models are loaded')
    sleep(10)
    del_models()
    return {"Message": "this is the historical play by play router"}


@router.get("/{game_id}")
async def fetch_game(game_id:int):
    data = historical_pbp_modelled_db.query(KeyConditionExpression=Key('GAME_ID').eq(game_id))['Items']
    pbps_df = pd.DataFrame.from_records(data)
    return pbps_df.to_json(orient='records')


@router.get("/predictions/{game_id}")
async def fetch_preds(game_id:int):
    preds = pd.DataFrame(historical_pbp_modelled_db.query(KeyConditionExpression=Key('GAME_ID').eq(game_id))['Items'])

    game_log = pd.DataFrame(game_log_db.query(
        IndexName="GameIdIndex",
        KeyConditionExpression=Key('GAME_ID').eq(game_id))['Items'])

    resp = preds.merge(game_log[['TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_DATE', 'GAME_ID', 'MATCHUP']],
           on=['GAME_ID'])

    return resp.to_json(orient='records')


