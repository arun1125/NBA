from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from src.historical_pbp.background import update_game_log
from src.database import db
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from pytz import utc


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

job = scheduler.add_job(myfunc, 'interval', hours=1, id='update_game_log')
scheduler.start()
# scheduler.shutdown()


@router.get("/")
async def read_root():
    return {"Message": "this is the historical play by play router"}


@router.get("/{game_id}")
async def fetch_game(game_id:int):
    data = db.historical_pbp.find({"GAME_ID":game_id})
    pbps_df = pd.DataFrame.from_records(data)
    return pbps_df.drop('_id', axis = 1).to_json(orient='records')


@router.get("/predictions/{game_id}")
async def fetch_preds(game_id:int):
    preds = pd.DataFrame.from_records(db.historical_pbp_modelled.find({"GAME_ID":game_id}))
    game_log = pd.DataFrame.from_records(db.game_log.find({'GAME_ID':game_id}))

    resp = preds.merge(game_log[['TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_DATE', 'GAME_ID', 'MATCHUP']],
           on=['GAME_ID'])

    return resp.drop('_id', axis = 1).to_json(orient='records')


