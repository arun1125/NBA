from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from src.database import db
import pandas as pd

router = APIRouter(
    prefix="/gamelog",
    tags=["gamelog"],
    responses={
        404:{"description":"Not found"},
        200:{"description":"data retrieved"}
        }
)

@router.get("/")
async def read_root():
    return {"Message": "this is the game log router"}


@router.get("/{game_date}")
async def fetch_games_on_date(game_date):
    data = db.game_log.find({"GAME_DATE":game_date})
    df = pd.DataFrame.from_records(data)
    resp = df[[
        'TEAM_ID', 'TEAM_ABBREVIATION', 
        'TEAM_NAME', 'GAME_ID', 
        'MATCHUP', 'GAME_DATE']].astype({'GAME_ID':int}).to_json(orient='records')
    return resp

