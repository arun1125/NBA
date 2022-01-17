import motor.motor_asyncio


client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/')
db = client['NBA']
historical_pbp = df.historical_pbp


async def fetch_game(game_id):
    document = await historical_pbp.find_one({"GAME_ID":game_id})
    return document
    
# async def fetch_all_games():
    
