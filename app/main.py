import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.game_log import main as game_log_main
from src.historical_pbp import main as historical_pbp_main
from src.historical_pbp.background import update_game_log


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(game_log_main.router)
app.include_router(historical_pbp_main.router)

from models.pbp import ResponseModel

@app.get("/", tags=["Root"])
async def read_root():
    return {"Hello":"World"}
    


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)