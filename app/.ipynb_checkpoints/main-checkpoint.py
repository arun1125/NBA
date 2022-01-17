import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

from database import fetch_game


@app.get("/", tags=["Root"])
async def read_root():
    return {"Hello":"world"}


@app.get("/api/{game_id}")
async def get_game(game_id):
    resp = await fetch_game(game_id)
    if resp:
        return resp
    raise HTTPException(404, "there is no game with this id")
    


if __name__ == "__main__":
    uvicorn.run("server.app:app", host="0.0.0.0", port=8000, reload=True)