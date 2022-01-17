from typing import Optional
from pydantic import BaseModel, Field

class PbpSchema(BaseModel):
    pass

def ResponseModel(data, message):
    return {
        "data": [data],
        "code": 200,
        "message": message,
    }