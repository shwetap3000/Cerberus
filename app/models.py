from pydantic import BaseModel


class JobRequest(BaseModel):
    type: str
    payload: dict