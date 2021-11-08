from pydantic import BaseModel
from typing import List, Optional, Union
import datetime


class ReceiveTag(BaseModel):
    task_id: Optional[str] = None
    status: Optional[str] = None
    date_receive: datetime.datetime = datetime.datetime.today()
    info: Optional[Union[str, dict]] = None
    eta: int = 0


class ResultResponse(BaseModel):
    status: Optional[str] = None
    message: Optional[str] = None
    result: Optional[Union[int, str, float, list]] = None
    date_done: Optional[datetime.datetime] = None


class UserActivityLog(BaseModel):
    user_id: str = 'resi_strats'
    mode: str = 'test'
    date: str
    start_time: str
    duration: int
