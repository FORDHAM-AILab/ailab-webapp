from pydantic import BaseModel
from typing import List, Optional, Union
import datetime


class Data(BaseModel):
    data: Union[str, dict, List]
    weights: Optional[List] = None
    port_value: Optional[float] = None


class ReceiveTag(BaseModel):
    task_id: Optional[str] = None
    status: Optional[str] = None
    date_receive: datetime.datetime = datetime.datetime.today()
    info: Optional[Union[str, dict]] = None
    eta: int = 0


class ResultResponse(BaseModel):
    # TODO: unify status -> status_code, unify status code to http.status
    # status = -1 means error, >=0 means succeed.
    status_code: Optional[Union[str, int]] = None
    message: Optional[str] = None
    result: Optional[Union[float, int, str, list, dict]] = None
    date_done: Optional[datetime.datetime] = None
    debug: Optional[str] = None


class UserActivityLog(BaseModel):
    user_id: str = 'user'
    mode: str = 'test'
    date: str
    start_time: str
    duration: int


class CDSData(BaseModel):
    REGION: list = None
    INDUSTRY: list = None
    OBLIGATION_ASSETRANK: list = None
    CREDIT_EVENTS: list = None
    CURRENCY: list = None
    limit: int = 100

