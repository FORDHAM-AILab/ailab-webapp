import typing

import starlette.responses
from pydantic import BaseModel
from typing import List, Optional, Union
import datetime

from starlette.background import BackgroundTask


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
    status_code: str
    content: Optional[typing.Any] = None
    message: Optional[str] = None
    date_done: Optional[str] = None
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


class DataIntegrator(BaseModel):
    table_names: List[str]
    join_types: List[str]
    join_cols: List[str]
    identifier: dict
    start_date: str
    end_date: str
    table_columns: dict
