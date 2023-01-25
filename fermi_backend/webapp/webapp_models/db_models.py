import datetime
from typing import Optional

from pydantic import BaseModel


class InternalUser(BaseModel):
    external_sub_id: str
    internal_sub_id: str
    username: str
    email: str
    created_at: datetime.datetime
    program: Optional[str]
    cohort: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    area_of_interest: Optional[str]
