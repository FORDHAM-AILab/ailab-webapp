import traceback
from fastapi import APIRouter
from datetime import datetime
from .. import CONSTS
from ..exceptions import router_error_handler
from ..helpers import round_result, sql_session_scope, parse_sql_results
from fermi_backend.models.Portfolio import Portfolio
from ..webapp_models.generic_models import ResultResponse, Data, CDSData
from ..CONSTS import ANALYTICS_DECIMALS
import pandas as pd

router = APIRouter(
    prefix="/filesys",
    tags=["filesys"]
)


@router.get("/search_file_by_name/{name}")
@router_error_handler
async def search_file_by_name(name: str):
    async with sql_session_scope() as session:
        results = session.execute("SELECT * FROM fermi.filesys_idx WHERE name=:name;", {"name": name})
        results = parse_sql_results(results, orient='records')
        file_info = results[0]

    return ResultResponse(content=file_info, status_code=CONSTS.HTTP_200_OK, message=f"Successfully queried data",
                          date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))
