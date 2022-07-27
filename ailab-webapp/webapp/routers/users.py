import json
import traceback
from datetime import datetime

from fastapi import APIRouter, Depends

from .. import helpers, CONSTS
from ..auth import schemes as auth_schemes
from ..helpers import sqlquote
from ..webapp_models.db_models import InternalUser
from fastapi.requests import Request
from ..webapp_models.generic_models import ResultResponse

router = APIRouter(
    prefix="/users",
    tags=["users"]
)

access_token_cookie_scheme = auth_schemes.AccessTokenCookieBearer()


@router.get('/get_user_profile')
async def get_user_profile(internal_user: InternalUser = Depends(access_token_cookie_scheme)) -> ResultResponse:
    try:
        with helpers.mysql_session_scope() as session:
            result = session.execute(f"""SELECT * FROM fermi.users WHERE internal_sub_id = '{internal_user.internal_sub_id}'""")
            result = result[0]

        return ResultResponse(status=0, result=result,
                              date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))
    except Exception as e:
        return ResultResponse(status=-1, message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))


@router.post('/update_user_profile')
async def update_user_profile(request:Request, internal_user: InternalUser = Depends(access_token_cookie_scheme)) -> ResultResponse:

    request = await request.json()
    user_profile = request['userProfile']
    user_profile = {k: sqlquote(v) for k,v in user_profile.items()}

    try:
        with helpers.mysql_session_scope() as session:
            session.execute(f"""UPDATE users SET username = {user_profile['username']},
                                                           program = {user_profile['program']},
                                                           username = {user_profile['username']},
                                                           cohort = {user_profile['cohort']},
                                                           first_name = {user_profile['first_name']},
                                                           last_name = {user_profile['last_name']},
                                                           area_of_interest = {user_profile['area_of_interest']}
                                WHERE internal_sub_id = '{internal_user.internal_sub_id}'""")

        return ResultResponse(status=0, message=f"Transaction succeed for user: {internal_user.username}",
                              date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))
    except Exception as e:
        return ResultResponse(status=-1, message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))