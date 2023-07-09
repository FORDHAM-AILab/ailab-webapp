import json
import traceback
from datetime import datetime

from fastapi import APIRouter, Depends
from sqlalchemy.sql import text
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
        async with helpers.sql_session_scope() as session:
            result = session.execute("""SELECT * FROM users WHERE internal_sub_id = :internal_sub_id""",
                                     {'internal_sub_id': internal_user.internal_sub_id})
            result = result[0]

        return ResultResponse(status_code=CONSTS.HTTP_200_OK, content=result,
                              date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))
    except Exception as e:
        return ResultResponse(status_code=CONSTS.HTTP_500_INTERNAL_SERVER_ERROR, message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))


@router.post('/update_user_profile')
async def update_user_profile(request:Request, internal_user: InternalUser = Depends(access_token_cookie_scheme)) -> ResultResponse:

    request = await request.json()
    user_profile = request['userProfile']
    # user_profile = {k: sqlquote(v) for k,v in user_profile.items()}

    try:
        async with helpers.sql_session_scope() as session:
            await session.execute(text("""UPDATE users SET username = :username,
                                                           program = :program,
                                                           cohort = :cohort,
                                                           first_name = :first_name,
                                                           last_name = :last_name,
                                                           area_of_interest = :area_of_interest
                                WHERE internal_sub_id = :internal_sub_id"""), {'username': user_profile['username'],
                                                                                 'program': user_profile['program'],
                                                                                 'cohort': user_profile['cohort'],
                                                                                 'first_name': user_profile['first_name'],
                                                                                 'last_name': user_profile['last_name'],
                                                                                 'area_of_interest': user_profile['area_of_interest'],
                                                                                 'internal_sub_id': internal_user.internal_sub_id})

        return ResultResponse(status_code=CONSTS.HTTP_200_OK, message=f"Successfully updated user profile: {internal_user.username}",
                              date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))
    except Exception as e:
        return ResultResponse(status_code=CONSTS.HTTP_500_INTERNAL_SERVER_ERROR, message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))