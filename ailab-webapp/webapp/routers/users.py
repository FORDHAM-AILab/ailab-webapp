from fastapi import APIRouter, Depends

router = APIRouter(
    prefix="/users",
    tags=["users"]
)

@router.post('/update_user_profile')
async def update_user_profile():
    pass