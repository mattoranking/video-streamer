from fastapi import APIRouter

from src.user.endpoints import router as user_router

router = APIRouter()

# /users
router.include_router(user_router, prefix="", tags=["users"])
