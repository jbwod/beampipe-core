from fastapi import APIRouter

from .datasets import router as datasets_router
from .health import router as health_router
from .login import router as login_router
from .logout import router as logout_router
from .runs import router as runs_router
from .sources import router as sources_router
from .tasks import router as tasks_router
from .users import router as users_router
from .workflows import router as workflows_router

router = APIRouter(prefix="/v1")
router.include_router(health_router)
router.include_router(login_router)
router.include_router(logout_router)
router.include_router(users_router)
router.include_router(tasks_router)
router.include_router(datasets_router)
router.include_router(workflows_router)
router.include_router(runs_router)
router.include_router(sources_router)
