from fastapi import APIRouter

from .deployment_profiles import router as deployment_profiles_router
from .executions import router as executions_router
from .health import router as health_router
from .login import router as login_router
from .logout import router as logout_router
from .projects import router as projects_router
from .sources import router as sources_router
from .tasks import router as tasks_router
from .users import router as users_router

router = APIRouter(prefix="/v1")
router.include_router(health_router)
router.include_router(login_router)
router.include_router(logout_router)
router.include_router(users_router)
router.include_router(tasks_router)
router.include_router(executions_router)
router.include_router(deployment_profiles_router)
router.include_router(sources_router)
router.include_router(projects_router)

__all__ = ["router"]
