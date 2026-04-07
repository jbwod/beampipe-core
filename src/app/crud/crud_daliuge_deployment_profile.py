from fastcrud import FastCRUD

from ..models.daliuge import DaliugeDeploymentProfile
from ..schemas.daliuge import (
    DaliugeDeploymentProfileCreate,
    DaliugeDeploymentProfileDelete,
    DaliugeDeploymentProfileRead,
    DaliugeDeploymentProfileUpdate,
)

CRUDDaliugeDeploymentProfile = FastCRUD[
    DaliugeDeploymentProfile,
    DaliugeDeploymentProfileCreate,
    DaliugeDeploymentProfileDelete,
    DaliugeDeploymentProfileUpdate,
    DaliugeDeploymentProfileUpdate,
    DaliugeDeploymentProfileRead,
]
crud_daliuge_deployment_profile = CRUDDaliugeDeploymentProfile(DaliugeDeploymentProfile)
