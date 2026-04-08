from fastcrud import FastCRUD

from ..models.daliuge import DaliugeDeploymentProfile
from ..schemas.daliuge import (
    DaliugeDeploymentProfileDbCreate,
    DaliugeDeploymentProfileDelete,
    DaliugeDeploymentProfileStored,
    DaliugeDeploymentProfileUpdate,
)

CRUDDaliugeDeploymentProfile = FastCRUD[
    DaliugeDeploymentProfile,
    DaliugeDeploymentProfileDbCreate,
    DaliugeDeploymentProfileDelete,
    DaliugeDeploymentProfileUpdate,
    DaliugeDeploymentProfileUpdate,
    DaliugeDeploymentProfileStored,
]
crud_daliuge_deployment_profile = CRUDDaliugeDeploymentProfile(DaliugeDeploymentProfile)
