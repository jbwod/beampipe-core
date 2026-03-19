from fastcrud import FastCRUD

from ..models.daliuge import DaliugeExecutionProfile
from ..schemas.daliuge import (
    DaliugeExecutionProfileCreate,
    DaliugeExecutionProfileDelete,
    DaliugeExecutionProfileRead,
    DaliugeExecutionProfileUpdate,
)

CRUDDaliugeExecutionProfile = FastCRUD[
    DaliugeExecutionProfile,
    DaliugeExecutionProfileCreate,
    DaliugeExecutionProfileDelete,
    DaliugeExecutionProfileUpdate,
    DaliugeExecutionProfileUpdate,
    DaliugeExecutionProfileRead,
]
crud_daliuge_execution_profile = CRUDDaliugeExecutionProfile(DaliugeExecutionProfile)
