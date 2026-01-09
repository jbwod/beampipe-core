from fastcrud import FastCRUD
#/Users/jblackwo/beampipe-core/docs/user-guide/database/crud.md
from ..models.ledger import RunRecord
from ..schemas.ledger import (
    RunRecordCreateInternal,
    RunRecordDelete,
    RunRecordRead,
    RunRecordUpdate,
    RunRecordUpdateInternal,
)

CRUDRunRecord = FastCRUD[
    RunRecord,
    RunRecordCreateInternal,
    RunRecordDelete,
    RunRecordUpdate,
    RunRecordUpdateInternal,
    RunRecordRead,
]
crud_run_records = CRUDRunRecord(RunRecord)

