from fastcrud import FastCRUD

# /Users/jblackwo/beampipe-core/docs/user-guide/database/crud.md
from ..models.ledger import BatchRunRecord
from ..schemas.ledger import (
    BatchRunRecordCreateInternal,
    BatchRunRecordDelete,
    BatchRunRecordRead,
    BatchRunRecordUpdate,
    BatchRunRecordUpdateInternal,
)

CRUDBatchRunRecord = FastCRUD[
    BatchRunRecord,
    BatchRunRecordCreateInternal,
    BatchRunRecordDelete,
    BatchRunRecordUpdate,
    BatchRunRecordUpdateInternal,
    BatchRunRecordRead,
]
crud_batch_run_records = CRUDBatchRunRecord(BatchRunRecord)
