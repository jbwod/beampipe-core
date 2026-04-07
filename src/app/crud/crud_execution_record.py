from fastcrud import FastCRUD

from ..models.ledger import BatchExecutionRecord
from ..schemas.ledger import (
    BatchExecutionRecordCreateInternal,
    BatchExecutionRecordDelete,
    BatchExecutionRecordRead,
    BatchExecutionRecordUpdate,
    BatchExecutionRecordUpdateInternal,
)

CRUDExecutionRecord = FastCRUD[
    BatchExecutionRecord,
    BatchExecutionRecordCreateInternal,
    BatchExecutionRecordDelete,
    BatchExecutionRecordUpdate,
    BatchExecutionRecordUpdateInternal,
    BatchExecutionRecordRead,
]
crud_batch_execution_records = CRUDExecutionRecord(BatchExecutionRecord)
