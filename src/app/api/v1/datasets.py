from fastapi import APIRouter

# - GET /datasets - list discovered
# - GET /datasets/{id} - get dataset details
# - POST /datasets/{id}/trigger - manually trigger processing
# - GET /datasets/{id}/runs - get run history for dataset

router = APIRouter(tags=["datasets"])

