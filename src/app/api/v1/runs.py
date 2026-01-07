from fastapi import APIRouter

# - GET /runs - list runs with filtering
# - GET /runs/{id} - get run details
# - POST /runs/{id}/retry - retry failed run
# - GET /runs/{id}/provenance - get run provenance

router = APIRouter(tags=["runs"])
