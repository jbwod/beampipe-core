from fastapi import APIRouter

# - GET /workflows - list workflows
# - POST /workflows - submit workflow
# - GET /workflows/{id} - get workflow status
# - DELETE /workflows/{id} - cancel workflow
# - GET /workflows/{id}/provenance - get provenance

router = APIRouter(tags=["workflows"])

