from fastapi import APIRouter

router = APIRouter(prefix="/sources", tags=["sources"])

@router.get("")
async def list_sources():
    pass

@router.post("")
async def register_source():
    pass

@router.get("/{source_id}")
async def get_source(source_id):
    pass

@router.patch("/{source_id}")
async def update_source(source_id):
    pass
