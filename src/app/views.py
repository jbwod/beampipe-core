from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/sources", response_class=HTMLResponse)
async def view_sources(request: Request) -> HTMLResponse:
    """testing adding a non API endpoint to the application."""
    html_content = """
    <html>
    <body>
    <h1>Sources</h1>
    <p>empty</p>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)
