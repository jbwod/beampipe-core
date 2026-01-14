from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/sources", response_class=HTMLResponse)
async def view_sources(request: Request) -> HTMLResponse:
    """testing adding a non API endpoint to the application."""
    html_content = """
<!DOCTYPE html>
<html>
<head>
    <title>Source Registry</title>
    <meta charset="utf-8">
</head>
<body>
    <h1>Source Registry</h1>
    <div>
        <label>Project Module: <input type="text" id="projectModule" placeholder="project-name"></label>
        <label>Enabled Only: <input type="checkbox" id="enabledOnly"></label>
        <button onclick="loadSources()">Refresh</button>
    </div>
    <div id="loading">Loading sources...</div>
    <div id="error" style="color: red; display: none;"></div>
    <table id="sourcesTable" border="1" style="border-collapse: collapse; margin-top: 20px; display: none;">
        <thead>
            <tr>
                <th>UUID</th>
                <th>Project Module</th>
                <th>Source Identifier</th>
                <th>Enabled</th>
                <th>Created At</th>
                <th>Updated At</th>
            </tr>
        </thead>
        <tbody id="sourcesBody">
        </tbody>
    </table>
    <div id="pagination" style="margin-top: 10px;"></div>
    
    <script>
        async function loadSources(page = 1) {
            const projectModule = document.getElementById('projectModule').value;
            const enabledOnly = document.getElementById('enabledOnly').checked;
            const itemsPerPage = 100;
            
            document.getElementById('loading').style.display = 'block';
            document.getElementById('error').style.display = 'none';
            document.getElementById('sourcesTable').style.display = 'none';
            
            try {
                let url = '/api/v1/sources?page=' + page + '&items_per_page=' + itemsPerPage;
                if (projectModule) {
                    url += '&project_module=' + encodeURIComponent(projectModule);
                }
                if (enabledOnly) {
                    url += '&enabled=true';
                }
                
                const response = await fetch(url);
                if (!response.ok) {
                    throw new Error('Failed to load sources: ' + response.statusText);
                }
                
                const data = await response.json();
                
                const tbody = document.getElementById('sourcesBody');
                tbody.innerHTML = '';
                
                if (data.data && data.data.length > 0) {
                    data.data.forEach(source => {
                        const row = tbody.insertRow();
                        row.insertCell(0).textContent = source.uuid;
                        row.insertCell(1).textContent = source.project_module;
                        row.insertCell(2).textContent = source.source_identifier;
                        row.insertCell(3).textContent = source.enabled ? 'Yes' : 'No';
                        row.insertCell(4).textContent = source.created_at || 'N/A';
                        row.insertCell(5).textContent = source.updated_at || 'N/A';
                    });
                    
                    document.getElementById('sourcesTable').style.display = 'table';
   
                    const paginationDiv = document.getElementById('pagination');
                    paginationDiv.innerHTML = '';
                    if (data.total_pages > 1) {
                        if (data.page > 1) {
                            const prevBtn = document.createElement('button');
                            prevBtn.textContent = 'Previous';
                            prevBtn.onclick = () => loadSources(data.page - 1);
                            paginationDiv.appendChild(prevBtn);
                        }
                        paginationDiv.appendChild(document.createTextNode(' Page ' + data.page + ' of ' + data.total_pages + ' '));
                        if (data.page < data.total_pages) {
                            const nextBtn = document.createElement('button');
                            nextBtn.textContent = 'Next';
                            nextBtn.onclick = () => loadSources(data.page + 1);
                            paginationDiv.appendChild(nextBtn);
                        }
                    }
                    paginationDiv.appendChild(document.createTextNode(' (Total: ' + data.total + ' sources)'));
                } else {
                    document.getElementById('error').textContent = 'No sources found.';
                    document.getElementById('error').style.display = 'block';
                }
            } catch (error) {
                document.getElementById('error').textContent = 'Error: ' + error.message;
                document.getElementById('error').style.display = 'block';
            } finally {
                document.getElementById('loading').style.display = 'none';
            }
        }
        window.onload = function() {
            loadSources();
        };
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_content)

