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

    <div id="loginSection" style="margin-bottom: 20px; padding: 10px; border: 1px solid #ccc;">
        <h3>Authentication</h3>
        <div>
            <label>Username: <input type="text" id="username"></label>
            <label>Password: <input type="password" id="password"></label>
            <button onclick="login()">Login</button>
            <button onclick="logout()">Logout</button>
            <span id="authStatus" style="margin-left: 10px;"></span>
        </div>
    </div>

    <div id="addSourceSection" style="margin-bottom: 20px; padding: 10px; border: 1px solid #ccc;">
        <h3>Add New Source</h3>
        <div>
            <label>Project Module: <input type="text" id="newProjectModule" placeholder="e.g. wallaby"></label>
            <label>Source Identifier: <input type="text" id="newSourceIdentifier" placeholder="HIPASSJ1303+07"></label>
            <label>Enabled: <input type="checkbox" id="newEnabled"></label>
            <button onclick="addSource()">Add Source</button>
        </div>
    </div>

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
                <th>Actions</th>
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
                        const enabledCell = row.insertCell(3);
                        enabledCell.textContent = source.enabled ? 'Yes' : 'No';
                        row.insertCell(4).textContent = source.created_at || 'N/A';
                        row.insertCell(5).textContent = source.updated_at || 'N/A';

                        const actionsCell = row.insertCell(6);
                        const toggleBtn = document.createElement('button');
                        toggleBtn.textContent = source.enabled ? 'Disable' : 'Enable';
                        toggleBtn.onclick = () => toggleSource(source.uuid, !source.enabled);
                        actionsCell.appendChild(toggleBtn);

                        const deleteBtn = document.createElement('button');
                        deleteBtn.textContent = 'Delete';
                        deleteBtn.onclick = () => deleteSource(source.uuid);
                        deleteBtn.style.marginLeft = '5px';
                        actionsCell.appendChild(deleteBtn);
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
                        const pageText = ' Page ' + data.page + ' of ' + data.total_pages + ' ';
                        paginationDiv.appendChild(document.createTextNode(pageText));
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
        function getAuthToken() {
            return localStorage.getItem('access_token');
        }

        function setAuthToken(token) {
            if (token) {
                localStorage.setItem('access_token', token);
                document.getElementById('authStatus').textContent = 'Authenticated';
                document.getElementById('authStatus').style.color = 'green';
            } else {
                localStorage.removeItem('access_token');
                document.getElementById('authStatus').textContent = 'Not authenticated';
                document.getElementById('authStatus').style.color = 'red';
            }
        }

        async function login() {
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;

            if (!username || !password) {
                alert('Please enter username and password');
                return;
            }

            try {
                const formData = new URLSearchParams();
                formData.append('username', username);
                formData.append('password', password);

                const response = await fetch('/api/v1/login', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                    },
                    body: formData
                });

                if (!response.ok) {
                    const error = await response.json();
                    throw new Error(error.detail || 'Login failed');
                }

                const data = await response.json();
                setAuthToken(data.access_token);
                document.getElementById('username').value = '';
                document.getElementById('password').value = '';
            } catch (error) {
                alert('Login error: ' + error.message);
            }
        }

        function logout() {
            setAuthToken(null);
        }

        async function addSource() {
            const token = getAuthToken();
            if (!token) {
                alert('Please login first');
                return;
            }

            const projectModule = document.getElementById('newProjectModule').value;
            const sourceIdentifier = document.getElementById('newSourceIdentifier').value;
            const enabled = document.getElementById('newEnabled').checked;

            if (!projectModule || !sourceIdentifier) {
                alert('Please enter project module and source identifier');
                return;
            }

            try {
                const response = await fetch('/api/v1/sources', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer ' + token
                    },
                    body: JSON.stringify({
                        project_module: projectModule,
                        source_identifier: sourceIdentifier,
                        enabled: enabled
                    })
                });

                if (!response.ok) {
                    const error = await response.json();
                    throw new Error(error.detail || 'Failed to add source');
                }

                document.getElementById('newProjectModule').value = '';
                document.getElementById('newSourceIdentifier').value = '';
                document.getElementById('newEnabled').checked = false;
                loadSources();
            } catch (error) {
                alert('Error adding source: ' + error.message);
            }
        }

        async function toggleSource(sourceId, newEnabled) {
            const token = getAuthToken();
            if (!token) {
                alert('Please login first');
                return;
            }

            if (!confirm('Are you sure you want to ' + (newEnabled ? 'enable' : 'disable') + ' this source?')) {
                return;
            }

            try {
                const response = await fetch('/api/v1/sources/' + sourceId, {
                    method: 'PATCH',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer ' + token
                    },
                    body: JSON.stringify({
                        enabled: newEnabled
                    })
                });

                if (!response.ok) {
                    const error = await response.json();
                    throw new Error(error.detail || 'Failed to update source');
                }

                loadSources();
            } catch (error) {
                alert('Error updating source: ' + error.message);
            }
        }

        async function deleteSource(sourceId) {
            const token = getAuthToken();
            if (!token) {
                alert('Please login first');
                return;
            }

            if (!confirm('Are you sure you want to delete this source? This action cannot be undone.')) {
                return;
            }

            try {
                const response = await fetch('/api/v1/sources/' + sourceId, {
                    method: 'DELETE',
                    headers: {
                        'Authorization': 'Bearer ' + token
                    }
                });

                if (!response.ok) {
                    const error = await response.text();
                    throw new Error(error || 'Failed to delete source');
                }

                loadSources();
            } catch (error) {
                alert('Error deleting source: ' + error.message);
            }
        }

        window.onload = function() {
            const token = getAuthToken();
            if (token) {
                setAuthToken(token);
            } else {
                setAuthToken(null);
            }
            loadSources();
        };
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_content)
