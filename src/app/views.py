from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/sources", response_class=HTMLResponse)
async def view_sources(request: Request) -> HTMLResponse:
    """Very simple HTML page for interacting with the source registry."""
    html_content = """
    <html>
      <head>
        <title>Sources</title>
      </head>
      <body>
        <h1>Source Registry</h1>

        <h2>Login</h2>
        <p>Enter API credentials to enable adding sources.</p>
        <form id="login-form">
          <label>Username:
            <input type="text" id="login-username" value="admin" />
          </label>
          <label>Password:
            <input type="password" id="login-password" value="Str1ngst!" />
          </label>
          <button type="submit">Login</button>
          <span id="login-status"></span>
        </form>

        <h2>Add Source</h2>
        <form id="add-source-form">
          <label>Project module:
            <input type="text" id="project-module" value="wallaby_hires" />
          </label>
          <label>Source identifier:
            <input type="text" id="source-identifier" placeholder="HIPASSJXXXX±YY" />
          </label>
          <label>Enabled:
            <input type="checkbox" id="source-enabled" checked />
          </label>
          <button type="submit">Add Source</button>
          <span id="add-source-status"></span>
        </form>

        <h2>Sources</h2>
        <table border="1">
          <thead>
            <tr>
              <th>UUID</th>
              <th>Project</th>
              <th>Identifier</th>
              <th>Enabled</th>
              <th>Last Checked</th>
              <th>Metadata</th>
            </tr>
          </thead>
          <tbody id="sources-table-body">
          </tbody>
        </table>

        <h2>Metadata</h2>
        <p id="metadata-header">Select a source to view its metadata.</p>
        <table border="1">
          <thead>
            <tr>
              <th>SBID</th>
              <th>Dataset ID</th>
              <th>Visibility Filename</th>
              <th>Staged URL</th>
              <th>Evaluation File</th>
              <th>RA</th>
              <th>DEC</th>
              <th>Vsys</th>
            </tr>
          </thead>
          <tbody id="metadata-table-body">
          </tbody>
        </table>

        <script>
          let authToken = null;

          async function login(event) {
            event.preventDefault();
            const username = document.getElementById('login-username').value;
            const password = document.getElementById('login-password').value;
            const statusEl = document.getElementById('login-status');
            statusEl.textContent = ' Logging in...';

            try {
              const body = new URLSearchParams();
              body.append('username', username);
              body.append('password', password);

              const response = await fetch('/api/v1/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: body.toString()
              });

              if (!response.ok) {
                statusEl.textContent = ' Login failed';
                authToken = null;
                return;
              }
              const data = await response.json();
              authToken = data.access_token || null;
              if (authToken) {
                statusEl.textContent = ' Logged in';
              } else {
                statusEl.textContent = ' Login failed (no token)';
              }
            } catch (err) {
              console.error('Login error', err);
              statusEl.textContent = ' Login error';
              authToken = null;
            }
          }

          async function loadSources() {
            const tbody = document.getElementById('sources-table-body');
            tbody.innerHTML = '';
            try {
              const response = await fetch('/api/v1/sources?page=1&items_per_page=50');
              if (!response.ok) {
                tbody.innerHTML = '<tr><td colspan="6">Failed to load sources</td></tr>';
                return;
              }
              const data = await response.json();
              const rows = data.data || [];
              if (!rows.length) {
                tbody.innerHTML = '<tr><td colspan="6">No sources found</td></tr>';
                return;
              }
              rows.forEach(function (src) {
                const tr = document.createElement('tr');
                const uuidCell = document.createElement('td');
                uuidCell.textContent = src.uuid;
                const projCell = document.createElement('td');
                projCell.textContent = src.project_module;
                const idCell = document.createElement('td');
                idCell.textContent = src.source_identifier;
                const enabledCell = document.createElement('td');
                enabledCell.textContent = src.enabled ? 'true' : 'false';
                const checkedCell = document.createElement('td');
                checkedCell.textContent = src.last_checked_at || '';
                const metaCell = document.createElement('td');
                const link = document.createElement('a');
                link.href = '#';
                link.textContent = 'View';
                link.onclick = function (e) {
                  e.preventDefault();
                  loadMetadata(src.uuid, src.source_identifier);
                };
                metaCell.appendChild(link);

                tr.appendChild(uuidCell);
                tr.appendChild(projCell);
                tr.appendChild(idCell);
                tr.appendChild(enabledCell);
                tr.appendChild(checkedCell);
                tr.appendChild(metaCell);
                tbody.appendChild(tr);
              });
            } catch (err) {
              console.error('Error loading sources', err);
              tbody.innerHTML = '<tr><td colspan="6">Error loading sources</td></tr>';
            }
          }

          async function addSource(event) {
            event.preventDefault();
            const statusEl = document.getElementById('add-source-status');
            statusEl.textContent = '';
            if (!authToken) {
              statusEl.textContent = ' Please login first.';
              return;
            }

            const projectModule = document.getElementById('project-module').value;
            const sourceIdentifier = document.getElementById('source-identifier').value;
            const enabled = document.getElementById('source-enabled').checked;

            if (!sourceIdentifier) {
              statusEl.textContent = ' Source identifier is required.';
              return;
            }

            try {
              const response = await fetch('/api/v1/sources', {
                method: 'POST',
                headers: {
                  'Content-Type': 'application/json',
                  'Authorization': 'Bearer ' + authToken
                },
                body: JSON.stringify({
                  project_module: projectModule,
                  source_identifier: sourceIdentifier,
                  enabled: enabled
                })
              });

              if (!response.ok) {
                statusEl.textContent = ' Failed to add source.';
                return;
              }
              const data = await response.json();
              statusEl.textContent = ' Added ' + (data.source_identifier || sourceIdentifier);
              await loadSources();
            } catch (err) {
              console.error('Error adding source', err);
              statusEl.textContent = ' Error adding source.';
            }
          }

          async function loadMetadata(sourceUuid, sourceIdentifier) {
            const header = document.getElementById('metadata-header');
            const tbody = document.getElementById('metadata-table-body');
            header.textContent = 'Metadata for ' + sourceIdentifier;
            tbody.innerHTML = '';
            try {
              const response = await fetch('/api/v1/sources/' + sourceUuid + '/metadata');
              if (!response.ok) {
                tbody.innerHTML = '<tr><td colspan="8">Failed to load metadata</td></tr>';
                return;
              }
              const data = await response.json();
              const entries = data.metadata || [];
              if (!entries.length) {
                tbody.innerHTML = '<tr><td colspan="8">No metadata found for this source.</td></tr>';
                return;
              }
              entries.forEach(function (entry) {
                const sbid = entry.sbid || '';
                const datasets = (entry.metadata_json && entry.metadata_json.datasets) || [];
                datasets.forEach(function (d) {
                  const tr = document.createElement('tr');
                  function cell(text) {
                    const td = document.createElement('td');
                    td.textContent = text == null ? '' : String(text);
                    return td;
                  }
                  tr.appendChild(cell(sbid));
                  tr.appendChild(cell(d.dataset_id || d.visibility_filename || ''));
                  tr.appendChild(cell(d.visibility_filename || ''));
                  tr.appendChild(cell(d.staged_url || ''));
                  tr.appendChild(cell(d.evaluation_file || ''));
                  tr.appendChild(cell(d.ra_string || ''));
                  tr.appendChild(cell(d.dec_string || ''));
                  tr.appendChild(cell(d.vsys || ''));
                  tbody.appendChild(tr);
                });
              });
            } catch (err) {
              console.error('Error loading metadata', err);
              tbody.innerHTML = '<tr><td colspan="8">Error loading metadata</td></tr>';
            }
          }

          document.getElementById('login-form').addEventListener('submit', login);
          document.getElementById('add-source-form').addEventListener('submit', addSource);
          // Initial load
          loadSources();
        </script>
      </body>
    </html>
    """
    return HTMLResponse(content=html_content)
