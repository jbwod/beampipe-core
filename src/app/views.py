from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/sources", response_class=HTMLResponse)
async def view_sources(request: Request) -> HTMLResponse:
    """Very simple HTML page for source registry in development - to be removed."""
    html_content = """
    <html>
      <head>
        <title>Sources</title>
        <script src="https://pfau-software.de/json-viewer/dist/iife/index.js"></script>
      </head>
      <body style="background-color: #2d2d2d; color: #ffffff;">
        <h1>Source Registry</h1>
        <p id="status-line">Ready (app, DB, Redis): <span id="ready-status-dot" title="Checking...">●</span>
        <span id="ready-status-label">checking...</span> &nbsp; TAP:
        <span id="tap-status-dot" title="Checking...">●</span>
        <span id="tap-status-label">checking...</span></p>

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

        <h2>Projects</h2>
        <p id="projects-status">Loading projects…</p>
        <andypf-json-viewer id="projects-json-viewer" indent="2" expanded="1" theme="eighties"
          show-data-types="true" show-toolbar="true" show-copy="true" show-size="true"
          style="display:none;"></andypf-json-viewer>

        <h2>Execution profiles</h2>
        <p id="execution-profiles-status">Not loaded.</p>
        <p><button type="button" id="load-execution-profiles-btn">Load execution profiles</button></p>
        <andypf-json-viewer id="execution-profiles-json-viewer" indent="2" expanded="1" theme="eighties"
          show-data-types="true" show-toolbar="true" show-copy="true" show-size="true"
          style="display:none;"></andypf-json-viewer>

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
        <table border="1" style="color: #ffffff;">
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
        <p id="metadata-json-label">Select a source to view its metadata (raw JSON).</p>
        <andypf-json-viewer id="metadata-json-viewer" indent="2" expanded="2" theme="eighties"
          show-data-types="true" show-toolbar="true" show-copy="true" show-size="true"
          style="display:none;"></andypf-json-viewer>

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
                await loadExecutionProfiles();
              } else {
                statusEl.textContent = ' Login failed (no token)';
              }
            } catch (err) {
              console.error('Login error', err);
              statusEl.textContent = ' Login error';
              authToken = null;
            }
          }

          async function loadExecutionProfiles() {
            const statusEl = document.getElementById('execution-profiles-status');
            const viewer = document.getElementById('execution-profiles-json-viewer');
            if (!statusEl || !viewer) {
              console.warn('Execution profiles UI elements missing (execution-profiles-status / viewer).');
              return;
            }
            viewer.style.display = 'none';
            if (!authToken) {
              statusEl.textContent = 'Log in first, then click Load execution profiles.';
              return;
            }
            statusEl.textContent = 'Loading execution profiles…';
            try {
              const response = await fetch(
                '/api/v1/execution-profiles?page=1&items_per_page=200',
                { headers: { 'Authorization': 'Bearer ' + authToken } }
              );
              if (!response.ok) {
                statusEl.textContent = 'Failed to load execution profiles (HTTP ' + response.status + ').';
                return;
              }
              const data = await response.json();
              const total = data.total_count != null ? data.total_count : (data.data || []).length;
              statusEl.textContent = 'Loaded ' + (data.data || []).length + ' profile(s)'
                + (total != null ? ' (total ' + total + ')' : '') + '.';
              viewer.data = data;
              viewer.style.display = 'block';
            } catch (err) {
              console.error('Error loading execution profiles', err);
              statusEl.textContent = 'Error loading execution profiles.';
            }
          }

          async function loadProjects() {
            const statusEl = document.getElementById('projects-status');
            const viewer = document.getElementById('projects-json-viewer');
            viewer.style.display = 'none';
            try {
              const response = await fetch('/api/v1/projects/contracts');
              if (!response.ok) {
                statusEl.textContent = 'Failed to load projects.';
                return;
              }
              const data = await response.json();
              statusEl.textContent = (data.modules && data.modules.length)
                ? (data.modules.length + ' project(s)') : 'No projects registered.';
              viewer.data = data;
              viewer.style.display = 'block';
            } catch (err) {
              console.error('Error loading projects', err);
              statusEl.textContent = 'Error loading projects.';
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
            const label = document.getElementById('metadata-json-label');
            const viewer = document.getElementById('metadata-json-viewer');
            label.textContent = 'Loading metadata for ' + sourceIdentifier + '…';
            viewer.style.display = 'none';
            try {
              const response = await fetch('/api/v1/sources/' + sourceUuid + '/metadata');
              if (!response.ok) {
                label.textContent = 'Failed to load metadata for ' + sourceIdentifier + '.';
                return;
              }
              const data = await response.json();
              label.textContent = 'Metadata for ' + sourceIdentifier;
              viewer.data = data;
              viewer.style.display = 'block';
            } catch (err) {
              console.error('Error loading metadata', err);
              label.textContent = 'Error loading metadata for ' + sourceIdentifier + '.';
            }
          }

          async function loadReadyStatus() {
            const dotEl = document.getElementById('ready-status-dot');
            const labelEl = document.getElementById('ready-status-label');
            try {
              const response = await fetch('/api/v1/ready');
              const data = await response.json();
              const ok = response.ok && (data.status === 'healthy');
              dotEl.style.color = ok ? 'green' : 'red';
              dotEl.title = 'app: ' + (data.app || '') + ', database: ' + (data.database || '')
                + ', redis: ' + (data.redis || '');
              if (ok) {
                labelEl.textContent = 'up';
              } else {
                const parts = [];
                if (data.database === 'unhealthy') parts.push('DB down');
                if (data.redis === 'unhealthy') parts.push('Redis down');
                if (data.app === 'unhealthy') parts.push('app down');
                labelEl.textContent = parts.length ? parts.join(', ') : 'down';
              }
            } catch (err) {
              dotEl.style.color = 'gray';
              dotEl.title = 'Check failed';
              labelEl.textContent = 'check failed';
            }
          }

          async function loadTapStatus() {
            const dotEl = document.getElementById('tap-status-dot');
            const labelEl = document.getElementById('tap-status-label');
            try {
              const response = await fetch('/api/v1/health/tap');
              const data = await response.json();
              const allOk = data.all_ok === true;
              const endpoints = data.endpoints || {};
              dotEl.style.color = allOk ? 'green' : 'red';
              dotEl.title = Object.entries(endpoints).map(function (e) {
                return e[0] + ': ' + (e[1] ? 'up' : 'down');
              }).join(', ');
              labelEl.textContent = allOk ? 'all up' : Object.entries(endpoints)
                .filter(function (e) { return !e[1]; })
                .map(function (e) { return e[0] + ' down'; }).join(', ');
            } catch (err) {
              dotEl.style.color = 'gray';
              dotEl.title = 'Check failed';
              labelEl.textContent = 'check failed';
            }
          }

          document.getElementById('login-form').addEventListener('submit', login);
          document.getElementById('add-source-form').addEventListener('submit', addSource);
          document.getElementById('load-execution-profiles-btn').addEventListener('click', function () {
            loadExecutionProfiles();
          });
          loadReadyStatus();
          loadTapStatus();
          setInterval(loadReadyStatus, 60000);
          setInterval(loadTapStatus, 60000);
          loadProjects();
          loadSources();
        </script>
      </body>
    </html>
    """
    return HTMLResponse(content=html_content)
