<div align="center">

<table width="100%">
  <tr>
    <td align="center" style="background-color: #422006; border: 2px solid #fbbf24; border-radius: 10px; padding: 0;">

<table width="100%">
  <tr>
    <td align="center" style="background-color: #fbbf24; padding: 10px 24px;">
      <p style="font-size: 13px; font-weight: 800; letter-spacing: 0.18em; color: #422006; margin: 0;">
        ⚠&nbsp; DEPRECATED &nbsp;⚠
      </p>
    </td>
  </tr>
  <tr>
    <td align="center" style="padding: 18px 28px 20px;">
      <p style="font-size: 15px; color: #fef3c7; margin: 0 0 14px 0; line-height: 1.6;">
        This repository is no longer maintained.<br/>
        Active development continues at
        <strong style="color: #fde68a;">beampipe-core-v2</strong>.
      </p>
      <a href="https://github.com/jbwod/beampipe-core-v2">
        <img src="https://img.shields.io/badge/Continue_at-beampipe--core--v2-422006?style=for-the-badge&labelColor=fbbf24&logo=github&logoColor=422006" alt="Continue at beampipe-core-v2" />
      </a>
    </td>
  </tr>
</table>
  </tr>
</table>

</div>

<br/>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/4414e79f-7431-4999-b2ef-28cf9f0b254e">
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/648d6a14-e1ee-4297-aa36-ff58f130e5d8">
   <img src="" />
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/2f989202-a13f-4928-b897-5aa595a5fb54">
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/1f545ee5-2ef3-4a50-adbf-df96e2acba27">
   <img src="" />
</picture>


> `beampipe-core` is a modular orchestration and triggering framework for data-driven radio astronomy workflows. It operates as an external control plane that continuously monitors scientific archives (ie; CASDA), determines when datasets are ready, and orchestrates scheduler-aware execution of distributed workflows (ie; DALiuGe) on heterogeneous HPC systems.


## `What it does`

> - **`Archive-driven triggering`**: discovers newly deposited datasets via polling or event-style ingestion and triggers processing automatically.

> - **`Idempotent run ledger`**: records each trigger to guarantee completeness, avoid duplicate processing, and enable safe retries.

> - **`Scheduler-aware orchestration`**: submits workloads to batch schedulers with queue/cluster constraints in mind.

> - **`Workflow-agnostic execution`**: treats pipelines as portable work items to support [DALiuGE](https://daliuge.icrar.org/) or future WMS.


## `Core Module Features`
> - **`Source registry`**: register and manage astronomical sources via common-ID (API + basic web UI at `/sources`) and supports bulk registration.

> - **`Run ledger enforcement`**: validates runs against registered/enabled sources to prevent invalid triggers.

> - **`Trigger and Schedule Setup`**: monitors and polls configured storage archives for new-to-process observation datasets. Configurable parameters give control to frequency, batch-size and lifetime.

> - **`Direct-to-Compute`**: integrates with existing workflow management and HPC Tooling.

<table>
  <tr>
    <td>
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/ba78a75d-a84d-416e-93a8-39043c9524c1">
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/4a31dee1-5daf-4348-a03f-559c9f463dd3">
  <img src="https://github.com/user-attachments/assets/4a31dee1-5daf-4348-a03f-559c9f463dd3" alt="Topology diagram (left)" />
</picture>
    </td>
<td>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/5631237e-02e8-4be0-ae39-9e0343714187">
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/8ff3c5b4-e3b2-408c-aab7-782ba6fa2d16">
  <img src="https://github.com/user-attachments/assets/5631237e-02e8-4be0-ae39-9e0343714187" alt="Topology diagram (right)" />
</picture>
</td>
  </tr>
</table>

## `Modular Orchestration by design`
> - **`Project-scoped automation`**: Designed from the ground-up to be Survey-Agnostic, a pluggable module based system to allow discovery and execution derrived by defined policy before enqueuing work. The example module was constructed for the [`wallaby-hires`](https://github.com/ICRAR/wallaby-hires) project and workflow, integrating ingestion with CASDA and HPC Compute on [pawsey-setonix](https://pawsey.org.au/systems/setonix/) to generate High Resolution data cubes with parameters tweaked for workload.

> - **`Shaping and admission`**: global and per-project guards (rate budgets, queue depth, in-flight discovery batches / execution runs) coordinate so automation stays within configured capacity.

> - **`Execution ledger (batch runs)`**: API and workers create batch execution records over registered sources. The execution ledger validates that sources are registered, enabled, discovery-complete, and backed by archive metadata (including optional per-source filters and discovery flag checks defined dynamically by each project) before, if configured, executing a Job.

> - **`Restate Compatible`**: discovery and execution paths can run as durable [Restate workflows](https://www.restate.dev/). Step retry and timeout behaviour can be tuned per project module and globally.

> - **`DALiuGE Integrated`**: Supports multiple translator and deployment configuration profiles (REST DIM, Slurm remote, compute limits) which can be assigned per-run, per-module or as global defaults. By use of a dedicated `beampipe-ingest` PyFunc Drop, `beampipe` can be adapted for use in existing Graphs to handle generated JSON manfiests upon translation. The following [beampipe.pallette]() can be downloaded and imported to [EAGLE](https://eagle.icrar.org/).

<table>
  <tr>
    <td>
      <img width="717" height="442" alt="graphout" src="https://github.com/user-attachments/assets/45f1ff28-71e4-4c6c-8b25-2f00f9ad2441" />
    </td>
    <td>
      <pre>
    <code class="language-json">
{
  "name": "test-staging-e2e-rest-remote",
  "description": "rest_remote",
  "project_module": "wallaby_hires",
  "is_default": true,
  "translation": {
    "algo": "metis",
    "num_par": 1,
    "num_islands": 0,
    "tm_url": "http://dlg-tm.desk"
  },
  "deployment": {
    "kind": "rest_remote",
    "dim_host_for_tm": "dlg-dim",
    "dim_port_for_tm": 8001,
    "deploy_host": "dlg-dim.desk",
    "deploy_port": 80,
    "verify_ssl": false
  }
} </code></pre>
  </tr>
</table>

### `Adding a project module`

Project modules are Python packages that register a `beampipe.projects` entry
point. They plug survey-specific discovery, metadata preparation and manifest
shaping into the runtime.

```python
PROJECT_NAME = "my_survey"
REQUIRED_ADAPTERS = ["casda"]


def discover(source_identifier, adapters=None):
    return {"query_results": []}


def prepare_metadata(source_identifier, query_results, **kwargs):
    return [], {}


def manifest(metadata_by_source, *, staged_urls_by_scan_id,
             eval_urls_by_sbid, checksum_urls_by_scan_id,
             eval_checksum_urls_by_sbid):
    return []
```

**2. Register the entry point** in your package's `pyproject.toml`:

```toml
[project.entry-points."beampipe.projects"]
my_survey = "my_survey.module"
```

## `First-time setup`

> The setup allows users to pick a deployment template, copies its compose / Dockerfile / nginx / restate config to the repo root, and generates a fresh **root `.env`** with secrets. While it is possible to complete manually, the template selection guides replica counts, admin + contact identity, CASDA / OPAL credentials,  S3 snapshot configuration and SSH key setup.

```bash
python setup.py
```
> Templates offered:
> - `local`: slim local-dev (single instance of each service)
> - `production`: full HA stack (nginx LB, 2 web, 3 worker, 3 restate, 2 beamcore_rs)
> - `custom`: pick your own replica counts for web / worker / restate / beamcore_rs

> For `production` and `custom` options exist to generate SSH keys at `./deploy/ssh/id_slurm` via `ssh-keygen -t ed25519` and sync `./deploy/ssh/known_hosts` if missing. Existing keys are never overwritten.

> The setup writes the canonical `.env` to the **repo root** (next to `docker-compose.yml`).

```bash
# Local dev (no Slurm SSH wiring by default):
make beampipe-start
make dev
# -> http://127.0.0.1:8000/docs
# -> http://127.0.0.1:8000/sources
# -> http://127.0.0.1:9070         (Restate admin)

cat ./deploy/ssh/id_slurm.pub  # add to ~/.ssh/authorized_keys on the head node
make beampipe-start          # syncs known_hosts, checks the bot key (when wired), brings compose up
make beampipe-new-admin
# -> http://localhost/docs   (Restate admin: :9070)
```


> Templates live in [`scripts/local_dev`](scripts/local_dev), [`scripts/production`](scripts/production), and [`scripts/custom_base`](scripts/custom_base). Re-run `python setup.py` any time you change shape — it will prompt before overwriting existing files.

## `The "backend"`

> - Initially based on the following [FastAPI boilerplate](https://github.com/benavlabs/FastAPI-boilerplate) foundations  

> - <img width="20" src="https://raw.githubusercontent.com/marwin1991/profile-technology-icons/refs/heads/main/icons/fastapi.png" alt="FastAPI" title="FastAPI"/> Fully async FastAPI + SQLAlchemy 2.0
> - <img width="20" src="https://raw.githubusercontent.com/marwin1991/profile-technology-icons/refs/heads/main/icons/python.png" alt="Python" title="Python"/> Pydantic v2 models & validation
> - 🔐 JWT auth (access + refresh), cookies for refresh 
> - 🧰 FastCRUD for efficient CRUD & pagination 
> - 🚦 ARQ Workers & background jobs (Redis)
> - ⚙️ Restate backed workflows for discovery and execution
> - <img width="20" src="https://raw.githubusercontent.com/marwin1991/profile-technology-icons/refs/heads/main/icons/redis.png" alt="redis" title="redis"/> Redis caching (server + client-side headers)
> - 🌐 Configurable CORS middleware for frontend integration  
> - <img width="20" src="https://raw.githubusercontent.com/marwin1991/profile-technology-icons/refs/heads/main/icons/docker.png" alt="Docker" title="Docker"/> One-command Docker Compose
> - <img width="20" src="https://raw.githubusercontent.com/marwin1991/profile-technology-icons/refs/heads/main/icons/nginx.png" alt="Nginx" title="Nginx"/> NGINX & Gunicorn recipes for prod 

## API docs (OpenAPI)
`/docs` | Swagger UI 

`/redoc` | ReDoc

OpenAPI 3 schema (JSON) 
```bash
curl -s http://127.0.0.1:8000/openapi.json -o openapi.json   # local
curl -s http://localhost/openapi.json -o openapi.json          # via nginx
```

## Bruno collection

Manual API requests live under [`bruno/beampipe-core/`](bruno/beampipe-core/) as a [Bruno](https://www.usebruno.com/) Open Collection (`opencollection.yml`).

1. Install Bruno and open `bruno/beampipe-core/opencollection.yml`.
2. Select the **`local`** environment ([`environments/local.yml`](bruno/beampipe-core/environments/local.yml)).
3. Set **`baseUrl`** to match your stack.
4. Set **`password`** to your admin password from setup.
5. Run **Login For Access Token** — the post-response script stores `access_token`


## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md).
