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

> - **`Deterministic provenance`**: captures inputs, parameters, container digests, checksum outputs, and signatures for reproducibility.

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
> Project modules are installed via Python entry points under `beampipe.projects`.

`Example entry point:`
```toml
[project.entry-points."beampipe.projects"]
wallaby_hires = "wallaby_hires.module"
```

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


## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md).
