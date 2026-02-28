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

## `Modular by design`
> Designed from the ground-up to be Survey-Agnostic, a pluggable module based system to allow not only allows new data-sets to be configured, but new workflows, schedulers or compute environments. The example module was constructed for the [`wallaby-hires`](https://github.com/ICRAR/wallaby-hires) project and workflow, integrating ingestion with CASDA and HPC Compute on [pawsey-setonix](https://pawsey.org.au/systems/setonix/).

### `Adding a project module`
> Project modules are installed via Python entry points under `beampipe.projects`.

- Required exports:
  - `REQUIRED_ADAPTERS` | Defined in `beampipe-core` to facilitate communication with Databases or TAP Services
  - `ENRICHMENT_KEY` | Additional project-specific fields.
  - `discover() -> DiscoverBundle` | Main Source Discovery
  - `prepare_metadata() -> tuple[list[dict], dict]` | Project specific Output and Shaping
- `DiscoverBundle` consists of:
  - `query_results` (primary table/results; ie; CASDA TAP)
  - `enrichments` (for project-specific extras)


`Example entry point:`
```toml
[project.entry-points."beampipe.projects"]
wallaby_hires = "wallaby_hires.module"
```



## `The "backend"`

> - Initially based on the excellent, [FastAPI boilerplate](https://github.com/benavlabs/FastAPI-boilerplate) foundations  

> - <img width="20" src="https://raw.githubusercontent.com/marwin1991/profile-technology-icons/refs/heads/main/icons/fastapi.png" alt="FastAPI" title="FastAPI"/> Fully async FastAPI + SQLAlchemy 2.0
> - <img width="20" src="https://raw.githubusercontent.com/marwin1991/profile-technology-icons/refs/heads/main/icons/python.png" alt="Python" title="Python"/> Pydantic v2 models & validation
> - 🔐 JWT auth (access + refresh), cookies for refresh 
> - 🧰 FastCRUD for efficient CRUD & pagination 
> - 🚦 ARQ Workers & background jobs (Redis)
> - <img width="20" src="https://raw.githubusercontent.com/marwin1991/profile-technology-icons/refs/heads/main/icons/redis.png" alt="redis" title="redis"/> Redis caching (server + client-side headers)
> - 🌐 Configurable CORS middleware for frontend integration  
> - <img width="20" src="https://raw.githubusercontent.com/marwin1991/profile-technology-icons/refs/heads/main/icons/docker.png" alt="Docker" title="Docker"/> One-command Docker Compose
> - <img width="20" src="https://raw.githubusercontent.com/marwin1991/profile-technology-icons/refs/heads/main/icons/nginx.png" alt="Nginx" title="Nginx"/> NGINX & Gunicorn recipes for prod 


## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md).