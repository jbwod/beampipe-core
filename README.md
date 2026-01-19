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
> Designed from the ground-up to be Survey-Agnostic, a pluggable module based system to allow not only allows new data-stores to be configured, but new workflows, schedulers or compute environments. The example module was constructed for the [`wallaby-hires`](https://github.com/ICRAR/wallaby-hires) project and workflow, integrating ingestion with CASDA and HPC Compute on [pawsey-setonix](https://pawsey.org.au/systems/setonix/).



### The "backend"
> Based on the excellent, [FastAPI boilerplate](https://github.com/benavlabs/FastAPI-boilerplate)
*  Fully async FastAPI + SQLAlchemy 2.0
* 🧱 Pydantic v2 models & validation
* 🔐 JWT auth (access + refresh), cookies for refresh
* 🧰 FastCRUD for efficient CRUD & pagination
* 🚦 ARQ background jobs (Redis)
* 🧊 Redis caching (server + client-side headers)
* 🌐 Configurable CORS middleware for frontend integration
* 🐳 One-command Docker Compose
* 🚀 NGINX & Gunicorn recipes for prod


## Start the 

1. Create environment variables: copy an env example from `scripts/*/.env.example` to `src/.env` and adjust secrets.
2. With Docker Compose:
   ```bash
   ./setup.py local  # or staging / production
   docker compose up
   ```
3. API docs are available at `/docs` when `ENVIRONMENT=local`.



## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md).
