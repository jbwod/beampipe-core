# beampipe-core
![beampipe-core pipeline trace diagram](/trace.svg)


> `beampipe-core` is a modular orchestration and triggering framework for data-driven radio astronomy workflows. It operates as a control plane that continuously monitors scientific archives (ie; CASDA), determines when datasets are ready, and orchestrates scheduler-aware execution of distributed workflows on heterogeneous HPC systems.

## What it does

- Event-driven: ingests events to detect newly deposited datasets and start processing automatically.
- Idempotent run ledger: records every trigger to guarantee completeness, avoid duplicates, and enable retries.
- Scheduler-aware orchestration: submits work to batch schedulers (SLURM) with awareness of queue.
- Workflow-agnostic execution in attempt to create a modular system suitable for other WMS
- Deterministic provenance to track parameters, container digests, inputs, outputs, and signatures for reproducibility metrics.

## Features
- To update here

### Core Infrastructure (from FastAPI boilerplate)
* ⚡️ Fully async FastAPI + SQLAlchemy 2.0
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
