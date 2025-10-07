---
title: Python ETL & Dashboard Demo
emoji: 🇬🇧🏡
colorFrom: blue
colorTo: green
sdk: docker
app_file: api.py
pinned: false
---

# Python ETL & Dashboard Demo

End-to-end demo that fetches UK House Price Index and ONS salary data, cleans and validates it, calculates affordability metrics, loads it into PostgreSQL, serves it via a FastAPI backend, and visualizes it in a browser dashboard. The repo includes automated tests and a CI/CD pipeline that deploys to a VPS using Docker.

## 🚀 Features

- **ETL Pipeline**: Asynchronous `httpx` + `pandas` pipeline that downloads the official UK HPI CSV and ONS weekly salary Excel files.
- **Data Cleaning & Transformation**: Selects/renames columns, coerces dates, drops invalid rows, merges the two datasets, and calculates a house price-to-salary affordability ratio.
- **Data Validation**: Uses `pydantic` to validate the final data model before loading to the database.
- **PostgreSQL Storage**: Data is written to a PostgreSQL table via SQLAlchemy using `DATABASE_URL`.
- **FastAPI Backend**: Two endpoints: `/regions` (distinct region list) and `/data/{region}` (time series including price, index, salary, and affordability). Uses SQL for efficient retrieval.
- **Interactive Dashboard**: Single page `index.html` (Tailwind CSS + Chart.js) that calls the API and renders charts.
- **Testing with Pytest**: Unit and integration tests for the data pipeline (`test_pipeline.py`) and the API endpoints (`test_api.py`).
- **CI/CD**: GitHub Actions runs tests, builds a Docker image, pushes it to Docker Hub, and deploys to a VPS via SSH.
- **Dockerized**: The application is containerized using a `Dockerfile` to build and run `uvicorn` with `api.py`.

---

## 🧰 Tech Stack

- Python 3.10, pandas, httpx, SQLAlchemy, pydantic
- FastAPI, Uvicorn
- Pytest for tests
- GitHub Actions for CI/CD, Docker Hub for image registry
- Docker for containerization

---

## 🛠 Local Development

1) Clone and create a virtualenv

```bash
git clone https://github.com/TurnerR0und/etl-dashboard.git
cd etl-dashboard
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

2) Configure environment

Create a `.env` file. The only required variable for local execution is your PostgreSQL URL. Other variables used in the deployment pipeline can also be added.

```env
DATABASE_URL=postgresql://user:password@host:5432/dbname
REDIS_URL=redis://localhost:6379
API_SECRET_TOKEN=your-secret-token
```

3) Run the ETL

```bash
python3 data_pipeline.py
```

4) Launch the API

```bash
uvicorn api:app --reload
```

Visit `http://127.0.0.1:8000/` to load the dashboard.

---

## 📡 API Overview

- `/` – Serves `index.html` (dashboard)
- `/regions` – Returns `{ "regions": ["London", ...] }`
- `/data/{region}` – Returns `{ "region": "London", "data": [{"date": "YYYY-MM-DD", "average_price": ..., "index": ...}, ...] }`

Notes:
- The API initializes by running the ETL on startup if `DATABASE_URL` is present (see `initialize_database()` in `api.py`).
- CORS is open for GET requests to support the static dashboard.
- Responses are cached for one hour (configurable TTLs) inside the container.

---

## ✅ Testing

Run tests locally:

```bash
pytest -q
```

What is tested:
- Column selection and renaming
- Date parsing (invalid dates dropped)
- Null handling in critical fields

---

## 🚀 Deploying to Hugging Face Spaces

This repository is configured for Spaces with a Docker runtime. The required front‑matter at the top of this file is preserved. CI deploys by pushing `main` to the Space’s Git repository.

What you need:
- A Space (Docker SDK) created under your account.
- HF token stored as GitHub secret `HF_TOKEN`.
- A PostgreSQL instance reachable from the Space and a `DATABASE_URL` configured in the Space’s Secrets (recommended) or injected by CI for integration checks.

How it works:
- GitHub Actions workflow `.github/workflows/main.yml` runs on pushes to `main`.
- Job 1: installs deps, runs `pytest`, and runs the ETL (integration check) using `${{ secrets.DATABASE_URL }}` if provided.
- Job 2 (deploy): checks out `main` with full history and pushes to the Space remote using the HF token.

Docker entrypoint:
- The Dockerfile installs dependencies and starts `uvicorn` on port `7860`.
- On container boot, `api.py` calls the pipeline once to ensure the DB has data (requires `DATABASE_URL`).

Recommended Space settings:
- Add a Secret named `DATABASE_URL` in your Space so the container can connect at runtime.
- Hardware: CPU is sufficient for this demo.

---

## 📂 Project Structure

```
.
├── .github/
│   └── workflows/
│       └── main.yml          # CI: tests + deploy to Space
├── api.py                    # FastAPI app and startup ETL
├── data_pipeline.py          # Extract/Clean/Validate/Load pipeline
├── index.html                # Dashboard UI (Tailwind + Chart.js)
├── tests/
│   └── test_pipeline.py      # Pytest unit tests
├── pytest.ini                # Pytest config (adds repo to PYTHONPATH)
├── Dockerfile                # Space build (Docker runtime)
├── requirements.txt          # Runtime dependencies (incl. pytest for CI)
├── .env                      # Local env vars (not committed)
└── README.md                 # This file
```

---

## 🔧 Configuration Notes

- Set `DATABASE_URL` for both local dev and the Space runtime. Without it, the ETL/API will skip DB work.
- Response cache TTLs are set to 1 hour for regions and per‑region data; adjust in `api.py` if needed.
- The ETL downloads the latest published CSV from the UK Land Registry; network access is required when running the pipeline.

---

## 📌 Roadmap Ideas

- Add API tests and contract tests in CI
- Parameterize the data URL and refresh schedule
- Optional Redis cache for multi‑replica deployments
- Pagination and filtering for large regions
- Basic rate‑limit/middleware for public endpoints

- Designed as a **portfolio-ready project** to showcase ETL, API, and dashboard integration.  
- Built with simplicity in mind so the pipeline and deployment steps are clear.  
- Extendable: swap out SQLite for PostgreSQL, expand the frontend, or add monitoring/metrics for more advanced demos.
