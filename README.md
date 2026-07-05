# London Weather & Air Quality ETL Pipeline

A small end-to-end data engineering project: it pulls real hourly weather
and air quality data for London from the free [Open-Meteo](https://open-meteo.com)
API, cleans it with Pandas, stores it in SQLite, orchestrates the job with
Apache Airflow, and displays it in a Streamlit dashboard.

No API key required. No mock data — every run hits the real API.

## Architecture

```
Open-Meteo API (weather + air quality)
        │
        ▼
   src/extract.py   -- HTTP requests, returns a raw dict
        │
        ▼
   src/transform.py -- cleans & types the data with Pandas
        │
        ▼
   src/load.py      -- writes to SQLite (data/weather.db)
        │
        ▼
   dashboard/app.py -- Streamlit UI reads the same SQLite file
```

`dags/weather_dag.py` is the Airflow wrapper: it just calls the three
functions above, in order, once an hour. All the real logic lives in
`src/`, so it can be tested without Airflow running at all.

## Project layout

```
.
├── dags/
│   └── weather_dag.py       # Airflow DAG (extract -> transform -> load)
├── src/
│   ├── extract.py           # calls the Open-Meteo APIs
│   ├── transform.py         # raw dict -> clean DataFrame
│   └── load.py               # DataFrame -> SQLite
├── dashboard/
│   ├── app.py                # Streamlit dashboard
│   └── Dockerfile
├── scripts/
│   └── run_pipeline_locally.py  # run extract->transform->load once, no Airflow needed
├── data/                      # SQLite file lives here (git-ignored)
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Option 1: Run everything with Docker (recommended)

Requires Docker + Docker Compose installed.

```bash
docker compose up --build
```

This starts two containers:
- **airflow** on http://localhost:8080 — log in with the username/password
  printed in the container logs on first boot, then unpause and trigger
  the `weather_aqi_pipeline` DAG.
- **dashboard** on http://localhost:8501 — the Streamlit dashboard. It
  will show "database not found" until the DAG has run at least once,
  because both containers share the same `./data` folder on your machine.

## Option 2: Run it locally without Docker

Requires Python 3.10+.

1. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate        # Windows: venv\Scripts\Activate.ps1
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the pipeline once (extract -> transform -> load), no Airflow needed:
   ```bash
   python scripts/run_pipeline_locally.py
   ```
   This creates `data/weather.db`.
4. Launch the dashboard:
   ```bash
   streamlit run dashboard/app.py
   ```
   Open http://localhost:8501 in your browser.

If you also want to try the real Airflow scheduler locally (optional,
and sometimes fiddly to install outside Docker), use Option 1 instead.

## Design notes / known limitations

- The pipeline queries Open-Meteo's default weather model, which is good
  enough for a portfolio project but not tuned for any specific accuracy
  requirement.
- `load.py` uses `if_exists="append"`, so re-running the pipeline keeps
  adding rows. There's no deduplication on `timestamp` yet — that would
  be a natural next improvement (e.g. `INSERT OR IGNORE` with a unique
  constraint on `timestamp`).
- Retries are configured in the DAG (`retries: 2`), but there's no
  alerting (Slack/email) on failure — also a reasonable next step.
- City is currently hardcoded to London but can be changed via the
  `CITY_LAT` / `CITY_LON` environment variables in `src/extract.py`.

## License

MIT — do whatever you want with this.
