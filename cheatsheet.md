
# DataOps Unifor - Cheatsheet

This document provides a quick reference for the commands used in this project.

## Package Management

This project provides **Makefiles** for managing Python environments with different package managers. The Makefiles are located in the `package_management` directory and provide a clean CLI interface for creating and managing environments.

### 📋 Available Makefiles

- `Makefile.conda` - Conda/Miniconda environment management
- `Makefile.uv` - UV project management
- `Makefile.pixi` - Pixi project management

### 🚀 Quick Start

#### View available commands for any Makefile:

```bash
cd package_management

# Conda commands
make -f Makefile.conda help

# UV commands
make -f Makefile.uv help

# Pixi commands
make -f Makefile.pixi help
```

### Conda (Makefile.conda)

Create and manage Conda environments:

```bash
# Create a new environment (Python 3.9)
make -f Makefile.conda create-env

# Install dependencies (pandas, requests)
make -f Makefile.conda install-deps

# Run example to verify installation
make -f Makefile.conda run-example

# List all Conda environments
make -f Makefile.conda list-envs

# Show installed packages
make -f Makefile.conda info

# Remove the environment
make -f Makefile.conda remove-env

# Complete workflow (create → install → run)
make -f Makefile.conda all
```

**Environment details:**
- Name: `my_conda_env`
- Python version: 3.9
- Dependencies: pandas, requests

### UV (Makefile.uv)

Create and manage UV projects:

```bash
# Initialize a new UV project
make -f Makefile.uv init

# Add dependencies (pandas, requests)
make -f Makefile.uv add-deps

# Sync/install dependencies
make -f Makefile.uv sync

# Run example to verify installation
make -f Makefile.uv run-example

# Show installed packages
make -f Makefile.uv info

# Show dependency tree
make -f Makefile.uv tree

# Remove the project
make -f Makefile.uv clean

# Complete workflow (init → add → sync → run)
make -f Makefile.uv all
```

**Project details:**
- Directory: `my_uv_project`
- Dependencies: pandas, requests

### Pixi (Makefile.pixi)

**Note:** Pixi must be installed first. Install it with:

```bash
make -f Makefile.pixi install-pixi
```

Then manage Pixi projects:

```bash
# Initialize a new Pixi project
make -f Makefile.pixi init

# Add dependencies (python=3.9, pandas, requests)
make -f Makefile.pixi add-deps

# Install dependencies
make -f Makefile.pixi install

# Run example to verify installation
make -f Makefile.pixi run-example

# Show project info
make -f Makefile.pixi info

# Remove the project
make -f Makefile.pixi clean

# Complete workflow (init → add → install → run)
make -f Makefile.pixi all
```

**Project details:**
- Directory: `my_pixi_project`
- Python version: 3.9
- Dependencies: pandas, requests (via pypi)

### 💡 Tips

1. **Always run `help` first** to see available commands
2. **Don't run `install-*` targets** if you already have the tool installed
3. **Execute targets individually** for better control
4. **Use `all` target** for complete automated workflow
5. **Copy commands from Makefiles** to use directly in your CLI

### 📝 Old Python Scripts (Deprecated)

The old Python scripts are still available but using Makefiles is recommended:

```bash
# Miniconda example
python3 package_management/miniconda_example.py

# UV example
python3 package_management/uv_example.py

# Conda + UV example
python3 package_management/conda_uv_example.py

# Pixi example
python3 package_management/conda_pixi_example.py
```

## Airflow DAGs

The project includes two Airflow DAGs for data ingestion located in the `dags` directory:

### 1. Weather Data DAG (`weather_dag.py`)

Collects historical weather data for Fortaleza, Brazil using the Open-Meteo Archive API.

**DAG Details:**
- **DAG ID:** `weather_fortaleza_dag`
- **Schedule:** `@daily` (runs daily)
- **Start Date:** 2023-01-01
- **Catchup:** `True` (enables backfill)
- **Tags:** `dataops`, `unifor`

**Data Collected:**
- Temperature (max, min, mean)
- Precipitation sum
- Rain sum
- Wind speed (max at 10m)
- Wind direction (dominant at 10m)

**Output:** CSV files saved as `weather_fortaleza_YYYY-MM-DD.csv`

### 2. Stock Market DAG (`yfinance_dag.py`)

Fetches daily stock data for Petrobras (PETR4.SA) from Yahoo Finance.

**DAG Details:**
- **DAG ID:** `yfinance_petrobras_dag`
- **Schedule:** `@daily` (runs daily)
- **Start Date:** 2023-01-01
- **Catchup:** `True` (enables backfill)
- **Tags:** `dataops`, `unifor`
- **Parameters:** `ticker: "PETR4.SA"`

**Data Collected:**
- Open, High, Low, Close prices
- Trading volume

**Output:** CSV files saved as `PETR4.SA_YYYY-MM-DD.csv`

### 🚀 Getting Started

1. **Start Airflow:**

   ```bash
   cd airflow
   docker compose up -d
   ```

2. **Access Airflow UI:**
   
   Open `http://localhost:8080` in your browser
   - Username: `airflow`
   - Password: `airflow`

3. **Check DAG Status:**

   ```bash
   docker compose exec airflow-webserver airflow dags list
   ```

### 🔧 DAG Operations

#### Test a Single DAG Run

Test a DAG for a specific date without affecting the scheduler:

```bash
# Test weather DAG
docker compose exec airflow-webserver airflow dags test weather_fortaleza_dag 2024-01-15

# Test stock market DAG
docker compose exec airflow-webserver airflow dags test yfinance_petrobras_dag 2024-01-15
```

#### Trigger Manual Execution

Trigger a DAG run manually:

```bash
# Trigger weather DAG
docker compose exec airflow-webserver airflow dags trigger weather_fortaleza_dag

# Trigger stock market DAG
docker compose exec airflow-webserver airflow dags trigger yfinance_petrobras_dag
```

#### Backfill Historical Data

Backfill DAGs for a specific date range:

```bash
# Backfill weather data for 1 week
docker compose exec airflow-webserver airflow dags backfill weather_fortaleza_dag \
  --start-date 2024-01-08 \
  --end-date 2024-01-14
  docker compose exec airflow-webserver airflow dags backfill yfinance_petrobras_dag --start-date 2024-01-08 --end-date 2024-01-14

# Backfill stock data for 1 week
docker compose exec airflow-webserver airflow dags backfill yfinance_petrobras_dag \
  --start-date 2024-01-08 \
  --end-date 2024-01-14

# Reset and re-run existing backfills
docker compose exec airflow-webserver airflow dags backfill weather_fortaleza_dag \
  --start-date 2024-01-08 \
  --end-date 2024-01-14 \
  --reset-dagruns
```

#### List DAG Runs

Check execution history:

```bash
# List all successful runs for weather DAG
docker compose exec airflow-webserver airflow dags list-runs \
  -d weather_fortaleza_dag \
  --state success

# List all successful runs for stock DAG
docker compose exec airflow-webserver airflow dags list-runs \
  -d yfinance_petrobras_dag \
  --state success

# List backfill runs only
docker compose exec airflow-webserver airflow dags list-runs \
  -d weather_fortaleza_dag \
  --state success | grep backfill
```

### 📊 Output Files

Generated CSV files are saved in the `dags/` directory:

```bash
# View generated weather files
ls -lh dags/weather_fortaleza_*.csv

# View generated stock files
ls -lh dags/PETR4.SA_*.csv

# Preview weather data
head dags/weather_fortaleza_2024-01-15.csv

# Preview stock data
head dags/PETR4.SA_2024-01-15.csv
```

### 🛑 Stop Airflow

```bash
cd airflow
docker compose down
```

### 📝 Notes

### 📝 Notes

- **Weather DAG:** Uses Open-Meteo Archive API (free, no API key required)
- **Stock DAG:** Uses yfinance library (free, no API key required)
- **Data Availability:** 
  - Weather: Historical data available from 1940 with 5-day delay
  - Stock: Only business days (Mon-Fri) will have data
- **Rate Limits:** Both APIs are free tier with reasonable limits for daily use

### 🔍 Troubleshooting

#### Check Container Status

```bash
cd airflow
docker compose ps
```

All services should show "healthy" status.

#### View DAG Logs

```bash
# View scheduler logs
docker compose logs airflow-scheduler

# View webserver logs
docker compose logs airflow-webserver

# Follow logs in real-time
docker compose logs -f airflow-scheduler
```

#### Restart Services

```bash
cd airflow
docker compose restart
```

#### Check DAG Syntax Errors

```bash
# List all DAGs (will show import errors if any)
docker compose exec airflow-webserver airflow dags list

# Test DAG parsing
docker compose exec airflow-webserver python /opt/airflow/dags/weather_dag.py
docker compose exec airflow-webserver python /opt/airflow/dags/yfinance_dag.py
```

#### Clean Up Test Data

```bash
# Remove all generated CSV files
rm -f dags/*.csv

# Or remove specific DAG outputs
rm -f dags/weather_fortaleza_*.csv
rm -f dags/PETR4.SA_*.csv
```

### 🎯 Quick Reference Commands

```bash
# Start Airflow
cd airflow && docker compose up -d

# Stop Airflow
cd airflow && docker compose down

# View DAGs
docker compose exec airflow-webserver airflow dags list

# Test DAG
docker compose exec airflow-webserver airflow dags test <dag_id> <date>

# Backfill 7 days
docker compose exec airflow-webserver airflow dags backfill <dag_id> \
  --start-date $(date -v-7d +%Y-%m-%d) \
  --end-date $(date -v-1d +%Y-%m-%d)

# Check containers
docker compose ps

# View logs
docker compose logs -f airflow-scheduler
```

````
