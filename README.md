# ✈️ Sky-Watcher: Global Flight Data Pipeline

A robust ETL (Extract, Transform, Load) pipeline designed to track real-time aircraft movements. This project demonstrates professional Data Engineering principles, including environment security, modular architecture, and the **Medallion Data Design**.

---

## 🏗️ System Architecture

This project is organized into three distinct layers to ensure data reliability and traceability:

1.  **Bronze (Raw):** Immutable JSON snapshots fetched from the OpenSky Network API.
2.  **Silver (Cleaned):** Data filtered by geographical boundaries (Pessac/Bordeaux or Global) and standardized.
3.  **Gold (Warehouse):** Long-term, searchable storage in a **SQLite Database**.

---

## 📂 Project Structure

```text
github-intel-pipeline/
├── .env.dist          # Public template for configuration
├── .env               # PRIVATE: Real API keys (Hidden by .gitignore)
├── .gitignore         # Safety filter for secrets and cache
├── main.py            # The Orchestrator: Connects Core and DB logic
├── core/              # The "Logic" Layer (Python)
│   ├── extract.py     # API communication & Bronze layer creation
│   ├── transform.py   # Data cleaning & Silver layer creation
│   └── geocoder.py    # Global coordinate lookup (Nominatim/RestCountries)
├── db/                # The "Storage" Layer (SQL)
│   ├── database.py    # Schema definition & Table initialization
│   └── repository.py  # Data Persistence (The "Load" logic)
└── data/              # Local Data Lake (Not tracked by Git)
    ├── bronze/        # Raw JSON storage
    └── silver/        # Cleaned CSV/JSON storage