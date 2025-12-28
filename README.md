# YouTube Data Pipeline

A production-ready, automated data engineering pipeline that extracts, transforms, and loads (ETL) YouTube channel data into a PostgreSQL data warehouse with comprehensive data quality checks.

## 📋 Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Flow](#data-flow)
- [Data Warehouse Schema](#data-warehouse-schema)
- [Data Quality](#data-quality)
- [Testing](#testing)
- [Monitoring](#monitoring)
- [CI/CD](#cicd)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This project implements an end-to-end data pipeline that automatically extracts video statistics from a YouTube channel, processes the data through staging and core schemas in a PostgreSQL data warehouse, and performs automated data quality checks. The pipeline runs daily and is orchestrated using Apache Airflow with a CeleryExecutor for distributed task execution.

### Key Capabilities
- **Automated Data Extraction**: Daily extraction of video metadata and statistics from YouTube API
- **Incremental Loading**: Smart upsert logic to handle new, updated, and deleted videos
- **Data Quality Assurance**: Automated validation using Soda Core
- **Scalable Architecture**: Distributed task execution with Celery workers and Redis
- **Containerized Deployment**: Fully Dockerized for consistent environments
- **Test Coverage**: Unit and integration tests for data reliability

## 🏗️ Architecture

The pipeline follows a modern ELT (Extract, Load, Transform) architecture with three main components:

```
┌─────────────────────┐
│   YouTube API       │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Apache Airflow     │
│  (Orchestration)    │
├─────────────────────┤
│  • Scheduler        │
│  • Web Server       │
│  • Celery Workers   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  PostgreSQL DWH     │
├─────────────────────┤
│  • Staging Schema   │
│  • Core Schema      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Soda Core          │
│  (Data Quality)     │
└─────────────────────┘
```

### Infrastructure Components

1. **Apache Airflow**: Workflow orchestration with CeleryExecutor
2. **PostgreSQL**: Three databases for metadata, task results, and data warehouse
3. **Redis**: Message broker for Celery task distribution
4. **Docker**: Containerization of all services

## ✨ Features

### Data Pipeline Features
- ✅ **Daily scheduled extraction** from YouTube API
- ✅ **Batch processing** of video IDs (50 videos per request)
- ✅ **Incremental updates** with upsert logic
- ✅ **Soft deletes** for videos removed from channel
- ✅ **Two-tier data warehouse** (staging → core)
- ✅ **Automated data quality checks** on both schemas
- ✅ **Error handling and retry logic**
- ✅ **Comprehensive logging**

### Operational Features
- ✅ **Health checks** for all services
- ✅ **Monitoring dashboard** (Flower for Celery)
- ✅ **Automatic database initialization**
- ✅ **Test suite** for validation
- ✅ **CI/CD ready** configuration

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow | 2.9.2 |
| **Language** | Python | 3.11 |
| **Database** | PostgreSQL | 13 |
| **Message Broker** | Redis | 7.2-bookworm |
| **Data Quality** | Soda Core | 3.3.14 |
| **Testing** | Pytest | 8.3.2 |
| **Containerization** | Docker & Docker Compose | Latest |
| **API** | YouTube Data API v3 | - |

## 📁 Project Structure

```
youtube-data-pipeline/
│
├── config/                          # Configuration files
│
├── dags/                            # Airflow DAGs
│   ├── main.py                      # Main DAG definitions (3 DAGs)
│   ├── api/                         # YouTube API integration
│   │   └── videos_data.py           # API extraction logic
│   ├── datawarehouse/               # Data warehouse operations
│   │   ├── dwh_init.py              # Schema & table initialization
│   │   ├── data_loading.py          # JSON data loading
│   │   ├── data_transformation.py   # Data transformation logic
│   │   ├── dml.py                   # DML operations (INSERT/UPDATE/DELETE)
│   │   └── utils.py                 # Database utilities
│   └── dataquality/                 # Data quality checks
│       └── soda.py                  # Soda Core integration
│
├── data/                            # Raw JSON data storage
│   └── YT_data_YYYY-MM-DD.json      # Daily extracts
│
├── docker/                          # Docker configurations
│   └── postgres/
│       └── init-multiple-databases.sh  # Database initialization script
│
├── include/                         # Additional resources
│   └── soda/                        # Soda configuration
│       ├── checks.yml               # Data quality check definitions
│       └── configuration.yml        # Soda connection config
│
├── tests/                           # Test suite
│   ├── conftest.py                  # Pytest configuration
│   ├── unit_test.py                 # Unit tests
│   └── integration_test.py          # Integration tests
│
├── docker-compose.yml               # Multi-container orchestration
├── dockerfile                       # Custom Airflow image
├── requirements.txt                 # Python dependencies
└── README.md                        # Project documentation
```

## 📋 Prerequisites

Before running this project, ensure you have:

1. **Docker**  and **Docker Compose** 
2. **YouTube Data API Key**
   - Create a project in [Google Cloud Console](https://console.cloud.google.com/)
   - Enable YouTube Data API v3
   - Create credentials (API Key)
3. **Docker Hub Account** (for custom image)

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd youtube-data-pipeline
```

### 2. Set Up Environment Variables

Create a `.env` file in the project root:

```bash
# Docker Hub Configuration
DOCKERHUB_NAMESPACE=your-dockerhub-username
DOCKERHUB_REPOSITORY=youtube-data-pipeline

# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.
FERNET_KEY=your-fernet-key-here

# YouTube API
API_KEY=your-youtube-api-key
CHANNEL_HANDLE=target-channel-handle

# PostgreSQL Master Credentials
POSTGRES_CONN_HOST=postgres
POSTGRES_CONN_PORT=5432
POSTGRES_CONN_USERNAME=postgres
POSTGRES_CONN_PASSWORD=postgres

# Airflow Metadata Database
METADATA_DATABASE_NAME=airflow
METADATA_DATABASE_USERNAME=airflow
METADATA_DATABASE_PASSWORD=airflow

# Celery Backend Database
CELERY_BACKEND_NAME=celery
CELERY_BACKEND_USERNAME=celery
CELERY_BACKEND_PASSWORD=celery

# ELT Data Warehouse Database
ELT_DATABASE_NAME=yt_elt
ELT_DATABASE_USERNAME=elt_user
ELT_DATABASE_PASSWORD=elt_password
```

### 3. Generate Fernet Key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copy the output to the `FERNET_KEY` variable in `.env`.

### 4. Build and Push Docker Image

```bash
# Build the custom Airflow image
docker build -t your-dockerhub-username/youtube-data-pipeline:latest .

# Push to Docker Hub
docker push your-dockerhub-username/youtube-data-pipeline:latest
```

### 5. Initialize Airflow

```bash
# Initialize the database
docker-compose up airflow-init

# Start all services
docker-compose up -d
```

## ⚙️ Configuration

### Airflow Variables
The following variables are automatically set via environment variables:
- `API_KEY`: YouTube Data API key
- `CHANNEL_HANDLE`: Target YouTube channel handle (e.g., @channelname)

### Airflow Connections
- `POSTGRES_DB_YT_ELT`: PostgreSQL connection to data warehouse

### Soda Configuration
Edit `include/soda/checks.yml` to customize data quality checks:
- Missing value checks
- Duplicate detection
- Business rule validation (e.g., likes < views)

## 💻 Usage

### Access Airflow UI
```
URL: http://localhost:8080
Username: airflow
Password: airflow
```

### Access Flower (Celery Monitor)
```
URL: http://localhost:5555
```

### Trigger DAGs

The pipeline consists of three chained DAGs:

1. **`produce_json`**: Extracts data from YouTube API
   - Scheduled: Daily at 2:00 PM (14:00)
   - Triggers: `update_datawarehouse`

2. **`update_datawarehouse`**: Loads data into staging and core schemas
   - Triggered by: `produce_json`
   - Triggers: `data_quality_checks`

3. **`data_quality_checks`**: Validates data quality
   - Triggered by: `update_datawarehouse`

**Manual Trigger:**
```bash
# Trigger from UI or CLI
docker exec -it <airflow-scheduler-container> airflow dags trigger produce_json
```

### Run Tests

```bash
# Execute unit tests
docker exec -it <airflow-worker-container> pytest /opt/airflow/tests/unit_test.py -v

# Execute integration tests
docker exec -it <airflow-worker-container> pytest /opt/airflow/tests/integration_test.py -v
```

## 🔄 Data Flow

### Step-by-Step Process

1. **Extract Phase** (`produce_json` DAG)
   ```
   get_playlist_id → get_videos_id → extract_video_details → save_to_json
   ```
   - Retrieves channel's upload playlist ID
   - Fetches all video IDs (paginated)
   - Extracts video details in batches of 50
   - Saves to `data/YT_data_YYYY-MM-DD.json`

2. **Load & Transform Phase** (`update_datawarehouse` DAG)
   ```
   populate_staging_table → populate_core_table
   ```
   - **Staging Schema**: Raw data with full history
     - Inserts new videos
     - Updates existing videos
     - Soft deletes removed videos
   
   - **Core Schema**: Transformed, business-ready data
     - Applies transformations (e.g., duration conversion)
     - Maintains referential integrity

3. **Quality Assurance Phase** (`data_quality_checks` DAG)
   ```
   staging_quality_check → core_quality_check
   ```
   - Validates data integrity
   - Checks business rules
   - Reports anomalies

## 🗄️ Data Warehouse Schema

### Staging Schema (`staging.yt_api`)
Raw data layer with all historical records.

**Columns:**
- `Video_ID` (VARCHAR, PRIMARY KEY)
- `Video_Title` (TEXT)
- `Published_At` (TIMESTAMP)
- `Video_Description` (TEXT)
- `Channel_Title` (VARCHAR)
- `Video_Duration` (VARCHAR)
- `Video_Definition` (VARCHAR)
- `Video_Caption` (VARCHAR)
- `Video_Views` (BIGINT)
- `Likes_Count` (BIGINT)
- `Comments_Count` (BIGINT)
- `Inserted_At` (TIMESTAMP)
- `Updated_At` (TIMESTAMP)

### Core Schema (`core.yt_api`)
Business layer with transformed data.

**Transformations:**
- Duration conversion (ISO 8601 → seconds)
- Data type normalization
- Business rule enforcement

## ✅ Data Quality

### Automated Checks (Soda Core)

**Staging & Core Schemas:**
- ✓ No missing `Video_ID` values
- ✓ No duplicate `Video_ID` values
- ✓ Likes count ≤ Video views
- ✓ Comments count ≤ Video views

**Custom SQL Checks:**
```yaml
checks for yt_api:
  - missing_count("Video_ID") = 0
  - duplicate_count("Video_ID") = 0
  - likes_count_greater_than_video_views = 0
  - comments_count_greater_than_video_views = 0
```

### Failure Handling
- Failed checks trigger task failure in Airflow
- Alerts can be configured via Airflow email settings
- Check results logged to Airflow task logs

## 🧪 Testing

### Test Suite Components

1. **Unit Tests** (`tests/unit_test.py`)
   - Data transformation logic
   - Data loading functions
   - SQL query generation

2. **Integration Tests** (`tests/integration_test.py`)
   - End-to-end pipeline validation
   - Database connectivity
   - Data quality check execution

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/unit_test.py -v

# Run with coverage
pytest tests/ --cov=dags --cov-report=html
```

## 📊 Monitoring

### Service Health Checks

**PostgreSQL:**
```bash
docker exec -it postgres pg_isready -U postgres -d airflow
```

**Redis:**
```bash
docker exec -it redis redis-cli ping
```

**Airflow:**
- Scheduler: `http://localhost:8080/health`
- Workers: Check via Flower at `http://localhost:5555`

### Logs

```bash
# View Airflow scheduler logs
docker logs airflow-scheduler -f

# View worker logs
docker logs airflow-worker -f

# View specific DAG run logs
# Navigate to Airflow UI → DAGs → [dag_name] → Graph → [task] → Logs
```

## 🔧 CI/CD

The project is configured for GitHub Actions with:

- Automated testing on pull requests
- Docker image building and pushing
- Environment variable management via GitHub Secrets

**Required Secrets:**
- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`
- All database credentials
- `API_KEY`
- `FERNET_KEY`

## 🐛 Troubleshooting

### Common Issues

**Issue: Airflow webserver not starting**
```bash
# Check logs
docker logs airflow-webserver

# Verify database migration
docker exec -it airflow-scheduler airflow db check
```

**Issue: YouTube API quota exceeded**
```
Solution: YouTube API has a daily quota limit (10,000 units by default)
- Monitor quota in Google Cloud Console
- Reduce request frequency or batch size
```

**Issue: PostgreSQL connection failed**
```bash
# Verify environment variables
docker exec -it postgres env | grep POSTGRES

# Check connection from Airflow
docker exec -it airflow-scheduler airflow connections get postgres_db_yt_elt
```

**Issue: Worker not picking up tasks**
```bash
# Restart workers
docker-compose restart airflow-worker

# Check Celery via Flower: http://localhost:5555
```

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Write unit tests for new features
- Follow PEP 8 style guide
- Update documentation for API changes
- Add docstrings to all functions

---

## 🙏 Acknowledgments

- Apache Airflow community for the robust orchestration framework
- Soda Core for data quality testing capabilities
- Google for YouTube Data API v3


---

**Built with ❤️ for data engineering excellence**