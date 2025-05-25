# ETL Pipeline Project

A robust and scalable ETL (Extract, Transform, Load) pipeline built with Apache Airflow for automated data processing of asset and user data, with integration to Supabase and Google Drive.

## 🏗️ Architecture Overview

This ETL pipeline leverages modern data engineering tools and practices:

- **Orchestration**: Apache Airflow 3.0.0 with CeleryExecutor
- **Database**: PostgreSQL 13 (Primary storage)
- **Cache/Message Broker**: Redis 7.2-bookworm
- **Language**: Python 3.11.9
- **Containerization**: Docker & Docker Compose
- **Admin Interface**: pgAdmin4 for database management

## 📋 Current Project Structure

```
ETL/
├── config/                    # Configuration files
│   ├── airflow.cfg           # Airflow configuration
│   └── config.py             # Application configuration constants
├── dags/                      # Airflow DAG definitions
│   ├── __init__.py
│   └── dag.py                # Main ETL pipeline DAG
├── main/                      # Core ETL functionality
│   ├── __init__.py
│   ├── email_service.py      # Email notification system
│   ├── extract.py            # Data extraction logic
│   ├── load.py               # Data loading to destinations
│   ├── tasks.py              # Airflow task callables
│   ├── transform_asset.py    # Asset data transformation
│   ├── transform_user.py     # User data transformation
│   ├── validate.py           # Data validation and splitting
│   └── utils/                # Utility modules
│       └── create_database.py # Database schema management
├── logs/                      # Airflow logs (auto-generated)
├── plugins/                   # Custom Airflow plugins
├── temp/                      # Temporary processing files
├── docker-compose.yaml        # Docker services configuration
├── Dockerfile                 # Custom Airflow image
├── pyproject.toml            # Poetry project configuration
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables
└── README.md                 # This documentation
```

## 🚀 Features

- ✅ **Automated ETL Pipeline**: Scheduled daily processing at 2 AM
- ✅ **Google Sheets Integration**: Data extraction from Google Sheets
- ✅ **Dual Data Processing**: Separate asset and user data transformation
- ✅ **Data Validation**: Comprehensive validation and data splitting
- ✅ **Multi-destination Loading**: Supabase and Google Drive integration
- ✅ **Email Notifications**: Success/failure notifications via SMTP
- ✅ **Database Management**: Automatic schema creation and management
- ✅ **Containerized Deployment**: Full Docker-based infrastructure
- ✅ **Web Admin Interface**: pgAdmin4 for database administration

## 🛠️ Technology Stack

| Component       | Technology     | Version      |
| --------------- | -------------- | ------------ |
| Orchestration   | Apache Airflow | 3.0.0        |
| Runtime         | Python         | 3.11.9       |
| Database        | PostgreSQL     | 13           |
| Cache/Broker    | Redis          | 7.2-bookworm |
| Database Admin  | pgAdmin4       | Latest       |
| Package Manager | Poetry         | Latest       |

## 📦 Dependencies

### Core Data Processing

- **pandas** (>=2.2.3): Data manipulation and analysis
- **numpy** (>=2.2.6): Numerical computing support
- **pyarrow** (>=20.0.0): Columnar data processing

### Google Services Integration

- **gspread** (>=6.2.1): Google Sheets API client
- **google-auth** (>=2.40.1): Google authentication
- **google-auth-oauthlib** (>=1.2.2): OAuth2 flow
- **google-auth-httplib2** (>=0.2.0): HTTP transport
- **google-api-python-client** (>=2.169.0): Google APIs client

### Database Connectivity

- **sqlalchemy** (>=2.0.41): Database ORM and connection management
- **psycopg2-binary** (>=2.9.10): PostgreSQL adapter

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose installed
- Git
- Python 3.11.9 (for local development)

### Installation

1. **Clone the repository**

   ```bash
   git clone <your-repository-url>
   cd ETL
   ```

2. **Environment Setup**

   ```bash
   # Your .env file is already configured with development settings
   # Update the following variables with your actual values:
   # - GOOGLE_SHEETS_ID
   # - GOOGLE_CREDENTIALS_JSON
   # - SMTP credentials
   # - Supabase configuration
   ```

3. **Start all services**

   ```bash
   # Build and start all services
   docker-compose up -d

   # Check service status
   docker-compose ps
   ```

4. **Access the interfaces**
   - **Airflow UI**: http://localhost:8080 (admin/admin123)
   - **pgAdmin**: http://localhost:5050 (admin@admin.com/root)
   - **Flower** (Celery monitoring): http://localhost:5555 (with `--profile flower`)

## ⚙️ Configuration

### Environment Variables (.env)

Your current configuration includes:

#### Core Airflow Settings

```properties
AIRFLOW_UID=50000
AIRFLOW_FERNET_KEY=Mg3T0pSm4oUbdWVio84z_uDbu6qBHKVcx1HCmpVBNcM=
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin123
```

#### Database Configuration

```properties
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=airflow_dev
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow123
```

#### Google Services (Update these)

```properties
GOOGLE_SHEETS_ID=your_development_spreadsheet_id
GOOGLE_CREDENTIALS_JSON={"type": "service_account", "project_id": "your-dev-project"}
GOOGLE_CREDENTIALS_TARGET_FOLDER_ID=your_dev_folder_id
```

#### Email Notifications (Update these)

```properties
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=dev-etl@company.com
SMTP_PASSWORD=your_dev_smtp_password
FROM_EMAIL=dev-etl@company.com
TO_EMAILS=developer@company.com,qa@company.com
```

#### External Services (Update these)

```properties
SUPABASE_URL=https://your-dev-project.supabase.co
SUPABASE_KEY=your_dev_supabase_anon_key
```

## 📊 Pipeline Overview

### ETL Workflow (dag.py)

The main pipeline (`asset_data_pipeline`) consists of:

1. **ensure_database_schema**: Creates/updates database schema
2. **extract**: Extracts data from Google Sheets
3. **transform_asset_data**: Processes asset-related data
4. **transform_user_data**: Processes user-related data
5. **validate_and_spliting**: Validates and splits processed data
6. **load**: Loads data to Supabase and Google Drive
7. **send_notification_email**: Sends success notification

### Task Dependencies

```
ensure_database_schema → extract → [transform_asset_data, transform_user_data]
                                  ↓
validate_and_spliting ← [transform_asset_data, transform_user_data]
                                  ↓
                               load
                                  ↓
                        send_notification_email
```

### Schedule

- **Frequency**: Daily at 2:00 AM (`0 2 * * *`)
- **Catchup**: Disabled
- **Tags**: `etl_main`, `supabase`, `production`

## 🔧 Development

### Using Poetry (Recommended)

```bash
# Install dependencies
poetry install

# Activate virtual environment
poetry shell

# Add new dependency
poetry add package-name

# Update dependencies
poetry update
```

### Using pip

```bash
# Install dependencies
pip install -r requirements.txt

# Generate requirements (if using Poetry)
poetry export -f requirements.txt --output requirements.txt
```

### Local Development

1. **Set development flags in .env**

   ```properties
   ETL_ENVIRONMENT=development
   DEBUG_MODE=true
   GENERATE_TEST_DATA=true
   MOCK_EXTERNAL_APIS=false
   ```

2. **Run individual components**

   ```bash
   # Test extraction
   python -c "from main.tasks import run_extractor; run_extractor()"

   # Test transformation
   python -c "from main.tasks import run_asset_transformer; run_asset_transformer()"
   ```

### Adding New Features

1. Create new modules in the `main/` directory
2. Add task functions in `main/tasks.py`
3. Update the DAG in `dags/dag.py`
4. Add any new dependencies to `pyproject.toml`

## 🚢 Deployment

### Production Deployment

1. **Update environment for production**

   ```properties
   ETL_ENVIRONMENT=production
   DEBUG_MODE=false
   ENCRYPT_SECRETS=true
   ENABLE_SSL=true
   ```

2. **Use production compose file**

   ```bash
   docker-compose -f docker-compose.prod.yml up -d
   ```

3. **Scale workers if needed**
   ```bash
   docker-compose up -d --scale airflow-worker=3
   ```

## 📊 Monitoring

### Available Interfaces

1. **Airflow Web UI** (http://localhost:8080)

   - DAG monitoring and management
   - Task logs and status
   - Connection and variable management

2. **pgAdmin4** (http://localhost:5050)

   - Database administration
   - Query execution
   - Performance monitoring

3. **Flower** (http://localhost:5555)
   ```bash
   # Enable Flower for Celery monitoring
   docker-compose --profile flower up -d
   ```

### Monitoring Configuration

Current monitoring settings in .env:

```properties
LOG_LEVEL=DEBUG
ENABLE_MONITORING=true
ENABLE_PERFORMANCE_TRACKING=true
ENABLE_DATA_VALIDATION=true
ENABLE_ERROR_RECOVERY=true
```

## 🐛 Troubleshooting

### Common Issues

1. **Services not starting**

   ```bash
   # Check logs
   docker-compose logs airflow-scheduler
   docker-compose logs postgres

   # Restart services
   docker-compose restart
   ```

2. **Database connection issues**

   ```bash
   # Verify PostgreSQL is running
   docker-compose ps postgres

   # Check database connectivity
   docker-compose exec postgres pg_isready -U airflow
   ```

3. **Memory/Resource issues**

   ```bash
   # Check system resources
   docker stats

   # Adjust limits in .env
   MEMORY_LIMIT_MB=2048
   MAX_WORKERS=1
   ```

4. **Google Sheets authentication**
   - Verify `GOOGLE_CREDENTIALS_JSON` format
   - Check Google API quotas
   - Ensure service account has proper permissions

### Log Locations

- **Airflow logs**: `./logs/` directory
- **Container logs**: `docker-compose logs <service-name>`
- **Application logs**: Check Airflow UI → Admin → Logs

## 🔧 Maintenance

### Regular Tasks

1. **Update dependencies**

   ```bash
   poetry update
   poetry export -f requirements.txt --output requirements.txt
   ```

2. **Clean up old logs**

   ```bash
   # Logs are automatically rotated, but manual cleanup:
   find ./logs -name "*.log" -mtime +7 -delete
   ```

3. **Database maintenance**
   ```bash
   # Access database via pgAdmin or CLI
   docker-compose exec postgres psql -U airflow -d airflow_dev
   ```

## 📚 API Reference

### Key Configuration Constants

Located in `config/config.py`:

- DAG default arguments
- Retry policies
- Email configuration
- Task timeouts

### Main Task Functions

Located in `main/tasks.py`:

- `run_extractor()`: Data extraction from Google Sheets
- `run_asset_transformer()`: Asset data processing
- `run_user_transformer()`: User data processing
- `run_validator()`: Data validation and splitting
- `run_loader()`: Loading to destinations

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Make changes and test locally
4. Update documentation if needed
5. Commit changes: `git commit -am 'Add new feature'`
6. Push to branch: `git push origin feature/new-feature`
7. Submit a Pull Request

### Development Guidelines

- Follow PEP 8 style guide
- Add docstrings to functions and classes
- Update `pyproject.toml` for new dependencies
- Test changes in development environment
- Update README for significant changes

## 📄 License

MIT License - see LICENSE file for details.

## 📞 Support

- **Issues**: Create a GitHub issue for bug reports
- **Email**: Configure in .env `TO_EMAILS` for notifications
- **Logs**: Check Airflow UI and container logs for debugging

---

**Current Version**: 0.1.0  
**Python Version**: 3.11.9  
**Airflow Version**: 3.0.0
