# PT Indonesia Comnets Plus ETL Pipeline

A production-ready, scalable ETL pipeline for processing asset and user data, with outputs to Supabase and Google Drive.

## Overview

This project implements a robust ETL (Extract, Transform, Load) pipeline using Apache Airflow for orchestration. The pipeline extracts data from various sources, transforms it according to business rules, validates the data, and loads it into destination systems.

## Project Structure

The codebase has been refactored for improved maintainability, scalability, and readability:

```
pt-indonesia-comnets-plus-etl-pipeline/
├── config/                    # Configuration files
│   ├── airflow.cfg           # Airflow configuration
│   ├── deploy.conf           # Deployment configuration
│   ├── nginx-ssl.conf        # Nginx SSL configuration
│   └── schema.sql            # Database schema definitions
├── dags/                      # Airflow DAGs
│   ├── __init__.py
│   └── dag.py                # Main pipeline DAG definition
├── main/                      # Core ETL functionality
│   ├── __init__.py
│   ├── config.py             # Centralized configuration constants
│   ├── email_service.py      # Email notification functionality
│   ├── extract.py            # Data extraction logic
│   ├── load.py               # Data loading logic
│   ├── main_pipeline.py      # Integrated ETL pipeline class
│   ├── tasks.py              # Airflow task callables
│   ├── transform_asset.py    # Asset data transformation
│   ├── transform_user.py     # User data transformation
│   ├── validate.py           # Data validation logic
│   └── utils/                # Utility modules
├── scripts/                   # Deployment and maintenance scripts
│   ├── backup.sh             # Database backup script
│   ├── backup_verify.sh      # Backup verification script
│   ├── db_migrate.sh         # Database migration script
│   ├── deploy.sh             # Deployment script
│   ├── health_check.sh       # Health monitoring script
│   ├── monitor_performance.sh # System performance monitoring
│   ├── setup_firewall.sh     # Firewall setup script
│   ├── setup_github_secrets.sh # GitHub secrets setup helper
│   ├── setup_monitoring.sh   # Comprehensive logging setup
│   ├── setup_s3_backup.sh    # External backup to S3 setup
│   ├── setup_ssl.sh          # SSL/TLS with Certbot setup
│   └── setup_vps.sh          # VPS setup script
├── test/                      # Unit and integration tests
├── .github/workflows/         # CI/CD configuration
├── docker-compose.yaml        # Development Docker composition
├── docker-compose.prod.yml    # Production Docker composition
├── Dockerfile                 # Production-ready Docker image definition
├── pyproject.toml             # Poetry project definition
├── requirements.txt           # Dependencies for non-Poetry environments
└── README.md                  # Project documentation
```

## Key Components

### Pipeline Architecture

The ETL pipeline consists of these main stages:

1. **Extraction**: Data is extracted from source systems (Google Sheets, APIs, etc.)
2. **Transformation**:
   - Asset data transformation
   - User data transformation
3. **Validation**: Data validation and splitting
4. **Loading**: Loading validated data to destination systems (Supabase, Google Drive)
5. **Notification**: Email notifications for pipeline success/failure

### Code Organization

- **Centralized Configuration**: Constants and settings in `main/config.py`
- **Modular Tasks**: Each ETL step is encapsulated in its own module
- **Integrated Pipeline**: `main/main_pipeline.py` provides a reusable ETL pipeline class
- **Clean DAG Definition**: `dags/dag.py` focuses solely on workflow orchestration
- **Improved Error Handling**: Consistent error handling across all pipeline components
- **Email Notifications**: Comprehensive email notification system for pipeline status

## Getting Started

### Prerequisites

- Python 3.11+
- Poetry (for dependency management)
- Docker and Docker Compose (for containerization)
- Access to required external services (Google Sheets, Supabase, SMTP server)

### Installation

#### Using Poetry (recommended)

1. Clone the repository
2. Install dependencies:
   ```bash
   poetry install
   ```
3. Activate the virtual environment:
   ```bash
   poetry shell
   ```

#### Using pip

```bash
pip install -r requirements.txt
```

### Configuration

1. Set up required environment variables or Airflow variables:

   - `SMTP_CREDENTIALS_JSON`: SMTP server credentials
   - `ETL_NOTIFICATION_RECIPIENT`: Email recipients for notifications

2. Configure database connection in `main/utils/config_database.py`

### Running the Pipeline

#### Using Airflow UI

1. Start Airflow services:
   ```bash
   docker-compose up
   ```
2. Access Airflow UI at `http://localhost:8080`
3. Enable and trigger the `asset_data_pipeline` DAG

#### Using Command Line

For local development or testing:

```bash
python -c "from main.main_pipeline import pipeline; pipeline.run_pipeline()"
```

## Production Deployment

### VPS Requirements

- At least 2GB RAM (4GB recommended)
- 2+ CPU cores
- 20GB+ disk space
- Ubuntu 20.04 LTS or newer

### Comprehensive Deployment Process

1. **Prepare your VPS**

   Update your system and install Git:

   ```bash
   apt-get update && apt-get upgrade -y
   apt-get install -y git
   ```

2. **Clone the repository**

   ```bash
   git clone https://github.com/yourusername/etl-pipeline.git
   cd etl-pipeline
   ```

3. **Run the VPS setup script**

   ```bash
   chmod +x scripts/setup_vps.sh
   ./scripts/setup_vps.sh
   ```

4. **Configure environment variables**

   The setup script will create an `.env` file from the template. Edit this file with your production settings:

   ```bash
   nano .env
   ```

5. **Configure Firewall**

   ```bash
   chmod +x scripts/setup_firewall.sh
   sudo ./scripts/setup_firewall.sh
   ```

6. **Setup SSL/TLS with Certbot**

   ```bash
   chmod +x scripts/setup_ssl.sh
   sudo ./scripts/setup_ssl.sh --domain your-domain.com --email your-email@example.com
   ```

7. **Setup External Backups to S3**

   ```bash
   chmod +x scripts/setup_s3_backup.sh
   ./scripts/setup_s3_backup.sh --bucket your-bucket-name
   ```

8. **Configure Comprehensive Monitoring**

   ```bash
   chmod +x scripts/setup_monitoring.sh
   sudo ./scripts/setup_monitoring.sh --email admin@example.com
   ```

9. **Deploy the application**

   ```bash
   chmod +x scripts/deploy.sh
   ./scripts/deploy.sh
   ```

10. **Verify deployment**

    Access the Airflow UI at https://your-domain.com

    Check the deployment status:

    ```bash
    ./scripts/health_check.sh
    ```

### Production Deployment Features

Our production-ready setup includes:

#### Security Measures

- **SSL/TLS Encryption**: Automatic setup with Let's Encrypt Certbot
- **Firewall Configuration**: UFW with restrictive rules
- **Fail2Ban Integration**: Protection against brute force attacks
- **Secure SSH Configuration**: Password authentication disabled
- **Automatic Security Updates**: Unattended upgrades for security patches

#### High Availability and Reliability

- **Automated Health Checks**: Regular monitoring of all services
- **Container Restart Policies**: Automatic recovery from failures
- **Comprehensive Logging**: Centralized logging with log rotation
- **Error Alerts**: Immediate notification of critical issues

#### Backup and Disaster Recovery

- **Automated Database Backups**: Daily local backups
- **External Backups to S3**: Offsite backup storage
- **Backup Verification**: Automatic verification of backup integrity
- **Backup Retention Policy**: Configurable retention periods
- **Backup Restoration Procedure**: Documented recovery process

#### Monitoring and Observability

- **System Performance Monitoring**: CPU, memory, disk, and network usage
- **Container Resource Tracking**: Docker container metrics
- **Airflow Task Monitoring**: DAG and task execution metrics
- **Custom Alert Thresholds**: Configurable alert thresholds
- **Daily Monitoring Reports**: Comprehensive daily status reports

#### CI/CD Integration

- **Automated Testing**: Test execution before deployment
- **Docker Image Building**: Automated Docker image creation
- **Deployment Automation**: Zero-downtime deployment to production
- **Deployment Verification**: Post-deployment health checks
- **Rollback Capability**: Easy rollback in case of deployment failure

### Using the Deployment Scripts

#### Deployment Script Options

The main deployment script (`scripts/deploy.sh`) includes the following options:

```bash
./scripts/deploy.sh --help
```

- `-f, --force`: Force deployment even if checks fail
- `-t, --tag TAG`: Specify Docker image tag (default: latest)
- `-n, --no-backup`: Skip database backup before deployment
- `-d, --dry-run`: Show what would be done without making changes
- `-v, --verbose`: Enable verbose output

#### Advanced Deployment Example

```bash
# Deploy a specific tag with verbose output
./scripts/deploy.sh --tag v1.2.3 --verbose

# Perform a dry run to check what would happen
./scripts/deploy.sh --dry-run

# Deploy without taking a backup
./scripts/deploy.sh --no-backup
```

## Monitoring and Maintenance

### Comprehensive Health Monitoring

The health monitoring system includes:

1. **Scheduled Health Checks**

   - Automatic checks every 10 minutes
   - Email alerts on service disruptions
   - Auto-recovery attempts for failed services

2. **System Performance Monitoring**

   - CPU, memory, and disk usage tracking
   - Network traffic analysis
   - Container resource usage monitoring

3. **Database Monitoring**

   - Connection pool status
   - Query performance tracking
   - Backup status verification

4. **Airflow Task Monitoring**
   - DAG success/failure tracking
   - Task duration monitoring
   - Resource usage per task

### Advanced Backup Strategies

1. **Tiered Backup System**

   - Local daily backups (7-day retention)
   - S3 external backups (30-day retention)
   - Weekly backup verification

2. **Backup Management**

   - Automatic cleanup of old backups
   - Compression to minimize storage
   - Backup metadata tracking

3. **Restoration Testing**
   - Documented restoration procedures
   - Optional periodic restoration tests

### Manual Operations

#### Manual Backup Creation

```bash
# Create a local backup
./scripts/backup.sh

# Create and push backup to S3
./scripts/backup_to_s3.sh

# Verify recent backups
./scripts/backup_verify.sh
```

#### Performance Analysis

```bash
# Generate a system performance report
./scripts/monitor_performance.sh

# Check comprehensive system status
./scripts/monitor_etl.sh
```

## Upgrading

To upgrade your ETL pipeline deployment:

1. Pull the latest code changes:

   ```bash
   git pull origin main
   ```

2. Run database migrations if needed:

   ```bash
   ./scripts/db_migrate.sh
   ```

3. Deploy the latest version:
   ```bash
   ./scripts/deploy.sh
   ```

## Testing

Run the tests with:

```bash
pytest
```

Or for more detailed test information:

```bash
pytest -v test/
```

Make sure to run integration tests in a controlled environment to avoid affecting production data.

## License

[Specify License]

## Contributors

- [Your Name/Team]
