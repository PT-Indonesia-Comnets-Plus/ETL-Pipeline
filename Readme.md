# Project is under construction🚧.

# ETL Pipeline dengan Poetry dan Docker

## Overview

Project ETL (Extract, Transform, Load) menggunakan Apache Airflow dengan Poetry untuk dependency management dan Docker untuk containerization.

## Prerequisites

- Python 3.11+
- Poetry
- Docker Desktop
- Docker Compose

## Installation Poetry (jika belum ada)

### Windows (PowerShell)

```powershell
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
```

### Linux/macOS

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

## Quick Start

### 1. Clone dan Setup

```bash
git clone <repository-url>
cd ETL
```

### 2. Setup Environment (Windows)

```powershell
# Jalankan script PowerShell
.\setup.ps1
```

### 3. Setup Environment (Linux/macOS)

```bash
# Buat file executable
chmod +x setup.sh

# Jalankan script
./setup.sh
```

### 4. Start Services

```bash
# Start semua services
docker-compose up -d

# Atau start dengan logs
docker-compose up
```

## Access Points

| Service    | URL                   | Credentials          |
| ---------- | --------------------- | -------------------- |
| Airflow UI | http://localhost:8080 | admin/admin          |
| PgAdmin    | http://localhost:5050 | admin@admin.com/root |
| PostgreSQL | localhost:5432        | airflow/airflow      |
| Redis      | localhost:6379        | -                    |

## Project Structure

```
ETL/
├── pyproject.toml          # Poetry configuration
├── poetry.lock             # Poetry lock file
├── Dockerfile              # Docker build configuration
├── docker-compose.yaml     # Docker services configuration
├── .env                    # Environment variables
├── setup.sh               # Linux/macOS setup script
├── setup.ps1              # Windows setup script
├── dags/                  # Airflow DAGs
├── main/                  # Main ETL modules
├── config/                # Configuration files
├── logs/                  # Airflow logs
├── plugins/               # Airflow plugins
├── temp/                  # Temporary files
└── test/                  # Test files
```

## Poetry Commands

### Development

```bash
# Install dependencies
poetry install

# Add new dependency
poetry add package-name

# Add development dependency
poetry add --group dev package-name

# Update dependencies
poetry update

# Show dependency tree
poetry show --tree

# Export requirements.txt (jika dibutuhkan)
poetry export -f requirements.txt --output requirements.txt
```

### Virtual Environment

```bash
# Activate virtual environment
poetry shell

# Run command in virtual environment
poetry run python script.py

# Check virtual environment info
poetry env info
```

## Docker Commands

### Build dan Deployment

```bash
# Build images
docker-compose build

# Build tanpa cache
docker-compose build --no-cache

# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f [service-name]

# Restart specific service
docker-compose restart [service-name]
```

### Debugging

```bash
# Execute command dalam container
docker-compose exec airflow-scheduler bash

# Check container status
docker-compose ps

# View resource usage
docker stats
```

## Environment Variables

Edit file `.env` untuk konfigurasi:

```bash
# Airflow Configuration
AIRFLOW_UID=197610
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

# Project Configuration
AIRFLOW_PROJ_DIR=.

# Application Configuration
Supabase_Conn=your_supabase_connection
Spreadsheet_ID=your_spreadsheet_id
GOOGLE_CREDENTIALS_TARGET_FOLDER_ID=your_folder_id
SMTP_CREDENTIALS_JSON=your_smtp_config
ETL_NOTIFICATION_RECIPIENT=your_email
Google_Credentials_Key=your_google_key
```

## Development Workflow

### 1. Mengembangkan DAG

1. Edit file di folder `dags/`
2. Airflow akan auto-reload DAG baru
3. Check di Airflow UI untuk memastikan DAG loaded

### 2. Mengembangkan ETL Logic

1. Edit file di folder `main/`
2. Restart Airflow workers jika perlu:
   ```bash
   docker-compose restart airflow-worker airflow-scheduler
   ```

### 3. Testing

```bash
# Test dengan Poetry
poetry run pytest

# Test dalam container
docker-compose exec airflow-scheduler python -m pytest
```

## Troubleshooting

### Common Issues

#### Poetry tidak ditemukan

```bash
# Add Poetry to PATH (Linux/macOS)
export PATH="$HOME/.local/bin:$PATH"

# Windows - tambahkan ke System PATH:
# %APPDATA%\Python\Scripts
```

#### Docker build error

```bash
# Clear Docker cache
docker system prune -a

# Rebuild tanpa cache
docker-compose build --no-cache
```

#### Permission issues (Linux/macOS)

```bash
# Fix ownership
sudo chown -R $USER:$USER logs/ dags/ plugins/ config/ temp/ main/

# Update AIRFLOW_UID di .env
echo "AIRFLOW_UID=$(id -u)" >> .env
```

#### Container tidak bisa start

```bash
# Check logs
docker-compose logs airflow-init

# Check available resources
docker system df
```

### Logs Location

- Airflow logs: `logs/`
- Docker logs: `docker-compose logs [service]`
- System logs: `/var/log/` (Linux) atau Event Viewer (Windows)

## Production Deployment

### Security Considerations

1. Ubah default passwords
2. Setup proper SSL certificates
3. Configure firewall rules
4. Use secrets management
5. Setup backup strategy

### Performance Tuning

1. Adjust worker processes
2. Configure resource limits
3. Setup monitoring
4. Optimize database connections

## Monitoring

### Health Checks

```bash
# Check all services
docker-compose ps

# Check Airflow health
curl http://localhost:8080/health

# Check database
docker-compose exec postgres pg_isready -U airflow
```

### Metrics

- Airflow UI: Task success/failure rates
- Docker stats: Resource usage
- Database: Connection pools
- Logs: Error patterns

## Backup dan Recovery

### Database Backup

```bash
# Backup PostgreSQL
docker-compose exec postgres pg_dump -U airflow airflow > backup.sql

# Restore
docker-compose exec -T postgres psql -U airflow airflow < backup.sql
```

### Configuration Backup

```bash
# Backup konfigurasi penting
tar -czf backup-config.tar.gz .env config/ dags/ main/
```

## Support

Untuk issues dan pertanyaan:

1. Check logs untuk error messages
2. Verify environment configuration
3. Test dengan minimal setup
4. Contact development team

---

**Last Updated**: May 2025
