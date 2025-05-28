# ETL Pipeline Project

A comprehensive and production-ready ETL (Extract, Transform, Load) pipeline built with Apache Airflow for automated data processing of asset and user data, featuring advanced CI/CD workflows, comprehensive testing framework, and complete development management tools.

## ���️ Architecture Overview

This ETL pipeline leverages modern data engineering tools and practices:

- **Orchestration**: Apache Airflow 3.0.0 with CeleryExecutor
- **Database**: PostgreSQL 13 (Primary storage)
- **Cache/Message Broker**: Redis 7.2-bookworm
- **Language**: Python 3.11.9
- **Containerization**: Docker & Docker Compose
- **Admin Interface**: pgAdmin4 for database management
- **Monitoring**: Flower for Celery task monitoring
- **CI/CD**: GitHub Actions with comprehensive workflows
- **Testing**: pytest with coverage reporting and performance testing
- **Development**: Cross-platform development management scripts

## ��� Enhanced Project Structure

```
ETL/
├── .github/workflows/         # CI/CD Pipelines
│   └── ci-cd.yml             # Comprehensive GitHub Actions workflow
├── config/                   # Configuration files
│   ├── airflow.cfg          # Airflow configuration
│   └── config.py            # Application configuration constants
├── dags/                    # Airflow DAG definitions
│   ├── __init__.py
│   └── dag.py               # Main ETL pipeline DAG
├── main/                    # Core ETL functionality
│   ├── __init__.py
│   ├── email_service.py     # Email notification system
│   ├── extract.py           # Data extraction logic
│   ├── load.py              # Data loading to destinations
│   ├── tasks.py             # Airflow task callables
│   ├── transform_asset.py   # Asset data transformation
│   ├── transform_user.py    # User data transformation
│   ├── validate.py          # Data validation and splitting
│   └── utils/               # Utility modules
│       └── create_database.py # Database schema management
├── scripts/                 # Development Management Scripts
│   ├── complete-setup.sh    # Linux/Mac complete setup
│   ├── complete-setup.bat   # Windows complete setup
│   ├── dev-env.sh          # Linux/Mac environment manager
│   ├── dev-env.bat         # Windows environment manager
│   ├── dev-manager.sh      # Enhanced Linux/Mac development manager
│   ├── dev-manager.bat     # Enhanced Windows development manager
│   ├── test-runner.sh      # Comprehensive testing script
│   └── init-db.sql         # Database initialization script
├── test/                    # Comprehensive Testing Framework
│   ├── conftest.py         # Test configuration and fixtures
│   ├── test_etl_units.py   # Unit tests for ETL components
│   └── test_integration.py # Integration tests for complete workflows
├── logs/                   # Airflow logs (auto-generated)
├── plugins/                # Custom Airflow plugins
├── temp/                   # Temporary processing files
├── docker-compose.yaml     # Production Docker services
├── docker-compose.dev.yaml # Development Docker services
├── Dockerfile              # Production Airflow image
├── Dockerfile.dev          # Development Airflow image
├── pyproject.toml          # Poetry project configuration
├── requirements.txt        # Production Python dependencies
├── requirements-dev.txt    # Development Python dependencies
├── pytest.ini             # Testing configuration
├── .env                    # Production environment variables
├── .env.development        # Development environment variables
├── .env.development.example # Development environment template
├── .gitignore             # Git ignore patterns
├── .flake8               # Code style configuration
├── DEVELOPMENT.md        # Development guide
├── SETUP_COMPLETE.md     # Setup completion documentation
└── README.md             # This comprehensive documentation
```

## ��� Enhanced Features

### Core ETL Pipeline
- ✅ **Automated ETL Pipeline**: Scheduled daily processing at 2 AM
- ✅ **Google Sheets Integration**: Data extraction from Google Sheets
- ✅ **Dual Data Processing**: Separate asset and user data transformation
- ✅ **Data Validation**: Comprehensive validation and data splitting
- ✅ **Multi-destination Loading**: Supabase and Google Drive integration
- ✅ **Email Notifications**: Success/failure notifications via SMTP
- ✅ **Database Management**: Automatic schema creation and management

### Development & Operations
- ✅ **Containerized Deployment**: Full Docker-based infrastructure
- ✅ **Development Environment**: Complete docker-compose.dev.yaml setup
- ✅ **Web Admin Interface**: pgAdmin4 for database administration
- ✅ **Monitoring Dashboard**: Flower for Celery task monitoring
- ✅ **Cross-platform Scripts**: Development management for Windows/Linux/Mac
- ✅ **Environment Management**: Automated setup and configuration scripts

### CI/CD & Testing
- ✅ **GitHub Actions CI/CD**: Multi-stage pipeline with build, test, security, deploy
- ✅ **Comprehensive Testing**: Unit, integration, DAG, and performance tests
- ✅ **Security Scanning**: Bandit security analysis and dependency scanning
- ✅ **Code Quality**: Black formatting, flake8 linting, and coverage reporting
- ✅ **Test Categorization**: Markers for unit, integration, dag, and performance tests
- ✅ **Automated Documentation**: Test reports and coverage badges

## ��� Quick Start

### Prerequisites
- Docker & Docker Compose installed
- Git
- Python 3.11.9 (for local development)

### Option 1: Complete Automated Setup

#### Linux/Mac
```bash
# Clone the repository
git clone <your-repository-url>
cd ETL

# Run complete setup script
chmod +x scripts/complete-setup.sh
./scripts/complete-setup.sh

# Start development environment
./scripts/dev-manager.sh start
```

#### Windows
```cmd
# Clone the repository
git clone <your-repository-url>
cd ETL

# Run complete setup script
scripts\complete-setup.bat

# Start development environment
scripts\dev-manager.bat start
```

### Option 2: Manual Setup

1. **Clone the repository**
   ```bash
   git clone <your-repository-url>
   cd ETL
   ```

2. **Environment Setup**
   ```bash
   # Copy and configure environment
   cp .env.development.example .env.development
   
   # Update configuration with your actual values
   ```

3. **Start Development Services**
   ```bash
   # Build and start all development services
   docker-compose -f docker-compose.dev.yaml up -d
   
   # Check service status
   docker-compose -f docker-compose.dev.yaml ps
   ```

## ��� Development Management

### Using Development Scripts

#### Environment Management
```bash
# Linux/Mac
./scripts/dev-env.sh [check|setup|start|stop|restart|logs|clean]

# Windows
scripts\dev-env.bat [check|setup|start|stop|restart|logs|clean]
```

#### Enhanced Development Manager
```bash
# Linux/Mac
./scripts/dev-manager.sh [start|stop|restart|status|logs|test|clean|build|init-db]

# Windows
scripts\dev-manager.bat [start|stop|restart|status|logs|test|clean|build|init-db]
```

#### Comprehensive Testing
```bash
# Run all tests
./scripts/test-runner.sh

# Run specific test categories
./scripts/test-runner.sh unit
./scripts/test-runner.sh integration
./scripts/test-runner.sh dag
./scripts/test-runner.sh performance
```

### Access Points

After starting the development environment:

- **Airflow UI**: http://localhost:8080 (admin/admin123)
- **pgAdmin**: http://localhost:5050 (admin@admin.com/admin123)
- **Flower** (Celery monitoring): http://localhost:5555
- **Redis Commander**: http://localhost:8081

## ��� Testing Framework

### Test Categories

#### Unit Tests (`test/test_etl_units.py`)
- Individual component testing
- Mock external dependencies
- Fast execution (< 1 second each)

#### Integration Tests (`test/test_integration.py`)
- End-to-end workflow testing
- Database integration
- External service integration

#### DAG Tests
- DAG structure validation
- Task dependency verification
- Configuration testing

#### Performance Tests
- Load testing
- Memory usage validation
- Execution time benchmarking

### Running Tests

```bash
# All tests
pytest

# Specific categories
pytest -m unit
pytest -m integration
pytest -m dag
pytest -m performance

# With coverage
pytest --cov=main --cov=dags

# Generate HTML coverage report
pytest --cov=main --cov=dags --cov-report=html
```

## ��� CI/CD Pipeline

### GitHub Actions Workflow

Our comprehensive CI/CD pipeline includes:

#### 1. Build Stage
- Python environment setup
- Dependency installation
- Code formatting check (Black)
- Linting (flake8)

#### 2. Test Stage
- Unit tests with coverage
- Integration tests
- DAG validation tests
- Performance tests

#### 3. Security Stage
- Security vulnerability scanning (Bandit)
- Dependency security check
- SAST analysis

#### 4. Deploy Stage
- Docker image building
- Container security scanning
- Deployment to staging/production

## ��� Troubleshooting

### Common Issues

#### 1. Services Not Starting
```bash
# Check logs
docker-compose -f docker-compose.dev.yaml logs airflow-scheduler
docker-compose -f docker-compose.dev.yaml logs postgres

# Restart services
docker-compose -f docker-compose.dev.yaml restart
```

#### 2. Database Connection Issues
```bash
# Verify PostgreSQL is running
docker-compose -f docker-compose.dev.yaml ps postgres

# Check database connectivity
docker-compose -f docker-compose.dev.yaml exec postgres pg_isready -U airflow

# Reset database
./scripts/dev-manager.sh init-db
```

#### 3. Test Failures
```bash
# Run tests with verbose output
pytest -v

# Clear test cache
pytest --cache-clear

# Debug mode
pytest --pdb
```

## ��� Contributing

### Development Process

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/new-feature`
3. **Set up development environment**: `./scripts/complete-setup.sh`
4. **Make changes and test locally**: `./scripts/test-runner.sh`
5. **Run code quality checks**: `black main/ && flake8 main/`
6. **Submit a Pull Request**

### Development Guidelines

- **Code Style**: Follow PEP 8, use Black formatter
- **Documentation**: Add docstrings to functions and classes
- **Testing**: Write tests for new features (aim for >90% coverage)
- **Dependencies**: Update `pyproject.toml` for new dependencies

---

## ��� Project Status

**Current Version**: 1.0.0  
**Python Version**: 3.11.9  
**Airflow Version**: 3.0.0  
**Docker Compose Version**: 3.8  

### Recent Updates

- ✅ Enhanced CI/CD pipeline with security scanning
- ✅ Comprehensive testing framework with coverage reporting
- ✅ Cross-platform development management scripts
- ✅ Complete Docker-based development environment
- ✅ Database initialization and migration scripts
- ✅ Performance monitoring and optimization tools
- ✅ Documentation overhaul with setup guides

---

*Last Updated: December 2024*  
*Maintained by: ETL Development Team*
