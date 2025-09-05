# MySQL Database Tests

This document explains how to set up and run the MySQL database tests for the DataHub Multi-Tenant Router.

## Prerequisites

1. **MySQL Server**: You need a running MySQL server (version 5.7 or higher)
2. **aiomysql**: The Python package is already included in `requirements.txt`

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up MySQL Database

Connect to your MySQL server and run the following SQL commands:

```sql
-- Create the test database
CREATE DATABASE datahub_router_test;

-- Create the test user
CREATE USER 'datahub'@'localhost' IDENTIFIED BY 'datahub';

-- Grant privileges to the test user
GRANT ALL PRIVILEGES ON datahub_router_test.* TO 'datahub'@'localhost';

-- Apply changes
FLUSH PRIVILEGES;
```

### 3. Verify MySQL Connection

You can test the connection manually:

```bash
mysql -h localhost -u datahub -p datahub_router_test
# Enter password: datahub
```

## Running MySQL Tests

### Run All MySQL Tests

```bash
pytest tests/test_db_mysql.py -v --run-mysql-tests
```

### Run Specific MySQL Tests

```bash
# Run a specific test
pytest tests/test_db_mysql.py::TestMySQLDatabase::test_create_instance_success -v --run-mysql-tests

# Run tests with coverage
pytest tests/test_db_mysql.py --cov=datahub.cloud.router.db.mysql --cov-report=term-missing --run-mysql-tests
```

## Test Configuration

The MySQL tests use the following configuration:

- **Host**: localhost
- **Port**: 3306
- **Database**: datahub_router_test
- **Username**: datahub
- **Password**: datahub
- **Charset**: utf8mb4

## Test Behavior

- **Default**: MySQL tests are **skipped by default** to avoid requiring a MySQL server for basic development
- **Manual Run**: Use the `--run-mysql-tests` flag to enable MySQL tests
- **Cleanup**: Tests automatically clean up their data after completion

## Troubleshooting

### Connection Issues

If you get connection errors:

1. **Verify MySQL is running**:

   ```bash
   sudo systemctl status mysql  # Linux
   brew services list | grep mysql  # macOS
   ```

2. **Check user permissions**:

   ```sql
   SHOW GRANTS FOR 'datahub'@'localhost';
   ```

3. **Test connection manually**:
   ```bash
   mysql -h localhost -u datahub -p datahub_router_test
   ```

### Database Already Exists

If the test database already exists, you can drop and recreate it:

```sql
DROP DATABASE IF EXISTS datahub_router_test;
CREATE DATABASE datahub_router_test;
```

### Port Issues

If MySQL is running on a different port, update the test configuration in `tests/test_db_mysql.py`:

```python
@pytest.fixture
def mysql_config(self):
    return {
        "host": "localhost",
        "port": 3307,  # Change this if needed
        "database": "datahub_router_test",
        "username": "datahub",
        "password": "datahub",
        "charset": "utf8mb4",
        "autocommit": True,
    }
```

## Test Coverage

The MySQL tests cover:

- Database initialization and schema creation
- CRUD operations for DataHub instances
- Tenant mapping operations
- Unknown tenant tracking
- Connection pool management
- Error handling and constraints
- Data persistence across connections

## Integration with CI/CD

For continuous integration, you can:

1. **Use Docker** to run MySQL in CI:

   ```yaml
   services:
     mysql:
       image: mysql:8.0
       environment:
         MYSQL_ROOT_PASSWORD: root
         MYSQL_DATABASE: datahub_router_test
         MYSQL_USER: datahub
         MYSQL_PASSWORD: datahub
   ```

2. **Skip MySQL tests** in CI if not needed:
   ```bash
   pytest --run-mysql-tests  # Only run if flag is provided
   ```

## Notes

- The MySQL tests are designed to be isolated and self-contained
- Each test cleans up after itself to avoid data pollution
- Tests use a separate database to avoid conflicts with other applications
- The test database is created and dropped as needed
