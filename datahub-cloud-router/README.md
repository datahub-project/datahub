# DataHub Multi-Tenant Router

A production-ready multi-tenant router for DataHub that routes webhook events to the correct DataHub instances based on tenant and team mappings. This router acts as a central hub for platform integrations, supporting Microsoft Teams integration with extensibility for other platforms.

## Features

- **Multi-tenant Routing**: Route webhook events to different DataHub instances based on tenant ID and team mappings
- **OAuth Callback Handling**: Support for both personal notifications and platform integration flows
- **Admin API**: RESTful API for managing DataHub instances and tenant mappings
- **Database Abstraction**: Support for SQLite, MySQL, and in-memory databases with extensible architecture
- **Health Monitoring**: Built-in health checks and monitoring endpoints
- **Event Logging**: Comprehensive event logging for debugging and audit trails
- **Platform Extensibility**: Designed to support Microsoft Teams and other platforms

## Installation

### As Part of DataHub Development

#### Using Gradle (Recommended)

The datahub-cloud-router is integrated into the DataHub build system:

```bash
# From the DataHub repository root, navigate to the router directory
cd datahub-cloud-router

# Install dependencies (using uv for lockfile-based installs)
../gradlew :datahub-cloud-router:installDev

# Install package in editable mode (required for CLI)
source venv/bin/activate
pip install -e .

# Run tests
../gradlew :datahub-cloud-router:test

# Format code
../gradlew :datahub-cloud-router:lintFix
```

#### Direct Installation

```bash
# From the datahub-cloud-router directory
cd datahub-cloud-router

# Setup with uv (recommended)
uv venv --python 3.11 venv
source venv/bin/activate
./scripts/sync.sh

# Install package in editable mode
pip install -e .
```

### From PyPI

```bash
pip install datahub-cloud-router
```

## Quick Start

### Run the Router

```bash
# Using the console script (after pip install -e .)
datahub-router

# Or directly with Python module
PYTHONPATH=src python -m datahub.cloud.router

# For development with auto-reload
PYTHONPATH=src python -m datahub.cloud.router --reload

# Alternative: using uvicorn directly for reload (development)
PYTHONPATH=src uvicorn datahub.cloud.router.app:app --host 0.0.0.0 --port 9005 --reload
```

### Default Configuration

The router starts with:

- **Port**: 9005
- **Default Target**: http://localhost:9003
- **Database**: SQLite (.dev/router.db)

### Access Points

- **Webhook URL**: http://localhost:9005/public/teams/webhook
- **Bot Framework URL**: http://localhost:9005/api/messages
- **Health Check**: http://localhost:9005/health
- **Admin API**: http://localhost:9005/admin/
- **Events Log**: http://localhost:9005/events

## Usage

### As a Python Package

```python
from datahub.cloud.router import MultiTenantRouter, DataHubMultiTenantRouter

# Create router instance
router = MultiTenantRouter()

# Create and run server
server = DataHubMultiTenantRouter(router)
server.run_server(host="0.0.0.0", port=9005)
```

### Admin API Examples

#### List DataHub Instances

```bash
curl http://localhost:9005/admin/instances
```

#### Create a DataHub Instance

```bash
curl -X POST http://localhost:9005/admin/instances \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Production DataHub",
    "url": "https://datahub.company.com",
    "deployment_type": "cloud"
  }'
```

#### Create Tenant Mapping

```bash
curl -X POST http://localhost:9005/admin/tenants/tenant-123/mapping \
  -H "Content-Type: application/json" \
  -d '{
    "instance_id": "prod-instance",
    "team_id": "team-456"
  }'
```

## Configuration

### Environment Variables

- `DATAHUB_ROUTER_TARGET_URL`: Default DataHub instance URL
- `DATAHUB_ROUTER_DB_TYPE`: Database type (sqlite, mysql, inmemory)
- `DATAHUB_ROUTER_DB_PATH`: Database file path (for SQLite)
- `DATAHUB_ROUTER_DB_HOST`: Database host (for MySQL)
- `DATAHUB_ROUTER_DB_PORT`: Database port (for MySQL)
- `DATAHUB_ROUTER_DB_NAME`: Database name (for MySQL)
- `DATAHUB_ROUTER_DB_USER`: Database username (for MySQL)
- `DATAHUB_ROUTER_DB_PASSWORD`: Database password (for MySQL)

### Database Types

#### SQLite (Default)

```python
from datahub.cloud.router.db import DatabaseFactory

config = {
    "type": "sqlite",
    "path": ".dev/router.db"
}
db = DatabaseFactory.get_database(config)
```

#### MySQL (Production)

```python
from datahub.cloud.router.db import DatabaseFactory

config = {
    "type": "mysql",
    "host": "localhost",
    "port": 3306,
    "database": "datahub_router",
    "username": "router_user",
    "password": "secure_password"
}
db = DatabaseFactory.get_database(config)
```

### MySQL Setup

For production deployments, MySQL is recommended for better performance and scalability:

#### 1. Install MySQL Dependencies

MySQL support is included by default with `aiomysql>=0.1.1`. For production usage, you may also want:

```bash
# Additional MySQL driver (optional)
pip install PyMySQL>=1.0.0
```

#### 2. Create MySQL Database

```sql
CREATE DATABASE datahub_router CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'router_user'@'localhost' IDENTIFIED BY 'secure_password';
GRANT ALL PRIVILEGES ON datahub_router.* TO 'router_user'@'localhost';
FLUSH PRIVILEGES;
```

#### 3. Configure Router for MySQL

```python
from datahub.cloud.router import MultiTenantRouter, DataHubMultiTenantRouter

# Create router with MySQL configuration
router = MultiTenantRouter(
    db_config={
        "type": "mysql",
        "host": "localhost",
        "port": 3306,
        "database": "datahub_router",
        "username": "router_user",
        "password": "secure_password",
        "max_connections": 10
    }
)

# Create and run server
server = DataHubMultiTenantRouter(router)
server.run_server(host="0.0.0.0", port=9005)
```

#### 4. Environment Variables for MySQL

```bash
export DATAHUB_ROUTER_DB_TYPE=mysql
export DATAHUB_ROUTER_DB_HOST=localhost
export DATAHUB_ROUTER_DB_PORT=3306
export DATAHUB_ROUTER_DB_NAME=datahub_router
export DATAHUB_ROUTER_DB_USER=router_user
export DATAHUB_ROUTER_DB_PASSWORD=secure_password
```

### MySQL Testing

For running MySQL-specific tests, you need to set up a test database:

#### 1. Create Test Database

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

#### 2. Run MySQL Tests

```bash
# Run all MySQL tests
pytest tests/test_db_mysql.py -v --run-mysql-tests

# Run specific MySQL test
pytest tests/test_db_mysql.py::TestMySQLDatabase::test_create_instance_success -v --run-mysql-tests

# Run with coverage
pytest tests/test_db_mysql.py --cov=datahub.cloud.router.db.mysql --cov-report=term-missing --run-mysql-tests
```

**Note**: MySQL tests are skipped by default. Use the `--run-mysql-tests` flag to enable them.

#### 3. Test Configuration

The MySQL tests use these default settings:

- **Host**: localhost
- **Port**: 3306
- **Database**: datahub_router_test
- **Username**: datahub
- **Password**: datahub

For detailed MySQL testing instructions, see [tests/README_MYSQL_TESTS.md](tests/README_MYSQL_TESTS.md).

#### In-Memory (Testing)

The in-memory database maintains the same data model without persistence:

```python
from datahub.cloud.router.db import DatabaseFactory

config = {
    "type": "inmemory"
}
db = DatabaseFactory.get_database(config)
```

### Schema Migrations

- **SQLite**: Automatic schema creation on first run via `CREATE TABLE IF NOT EXISTS`
- **MySQL**: Schema initialization with proper foreign key constraints and indexes
- **Future migrations**: Schema versioning can be added to `datahub_instances` table

## Code Walkthrough

### Core Components

The DataHub Multi-Tenant Router is built with a modular architecture that separates concerns and enables extensibility. Here's a detailed walkthrough of the key components:

#### 1. MultiTenantRouter (core.py)

The core routing engine that handles event routing logic:

```python
class MultiTenantRouter:
    def __init__(self, db_path: str = ".dev/router.db", default_target_url: str = "http://localhost:9003"):
        # Initialize database abstraction layer
        db_config = DatabaseConfig(type="sqlite", config={"path": db_path})
        db_factory = DatabaseFactory(db_config)
        self.db = db_factory.get_database()

        # Ensure default instance exists
        self._ensure_default_instance()

    async def route_event(self, event: Dict[str, Any], tenant_id: str, team_id: Optional[str] = None):
        """Route an event to the appropriate DataHub instance based on tenant/team mapping."""

        # Strategy 1: Team-specific routing (for large organizations)
        if team_id:
            instance = await self._get_instance_by_tenant_and_team(tenant_id, team_id)
            if instance:
                return await self._forward_to_instance(instance, event)

        # Strategy 2: Tenant-only routing (most common)
        instance = await self._get_instance_by_tenant(tenant_id)
        if instance:
            return await self._forward_to_instance(instance, event)

        # Strategy 3: No fallback - reject unknown tenants
        await self._create_unknown_tenant_alert(tenant_id, team_id)
        raise UnknownTenantError(f"No routing found for tenant {tenant_id}")
```

**Key Features:**

- **Multi-strategy routing**: Team-specific → Tenant-specific → Reject unknown
- **Database abstraction**: Uses the database interface for persistence
- **Error handling**: Creates unknown tenant alerts for admin review
- **Async support**: Full async/await for high performance

#### 2. Database Abstraction Layer (db/)

The database layer provides a clean interface for different storage backends:

```python
# Interface (db/interface.py)
class Database(ABC):
    @abstractmethod
    def create_instance(self, instance: DataHubInstance) -> bool:
        pass

    @abstractmethod
    def get_instance_by_tenant(self, tenant_id: str) -> Optional[DataHubInstance]:
        pass

    @abstractmethod
    def create_mapping(self, mapping: TenantMapping) -> bool:
        pass

# Factory (db/interface.py)
class DatabaseFactory:
    @staticmethod
    def get_database(config: DatabaseConfig) -> Database:
        if config.type == "sqlite":
            return SQLiteDatabase(config.config)
        elif config.type == "mysql":
            return MySQLDatabase(config.config)
        elif config.type == "inmemory":
            return InMemoryDatabase()
        else:
            raise ValueError(f"Unsupported database type: {config.type}")
```

**Supported Database Backends:**

- **SQLite**: File-based storage with same schema design, good for development and small deployments
- **MySQL**: Production-ready with connection pooling, optimized for high concurrency
- **In-Memory**: Fast testing backend that maintains the same data model in memory

#### 3. FastAPI Server (server.py)

The web server that handles HTTP requests and integrates with the router:

```python
class DataHubMultiTenantRouter:
    def __init__(self, router: MultiTenantRouter):
        self.router = router
        self.app = FastAPI(
            title="DataHub Multi-Tenant Router",
            description="Multi-tenant platform integration router for DataHub",
            version="0.1.0"
        )
        self._setup_routes()
        self._setup_admin_routes()

    def _setup_routes(self):
        @self.app.post("/public/teams/webhook")
        async def teams_webhook(request: Request):
            """Handle Teams webhook events."""
            body = await request.json()
            tenant_id = body.get("tenantId") or body.get("tenant_id")
            team_id = self._extract_team_id(body)

            # Route the event through the core router
            result = await self.router.route_event(body, tenant_id, team_id)
            return JSONResponse(content=result)

        @self.app.get("/public/teams/oauth/callback")
        async def oauth_callback(request: Request):
            """Handle OAuth callback and auto-register tenant mappings."""
            # Extract OAuth parameters
            query_params = dict(request.query_params)
            auth_code = query_params.get("code")
            state = query_params.get("state")

            # Decode state to get target instance info
            decoded_state = self.decode_oauth_state(state)
            target_url = decoded_state.get("url")

            # Forward to DataHub integration service
            # Auto-register tenant mapping on success
            return await self._handle_oauth_flow(auth_code, decoded_state)
```

**Key Endpoints:**

- **`/public/teams/webhook`**: Receives Teams events and routes them
- **`/public/teams/oauth/callback`**: Handles OAuth callbacks with auto-registration
- **`/admin/*`**: RESTful API for managing instances and mappings
- **`/health`**: Health monitoring and statistics

#### 4. Data Models (db/models.py)

Clean data structures using Pydantic and dataclasses:

```python
@dataclass
class DataHubInstance:
    id: str
    name: str
    deployment_type: DeploymentType
    url: Optional[str] = None
    connection_id: Optional[str] = None
    api_token: Optional[str] = None
    health_check_url: Optional[str] = None
    is_active: bool = True
    is_default: bool = False
    max_concurrent_requests: int = 100
    timeout_seconds: int = 30
    health_status: HealthStatus = HealthStatus.UNKNOWN
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    last_health_check: Optional[str] = None

@dataclass
class TenantMapping:
    id: str
    tenant_id: str
    datahub_instance_id: str
    team_id: Optional[str] = None
    team_name: Optional[str] = None
    is_default_for_tenant: bool = False
    is_active: bool = True
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    created_by: Optional[str] = None
```

#### 5. OAuth Flow Integration

The router handles OAuth callbacks and automatically creates tenant mappings:

```python
async def auto_register_tenant_after_oauth(self, datahub_url: str, instance_id: str, tenant_id: str):
    """Auto-register tenant mapping after successful OAuth."""

    # Check if instance exists, create if needed
    instance = self.router.db.get_instance(instance_id)
    if not instance:
        instance = self.router.db.create_instance(
            name=f"DataHub Instance ({datahub_url})",
            url=datahub_url,
            deployment_type=DeploymentType.CLOUD,
            is_active=True,
            is_default=False
        )
        instance_id = instance.id

    # Create tenant mapping
    mapping = self.router.db.create_mapping(
        tenant_id=tenant_id,
        instance_id=instance_id,
        created_by="oauth_auto_registration"
    )

    # Remove from unknown tenants if it exists
    self.router.db.remove_unknown_tenant(tenant_id)
```

### Request Flow Example

Here's how a Teams webhook event flows through the system:

1. **Teams sends webhook** → `POST /public/teams/webhook`
2. **Server extracts tenant/team info** from the event payload
3. **Router looks up mapping** using tenant ID and team ID
4. **Instance found** → Forward event to target DataHub instance
5. **Instance not found** → Create unknown tenant alert, reject event
6. **Response returned** to Teams with appropriate status

### OAuth Flow Example

1. **User initiates OAuth** in DataHub settings
2. **DataHub redirects** to Microsoft with router callback URL
3. **Microsoft redirects back** → `GET /public/teams/oauth/callback`
4. **Router decodes state** to get target instance info
5. **Router forwards OAuth** to DataHub integration service
6. **On success** → Auto-create tenant mapping
7. **Redirect user** back to DataHub with success status

## Database Schema

The router uses a relational database design with three core tables optimized for multi-tenant routing performance.

### Schema Overview

#### 1. DataHub Instances Table (`datahub_instances`)

Stores registered DataHub instances that can receive routed events:

```sql
CREATE TABLE datahub_instances (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    deployment_type ENUM('cloud', 'on_premise', 'hybrid') NOT NULL,
    url TEXT,
    connection_id VARCHAR(255),
    api_token TEXT,
    health_check_url TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    is_default BOOLEAN DEFAULT FALSE,
    max_concurrent_requests INT DEFAULT 100,
    timeout_seconds INT DEFAULT 30,
    health_status ENUM('healthy', 'unhealthy', 'unknown', 'degraded') DEFAULT 'unknown',
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    last_health_check DATETIME
);
```

**Key Features:**

- UUID primary keys for global uniqueness
- Support for cloud, on-premise, and hybrid deployments
- Built-in health monitoring and connection limits
- Soft deletion via `is_active` flag
- Single default instance support

#### 2. Tenant Instance Mappings Table (`tenant_instance_mappings`)

Maps Microsoft Teams tenants (and optionally teams) to specific DataHub instances:

```sql
CREATE TABLE tenant_instance_mappings (
    id VARCHAR(255) PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    datahub_instance_id VARCHAR(255) NOT NULL,
    team_id VARCHAR(255),
    team_name VARCHAR(255),
    is_default_for_tenant BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    created_by VARCHAR(255) DEFAULT 'system',
    FOREIGN KEY (datahub_instance_id) REFERENCES datahub_instances(id),
    UNIQUE KEY unique_tenant_team (tenant_id, team_id)
);
```

**Key Features:**

- Support for both tenant-level and team-level routing
- Automatic creation via OAuth success flows (`created_by = 'oauth_auto_registration'`)
- Composite unique constraint prevents duplicate mappings
- Cascading deletes when instances are removed

#### 3. Unknown Tenants Table (`unknown_tenants`)

Tracks unrouted webhook events for administrative review:

```sql
CREATE TABLE unknown_tenants (
    id VARCHAR(255) PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    team_id VARCHAR(255),
    team_name VARCHAR(255),
    first_seen DATETIME NOT NULL,
    last_seen DATETIME NOT NULL,
    event_count INT DEFAULT 1,
    status ENUM('pending_review', 'assigned', 'ignored') DEFAULT 'pending_review',
    assigned_instance_id VARCHAR(255),
    FOREIGN KEY (assigned_instance_id) REFERENCES datahub_instances(id) ON DELETE SET NULL,
    UNIQUE KEY unique_tenant_team (tenant_id, team_id)
);
```

**Key Features:**

- Automatic event counting and timestamp tracking
- Admin workflow support (pending → assigned → resolved)
- Prevents spam by updating existing records rather than creating duplicates
- Optional assignment to DataHub instances for batch resolution

### Database Indexes

Optimized for multi-tenant routing performance:

```sql
-- Tenant routing lookups
CREATE INDEX idx_tenant_mappings_tenant_id ON tenant_instance_mappings(tenant_id);
CREATE INDEX idx_tenant_mappings_instance_id ON tenant_instance_mappings(datahub_instance_id);

-- Instance filtering
CREATE INDEX idx_instances_deployment_type ON datahub_instances(deployment_type);
CREATE INDEX idx_instances_is_active ON datahub_instances(is_active);
CREATE INDEX idx_instances_is_default ON datahub_instances(is_default);

-- Admin workflow
CREATE INDEX idx_unknown_tenants_status ON unknown_tenants(status);
```

### Routing Logic

The schema supports a hierarchical routing strategy:

1. **Team-Specific Routing**: `SELECT * FROM tenant_instance_mappings WHERE tenant_id = ? AND team_id = ?`
2. **Tenant-Only Routing**: `SELECT * FROM tenant_instance_mappings WHERE tenant_id = ? AND team_id IS NULL`
3. **Default Instance Fallback**: `SELECT * FROM datahub_instances WHERE is_default = TRUE AND is_active = TRUE`
4. **Unknown Tenant Tracking**: `INSERT INTO unknown_tenants (tenant_id, team_id, first_seen, last_seen, event_count)`

### Data Model Classes

Python dataclasses provide type-safe database interactions:

```python
@dataclass
class DataHubInstance:
    id: str
    name: str
    deployment_type: DeploymentType  # Enum: cloud, on_premise, hybrid
    url: str
    health_status: HealthStatus      # Enum: healthy, unhealthy, unknown, degraded
    is_active: bool = True
    is_default: bool = False
    # ... additional configuration fields

@dataclass
class TenantMapping:
    id: str
    tenant_id: str              # Microsoft Teams tenant ID
    instance_id: str            # Foreign key to datahub_instances
    team_id: Optional[str]      # Optional Microsoft Teams team ID
    created_by: str = "system"  # Tracks creation source

@dataclass
class UnknownTenant:
    id: str
    tenant_id: str
    team_id: Optional[str]
    event_count: int = 1
    # ... timestamp and status fields
```

## Architecture

### Package Structure

```
datahub/
└── cloud/
    └── router/
        ├── __init__.py          # Main package exports
        ├── cli.py              # Command-line interface
        ├── core.py             # Core router logic
        ├── server.py           # FastAPI server
        ├── db/
        │   ├── __init__.py     # Database interface
        │   ├── interface.py    # Abstract database interface
        │   ├── sqlite.py       # SQLite implementation
        │   ├── mysql.py        # MySQL implementation
        │   └── inmemory.py     # In-memory implementation
        └── models.py           # Data models
```

### Key Components

- **MultiTenantRouter**: Core routing logic and tenant management
- **DataHubMultiTenantRouter**: FastAPI server with webhook endpoints
- **Database Interface**: Abstract database layer for extensibility
- **Admin API**: RESTful API for configuration management

## Development

### Setup Development Environment

#### Using Gradle (Recommended)

```bash
# From DataHub repository root, navigate to router directory
cd datahub-cloud-router
../gradlew :datahub-cloud-router:installDev

# Install package in editable mode (required for CLI and imports)
source venv/bin/activate
pip install -e .
```

#### Direct Setup

```bash
# Create virtual environment with uv
uv venv --python 3.11 venv
source venv/bin/activate

# Install dependencies from lockfiles
./scripts/sync.sh

# Install package in editable mode
pip install -e .
```

### Build System Integration

This module is integrated into the DataHub Gradle build system:

- **Module Path**: `:datahub-cloud-router`
- **Build Tool**: Gradle with uv and ruff
- **Dependencies**: Lockfile-based with `uv pip sync`
- **Package Install**: Manual editable install required (`pip install -e .`)
- **Python Version**: 3.11+
- **Line Length**: 88 characters (matches parent module)

### Available Gradle Tasks

```bash
# Build the module
./gradlew :datahub-cloud-router:build

# Install dependencies
./gradlew :datahub-cloud-router:install
./gradlew :datahub-cloud-router:installDev

# Code quality
./gradlew :datahub-cloud-router:lint
./gradlew :datahub-cloud-router:lintFix

# Testing
./gradlew :datahub-cloud-router:test
```

### Run Tests

#### Using Gradle

```bash
./gradlew :datahub-cloud-router:test
```

#### Direct pytest

```bash
pytest
```

### Code Formatting

#### Using Gradle

```bash
# Check formatting
./gradlew :datahub-cloud-router:lint

# Fix formatting
./gradlew :datahub-cloud-router:lintFix
```

#### Direct ruff

```bash
# Check and fix issues
ruff check --fix src/datahub/ tests/

# Format code
ruff format src/datahub/ tests/

# Type checking
PYTHONPATH=src mypy src/datahub/ tests/
```

### Code Quality Standards

- **Linting**: ruff (replaces flake8)
- **Formatting**: ruff format (replaces black)
- **Type Checking**: mypy
- **Line Length**: 88 characters
- **Target Python**: 3.8+ (built with 3.11)
- **Import Sorting**: Automatic via ruff

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

Apache License 2.0 - see LICENSE file for details.
