# DataHub Authentication E2E Tests

This document provides instructions for running the authentication end-to-end tests that validate current DataHub authentication behavior before implementing the two-filter authentication approach.

## Purpose

The `test_authentication_e2e.py` test suite captures the **current authentication behavior** and serves as a comprehensive regression test to ensure backward compatibility when implementing authentication refactoring.

## Quick Start

### Prerequisites

1. **DataHub must be running locally**

   ```bash
   # From project root
   ./gradlew quickstartDebug
   ```

2. **Set up Python environment** (if not already done)

   ```bash
   cd smoke-test
   python3 -m venv venv
   source venv/bin/activate
   pip install --upgrade pip wheel setuptools
   pip install -r requirements.txt
   ```

### Environment Variables

```bash
export DATAHUB_VERSION=v1.0.0rc3-SNAPSHOT  # or current version
export TEST_STRATEGY=no_cypress_suite1     # for authentication tests
```

### Running Authentication Tests

```bash
cd smoke-test
source venv/bin/activate

# Set environment variables
export DATAHUB_VERSION=v1.0.0rc3-SNAPSHOT
export TEST_STRATEGY=no_cypress_suite1

# Run all authentication tests
pytest test_authentication_e2e.py -vv

# Run with detailed logging to see authentication behavior
pytest test_authentication_e2e.py -vv -s --log-cli-level=INFO

# Run specific test categories
pytest test_authentication_e2e.py -k "public" -vv       # Public endpoint tests
pytest test_authentication_e2e.py -k "protected" -vv    # Protected endpoint tests
pytest test_authentication_e2e.py -k "token" -vv        # Token authentication tests
pytest test_authentication_e2e.py -k "admin" -vv        # Authorization tests

# Run specific test methods
pytest test_authentication_e2e.py::test_health_endpoint_no_auth -vv
pytest test_authentication_e2e.py::test_graphql_endpoint_no_auth -vv
```

## Test Categories

### 🔓 Public Endpoints Tests (Fast)

- Tests excluded paths that should work without authentication
- `/health`, `/config`, `/actuator/prometheus`, etc.
- Should never return 401 Unauthorized

### 🔒 Protected Endpoints Tests (Fast)

- Tests endpoints that require authentication
- GraphQL API, Rest.li API, OpenAPI endpoints
- Should return 401 without valid authentication

### 🎫 Token Authentication Tests (Medium)

- Tests API token generation and usage
- Session cookie vs Bearer token authentication
- Token validation and cleanup

### 👑 Authorization Tests (Slow)

- Tests privilege-based access control
- Admin endpoints requiring special permissions
- Creates/cleans up test users with limited privileges

### 🧪 Edge Cases & Behavior Tests (Fast)

- Malformed authentication headers
- Authentication priority and fallback behavior
- Cross-service authentication consistency

## Expected Results

### ✅ Successful Test Run

```
============== Authentication Test Results ==============
✅ Public endpoints working: 3/3
✅ Protected endpoints secured: 2/2
✅ Authentication methods working: 1/1

test_authentication_e2e.py::test_health_endpoint_no_auth PASSED
test_authentication_e2e.py::test_excluded_paths_no_auth[/config] PASSED
test_authentication_e2e.py::test_graphql_endpoint_no_auth PASSED
test_authentication_e2e.py::test_protected_endpoints_no_auth[/openapi/v1/system-info-GET] PASSED
...
✅ Authentication behavior validation complete - ready for two-filter refactoring
```

### 📊 What Each Test Validates

| Test                                      | Current Behavior                          | Purpose                                         |
| ----------------------------------------- | ----------------------------------------- | ----------------------------------------------- |
| `test_health_endpoint_no_auth`            | `/health` returns 200 without auth        | Validates health checks work for load balancers |
| `test_excluded_paths_no_auth`             | Excluded paths don't require auth         | Documents current public endpoints              |
| `test_graphql_endpoint_no_auth`           | GraphQL returns 401 without auth          | Ensures API security                            |
| `test_protected_endpoints_no_auth`        | Protected APIs return 401                 | Validates authentication is enforced            |
| `test_admin_endpoints_require_privileges` | Admin APIs return 403 for non-admin users | Tests authorization logic                       |
| `test_api_token_authentication`           | Bearer tokens work for API access         | Validates token-based auth                      |

## Troubleshooting

### ❌ Common Issues

**Connection refused errors**

```bash
# Check if DataHub is running
curl -s http://localhost:8080/health
curl -s http://localhost:9002/api/v2/graphql
```

**401 Unauthorized for excluded paths**

```bash
# This indicates excluded paths configuration may have changed
# Check current excluded paths in application.yaml:
# authentication.excludedPaths: /health,/config,/actuator/prometheus
```

**403 Forbidden instead of expected 401**

```bash
# This might indicate authentication is working but authorization is failing
# Check the test logs to understand the specific behavior
```

**Test user creation failures**

```bash
# Admin endpoints might not be accessible
# Ensure DataHub is fully started and admin user exists
```

### 🔍 Debug Commands

```bash
# Check DataHub services status
curl -s http://localhost:8080/health | head -5
curl -s http://localhost:9002/health | head -5

# Test public endpoints directly
curl -s http://localhost:8080/config | head -10
curl -s http://localhost:8080/actuator/prometheus | head -10

# Test protected endpoints (should return 401)
curl -s http://localhost:8080/openapi/v1/system-info
curl -X POST http://localhost:9002/api/v2/graphql -d '{"query":"{ me { corpUser { username } } }"}'

# Check authentication configuration
curl -s http://localhost:8080/actuator/configprops | grep -i auth
```

## Integration with Two-Filter Development

### Before Implementing Two-Filter Approach

1. **Run baseline tests** to capture current behavior:

   ```bash
   pytest test_authentication_e2e.py -vv > baseline_results.txt
   ```

2. **Verify all tests pass** - this becomes your regression test suite

### After Implementing Two-Filter Approach

1. **Run same tests** to ensure backward compatibility:

   ```bash
   pytest test_authentication_e2e.py -vv > after_refactor_results.txt
   ```

2. **Compare results** - behavior should be identical:

   ```bash
   diff baseline_results.txt after_refactor_results.txt
   ```

3. **Add new tests** for progressive disclosure features:
   ```bash
   # Add tests for endpoints that now work with optional authentication
   pytest test_authentication_e2e.py::test_config_progressive_disclosure -vv
   ```

## Performance

- **Fast tests** (public/protected endpoints): ~30 seconds
- **Token tests** (requires API token generation): ~1-2 minutes
- **Admin tests** (creates/deletes users): ~2-3 minutes
- **Full suite**: ~3-5 minutes

## Next Steps

After running these tests successfully:

1. ✅ Document current authentication behavior
2. ✅ Use as regression test suite
3. ⚠️ Implement two-filter authentication approach
4. ✅ Re-run tests to ensure backward compatibility
5. ➕ Add tests for new optional authentication features

---

💡 **Pro Tip**: Run `pytest test_authentication_e2e.py::test_authentication_behavior_summary -vv -s` first to get a quick overview of your DataHub authentication setup.
