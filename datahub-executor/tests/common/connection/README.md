# Database Connection Lifecycle Management Tests

This directory contains comprehensive tests for the database connection lifecycle management and memory leak prevention fixes implemented in the datahub-executor.

## Test Files

### Connection Lifecycle Tests

- `test_snowflake_connection.py` - Tests for SnowflakeConnection lifecycle management
- `test_redshift_connection.py` - Tests for RedshiftConnection lifecycle management
- `test_databricks_connection.py` - Tests for DatabricksConnection lifecycle management
- `test_bigquery_connection.py` - Tests for BigQueryConnection lifecycle management

### Cursor Management Tests

- `../source/test_cursor_management.py` - Tests for cursor context manager usage across all database sources

## What These Tests Cover

### 🔄 **Connection Lifecycle Management**

- ✅ Proper connection creation and reuse
- ✅ Connection health checking and reconnection
- ✅ Explicit connection cleanup via `close()` method
- ✅ Context manager support (`with connection:`)
- ✅ Automatic cleanup via `weakref.finalize()`
- ✅ Exception handling during connection operations
- ✅ Finalizer updating when reconnecting

### 🎯 **Memory Leak Prevention**

- ✅ Cursor context manager usage (`with cursor:`)
- ✅ Cursor cleanup even when exceptions occur
- ✅ Connection cleanup when objects are garbage collected
- ✅ Safe rollback handling (Redshift)
- ✅ Resource cleanup in all error scenarios

### 🧪 **Test Coverage**

#### **SnowflakeConnection Tests**

- Connection creation and basic properties
- Connection reuse and reconnection logic
- Connection health checking via `is_closed()`
- Explicit close and context manager usage
- Finalizer cleanup and garbage collection
- Exception handling in connection creation and cleanup

#### **RedshiftConnection Tests**

- Connection creation with all configuration options
- Connection reuse and reconnection logic
- Connection health checking via `closed` property
- Password handling (including None password)
- Extra client options support
- Rollback handling in cursor operations

#### **DatabricksConnection Tests**

- Connection creation and workspace URL parsing
- Connection reuse and reconnection logic
- Connection health checking via `_closed` attribute
- Server hostname extraction from workspace URL
- Exception handling with SourceConnectionErrorException

#### **BigQueryConnection Tests**

- Connection creation and client management
- Connection reuse (BigQuery client doesn't need health checking)
- Lightweight lifecycle management
- Exception handling in client creation

#### **Cursor Management Tests**

- Context manager usage in all `_execute_fetchall_query` methods
- Context manager usage in all `_execute_fetchone_query` methods
- Cursor cleanup when exceptions occur
- Integration tests across all database sources
- Rollback handling in Redshift with proper cursor cleanup

## Running the Tests

### With pytest (recommended):

```bash
# Run all connection tests
pytest tests/common/connection/ -v

# Run specific connection tests
pytest tests/common/connection/test_snowflake_connection.py -v
pytest tests/common/connection/test_redshift_connection.py -v
pytest tests/common/connection/test_databricks_connection.py -v
pytest tests/common/connection/test_bigquery_connection.py -v

# Run cursor management tests
pytest tests/common/source/test_cursor_management.py -v
```

## Memory Leak Issues Addressed

### 🚨 **Critical Issues Fixed:**

1. **Cursor Memory Leaks** - Cursors were created but never closed, especially in error conditions
2. **Connection Reuse Without Lifecycle Management** - Connections were reused indefinitely without health checking
3. **No Cleanup on Object Destruction** - No automatic cleanup when connection objects were garbage collected
4. **Exception Safety** - Resources weren't cleaned up when exceptions occurred

### ✅ **Solutions Implemented:**

1. **Context Managers for Cursors** - All cursor operations now use `with cursor:` pattern
2. **Connection Health Checking** - Connections are checked for health and reconnected if needed
3. **Automatic Cleanup** - `weakref.finalize()` ensures cleanup when objects are garbage collected
4. **Exception Safety** - Resources are cleaned up in all code paths, including exceptions
5. **Context Manager Support** - All connections support `with connection:` usage

## Test Quality Features

- **Comprehensive Mocking** - All external dependencies are properly mocked
- **Exception Testing** - Tests verify proper behavior when exceptions occur
- **Memory Management Testing** - Tests verify garbage collection and finalizer behavior
- **Integration Testing** - Tests verify that all sources consistently use context managers
- **Edge Case Coverage** - Tests cover None passwords, missing attributes, cleanup failures, etc.

These tests ensure that the memory leak fixes are working correctly and will prevent regressions in the future.
