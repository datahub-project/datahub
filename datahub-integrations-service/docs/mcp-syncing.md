# MCP Server Syncing Guide

## Overview

The MCP server code is maintained in two repositories:

- **datahub-fork**: `datahub-integrations-service/src/datahub_integrations/mcp/`
- **mcp-server-datahub**: Standalone repository at `/Users/alex/work/mcp-server-datahub/`

**Key principle**: The entire `mcp/` folder (source code + tests) is fully syncable between repositories.

## What's Syncable

### ✅ Fully Syncable Folders

**Source Code**: `src/datahub_integrations/mcp/`

- `mcp_server.py` - Main MCP server implementation
- `mcp_telemetry.py` - Telemetry middleware
- `router.py` - HTTP routing setup
- `gql/*.gql` - GraphQL query definitions
- `__init__.py`

**Tests**: `tests/mcp/`

- All test files (`test_*.py`)
- `conftest.py` - Test configuration
- `__init__.py`

### ❌ Integration-Specific (NOT Synced)

**Source Code**: `src/datahub_integrations/mcp_integration/`

- `tool.py` - ToolWrapper, async_background utilities (datahub-fork specific)

**Tests**: `tests/mcp_integration/`

- `test_tool.py` - Tests for ToolWrapper
- `test_semantic_search.py` - Tests for cloud-only semantic search feature

## How to Sync

### From datahub-fork → mcp-server-datahub

When you've made improvements to the MCP server in datahub-fork:

```bash
# Option 1: Sync everything (recommended)
# Source code
cp -r /Users/alex/work/datahub-fork/datahub-integrations-service/src/datahub_integrations/mcp/* \
      /Users/alex/work/mcp-server-datahub/src/mcp_server_datahub/

# Tests
cp -r /Users/alex/work/datahub-fork/datahub-integrations-service/tests/mcp/* \
      /Users/alex/work/mcp-server-datahub/tests/

# Option 2: Sync individual files if needed
cp src/datahub_integrations/mcp/mcp_server.py \
   /Users/alex/work/mcp-server-datahub/src/mcp_server_datahub/mcp_server.py
```

### From mcp-server-datahub → datahub-fork

```bash
# Source code
cp -r /Users/alex/work/mcp-server-datahub/src/mcp_server_datahub/* \
      /Users/alex/work/datahub-fork/datahub-integrations-service/src/datahub_integrations/mcp/

# Tests
cp -r /Users/alex/work/mcp-server-datahub/tests/* \
      /Users/alex/work/datahub-fork/datahub-integrations-service/tests/mcp/
```

## Architecture

```
datahub_integrations/
├── mcp/                          # ✅ FULLY SYNCABLE
│   ├── mcp_server.py
│   ├── mcp_telemetry.py
│   ├── router.py
│   ├── gql/*.gql
│   └── __init__.py
│
└── mcp_integration/              # ❌ NOT SYNCED (integration-specific)
    ├── tool.py
    └── __init__.py

tests/
├── mcp/                          # ✅ FULLY SYNCABLE
│   ├── test_*.py
│   ├── conftest.py
│   └── __init__.py
│
└── mcp_integration/              # ❌ NOT SYNCED (integration-specific)
    ├── test_tool.py
    ├── test_semantic_search.py
    └── __init__.py
```

## Why This Works

1. **No Dependencies**: The `mcp/` folder has zero dependencies on `mcp_integration/`
2. **Clean Separation**: Integration-specific code lives in `mcp_integration/`
3. **Future-Proof**: Supports breaking `mcp_server.py` into smaller files (all within `mcp/`)

## Future Evolution

This structure enables refactoring `mcp_server.py` into smaller files:

```
mcp/
├── mcp_server.py          # Can be split into:
├── tools/                 # Tool implementations
├── lineage/               # Lineage logic
├── search/                # Search logic
└── gql/                   # GraphQL queries
```

All of these would remain fully syncable!

## Notes

### About `copy_mcp_server.py`

The script `scripts/copy_mcp_server.py` is now **obsolete**. It was designed to copy only selected files, but now you can simply sync the entire `mcp/` folder.

### About Semantic Search

Semantic search is a **DataHub Cloud-only feature** not available in open source. Tests for this feature are kept in `tests/mcp_integration/test_semantic_search.py` and are not synced to mcp-server-datahub.

### About Dependencies

The main dependency to watch is `TokenCountEstimator` from `datahub_integrations.chat.context_reducer`, which is imported by `mcp_server.py`. When syncing to mcp-server-datahub, ensure this dependency is handled appropriately.

## Testing After Sync

```bash
# In datahub-fork
cd /Users/alex/work/datahub-fork/datahub-integrations-service
uv run pytest tests/mcp/ -v              # Test MCP server code
uv run pytest tests/mcp_integration/ -v  # Test integration utilities

# In mcp-server-datahub
cd /Users/alex/work/mcp-server-datahub
uv run pytest -v
```
