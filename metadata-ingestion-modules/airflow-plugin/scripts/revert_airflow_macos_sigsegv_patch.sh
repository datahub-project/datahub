#!/bin/bash
# Revert script for Airflow 3.0 SIGSEGV patches on macOS
# This script restores the original Airflow files from backups
#
# Usage: ./scripts/revert_airflow_macos_sigsegv_patch.sh [tox_env_path]
#
# If tox_env_path is not provided, it defaults to .tox/py311-airflow302

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default tox environment path
TOX_ENV_PATH="${1:-.tox/py311-airflow302}"

echo -e "${YELLOW}Airflow 3.0 macOS SIGSEGV Patch Revert Script${NC}"
echo "==============================================="
echo ""

# Check if tox environment exists
if [ ! -d "$TOX_ENV_PATH" ]; then
    echo -e "${RED}Error: Tox environment not found at $TOX_ENV_PATH${NC}"
    exit 1
fi

SITE_PACKAGES="$TOX_ENV_PATH/lib/python3.11/site-packages"

if [ ! -d "$SITE_PACKAGES" ]; then
    echo -e "${RED}Error: Site-packages directory not found at $SITE_PACKAGES${NC}"
    exit 1
fi

echo "Using tox environment: $TOX_ENV_PATH"
echo ""

REVERTED=0
ERRORS=0

# Revert LocalExecutor
echo -e "${YELLOW}Checking LocalExecutor...${NC}"

LOCAL_EXECUTOR="$SITE_PACKAGES/airflow/executors/local_executor.py"
LOCAL_EXECUTOR_BAK="$LOCAL_EXECUTOR.bak"

if [ -f "$LOCAL_EXECUTOR_BAK" ]; then
    echo "Restoring LocalExecutor from backup..."
    mv "$LOCAL_EXECUTOR_BAK" "$LOCAL_EXECUTOR"
    echo -e "${GREEN}✓ LocalExecutor restored${NC}"
    REVERTED=$((REVERTED + 1))
else
    if grep -q "# Patch for macOS: setproctitle causes SIGSEGV" "$LOCAL_EXECUTOR" 2>/dev/null; then
        echo -e "${YELLOW}! LocalExecutor is patched but no backup found${NC}"
        echo "  You may need to reinstall Airflow or recreate the tox environment"
        ERRORS=$((ERRORS + 1))
    else
        echo -e "${GREEN}✓ LocalExecutor was not patched${NC}"
    fi
fi
echo ""

# Revert API Server Command
echo -e "${YELLOW}Checking API Server Command...${NC}"

API_SERVER_CMD="$SITE_PACKAGES/airflow/cli/commands/api_server_command.py"
API_SERVER_CMD_BAK="$API_SERVER_CMD.bak"

if [ -f "$API_SERVER_CMD_BAK" ]; then
    echo "Restoring API Server Command from backup..."
    mv "$API_SERVER_CMD_BAK" "$API_SERVER_CMD"
    echo -e "${GREEN}✓ API Server Command restored${NC}"
    REVERTED=$((REVERTED + 1))
else
    if grep -q "# Patch for macOS: setproctitle causes SIGSEGV" "$API_SERVER_CMD" 2>/dev/null; then
        echo -e "${YELLOW}! API Server Command is patched but no backup found${NC}"
        echo "  You may need to reinstall Airflow or recreate the tox environment"
        ERRORS=$((ERRORS + 1))
    else
        echo -e "${GREEN}✓ API Server Command was not patched${NC}"
    fi
fi
echo ""

# Note about setproctitle
echo -e "${YELLOW}Note about setproctitle package:${NC}"
echo "The setproctitle package was removed by the patch."
echo "To reinstall it, run:"
echo "  $TOX_ENV_PATH/bin/python -m pip install setproctitle"
echo ""

# Summary
echo "========================================"
if [ $ERRORS -gt 0 ]; then
    echo -e "${YELLOW}⚠ Revert completed with warnings${NC}"
    echo "Files reverted: $REVERTED"
    echo "Warnings: $ERRORS"
    echo ""
    echo "Some patches could not be reverted because backups were not found."
    echo "To fully restore, consider recreating the tox environment:"
    echo "  rm -rf $TOX_ENV_PATH"
    echo "  tox -e py311-airflow302 --recreate"
elif [ $REVERTED -gt 0 ]; then
    echo -e "${GREEN}✓ Revert completed successfully${NC}"
    echo "Files reverted: $REVERTED"
    echo ""
    echo "The patches have been removed."
    echo "Don't forget to reinstall setproctitle if needed."
else
    echo -e "${GREEN}✓ No patches to revert${NC}"
    echo "The environment was not patched."
fi
echo "========================================"