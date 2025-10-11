#!/bin/bash
# Patch script for fixing Airflow 3.0 SIGSEGV crashes on macOS
# This script removes setproctitle and patches Airflow to make it optional
#
# Usage: ./scripts/patch_airflow_macos_sigsegv.sh [tox_env_path]
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

echo -e "${YELLOW}Airflow 3.0 macOS SIGSEGV Patch Script${NC}"
echo "========================================"
echo ""

# Check if running on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    echo -e "${YELLOW}Warning: This script is intended for macOS only.${NC}"
    echo "You are running on: $OSTYPE"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check if tox environment exists
if [ ! -d "$TOX_ENV_PATH" ]; then
    echo -e "${RED}Error: Tox environment not found at $TOX_ENV_PATH${NC}"
    echo "Please create the tox environment first with: tox -e py311-airflow302"
    exit 1
fi

SITE_PACKAGES="$TOX_ENV_PATH/lib/python3.11/site-packages"

if [ ! -d "$SITE_PACKAGES" ]; then
    echo -e "${RED}Error: Site-packages directory not found at $SITE_PACKAGES${NC}"
    exit 1
fi

echo "Using tox environment: $TOX_ENV_PATH"
echo ""

# Step 1: Remove setproctitle package
echo -e "${YELLOW}Step 1: Removing setproctitle package...${NC}"

SETPROCTITLE_DIRS=$(find "$SITE_PACKAGES" -maxdepth 1 -name "setproctitle*" -type d 2>/dev/null || true)

if [ -z "$SETPROCTITLE_DIRS" ]; then
    echo -e "${GREEN}✓ setproctitle is already removed${NC}"
else
    echo "Found setproctitle packages:"
    echo "$SETPROCTITLE_DIRS"
    echo ""

    for dir in $SETPROCTITLE_DIRS; do
        echo "Removing: $dir"
        rm -rf "$dir"
    done

    echo -e "${GREEN}✓ setproctitle package removed${NC}"
fi
echo ""

# Step 2: Patch LocalExecutor
echo -e "${YELLOW}Step 2: Patching LocalExecutor...${NC}"

LOCAL_EXECUTOR="$SITE_PACKAGES/airflow/executors/local_executor.py"

if [ ! -f "$LOCAL_EXECUTOR" ]; then
    echo -e "${RED}Error: LocalExecutor not found at $LOCAL_EXECUTOR${NC}"
    exit 1
fi

# Check if already patched
if grep -q "# Patch for macOS: setproctitle causes SIGSEGV" "$LOCAL_EXECUTOR"; then
    echo -e "${GREEN}✓ LocalExecutor is already patched${NC}"
else
    # Create backup
    cp "$LOCAL_EXECUTOR" "$LOCAL_EXECUTOR.bak"
    echo "Created backup: $LOCAL_EXECUTOR.bak"

    # Apply patch using sed
    # This replaces the line "from setproctitle import setproctitle" with the patched version
    sed -i.tmp '/^from setproctitle import setproctitle$/c\
# Patch for macOS: setproctitle causes SIGSEGV crashes on macOS due to fork() + native extensions\
# See: https://github.com/benoitc/gunicorn/issues/3021\
try:\
    from setproctitle import setproctitle\
except ImportError:\
    def setproctitle(title):\
        pass  # No-op if setproctitle is not available
' "$LOCAL_EXECUTOR"

    rm -f "$LOCAL_EXECUTOR.tmp"

    echo -e "${GREEN}✓ LocalExecutor patched successfully${NC}"
fi
echo ""

# Step 3: Patch API Server Command
echo -e "${YELLOW}Step 3: Patching API Server Command...${NC}"

API_SERVER_CMD="$SITE_PACKAGES/airflow/cli/commands/api_server_command.py"

if [ ! -f "$API_SERVER_CMD" ]; then
    echo -e "${RED}Error: API Server Command not found at $API_SERVER_CMD${NC}"
    exit 1
fi

# Check if already patched
if grep -q "# Patch for macOS: setproctitle causes SIGSEGV" "$API_SERVER_CMD"; then
    echo -e "${GREEN}✓ API Server Command is already patched${NC}"
else
    # Create backup
    cp "$API_SERVER_CMD" "$API_SERVER_CMD.bak"
    echo "Created backup: $API_SERVER_CMD.bak"

    # Apply patch using sed
    sed -i.tmp '/^from setproctitle import setproctitle$/c\
# Patch for macOS: setproctitle causes SIGSEGV crashes on macOS due to fork() + native extensions\
# See: https://github.com/benoitc/gunicorn/issues/3021\
try:\
    from setproctitle import setproctitle\
except ImportError:\
    def setproctitle(title):\
        pass  # No-op if setproctitle is not available
' "$API_SERVER_CMD"

    rm -f "$API_SERVER_CMD.tmp"

    echo -e "${GREEN}✓ API Server Command patched successfully${NC}"
fi
echo ""

# Step 4: Verify patches
echo -e "${YELLOW}Step 4: Verifying patches...${NC}"

VERIFICATION_FAILED=0

# Verify setproctitle is removed
if find "$SITE_PACKAGES" -maxdepth 1 -name "setproctitle*" -type d 2>/dev/null | grep -q .; then
    echo -e "${RED}✗ setproctitle package still exists${NC}"
    VERIFICATION_FAILED=1
else
    echo -e "${GREEN}✓ setproctitle package removed${NC}"
fi

# Verify LocalExecutor patch
if grep -q "# Patch for macOS: setproctitle causes SIGSEGV" "$LOCAL_EXECUTOR" && \
   grep -q "try:" "$LOCAL_EXECUTOR" && \
   grep -q "except ImportError:" "$LOCAL_EXECUTOR"; then
    echo -e "${GREEN}✓ LocalExecutor patch verified${NC}"
else
    echo -e "${RED}✗ LocalExecutor patch verification failed${NC}"
    VERIFICATION_FAILED=1
fi

# Verify API Server Command patch
if grep -q "# Patch for macOS: setproctitle causes SIGSEGV" "$API_SERVER_CMD" && \
   grep -q "try:" "$API_SERVER_CMD" && \
   grep -q "except ImportError:" "$API_SERVER_CMD"; then
    echo -e "${GREEN}✓ API Server Command patch verified${NC}"
else
    echo -e "${RED}✗ API Server Command patch verification failed${NC}"
    VERIFICATION_FAILED=1
fi

echo ""

if [ $VERIFICATION_FAILED -eq 0 ]; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}✓ All patches applied successfully!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "The Airflow 3.0 SIGSEGV issue should now be fixed."
    echo "You can run your tests with:"
    echo "  tox -e py311-airflow302 -- tests/integration/test_plugin.py -v"
    echo ""
    echo "Backups were created with .bak extension if you need to revert."
    exit 0
else
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}✗ Some patches failed verification${NC}"
    echo -e "${RED}========================================${NC}"
    echo ""
    echo "Please check the output above and verify manually."
    echo "Backups were created with .bak extension."
    exit 1
fi