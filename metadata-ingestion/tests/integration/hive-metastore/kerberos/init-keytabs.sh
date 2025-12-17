#!/bin/sh
# =============================================================================
# Initialize Kerberos Principals and Keytabs
# =============================================================================
# This script runs inside the KDC container after it starts.
# It creates all necessary service principals and exports keytabs.
#
# Principals Created:
# - hive/hive-metastore@TEST.LOCAL (HMS service - short hostname)
# - hive/hive-metastore.test.local@TEST.LOCAL (HMS service - FQDN)
# - hive/hive-server@TEST.LOCAL (HiveServer2 service)
# - testuser@TEST.LOCAL (Test user for client connections)
# - HTTP/* (HTTP principals for web UIs)
# =============================================================================

set -e

REALM="TEST.LOCAL"
KEYTAB_DIR="/keytabs"

echo "=============================================="
echo "Initializing Kerberos Principals"
echo "=============================================="

# Clean up any existing keytabs to ensure fresh keys
rm -f ${KEYTAB_DIR}/*.keytab 2>/dev/null || true

echo ""
echo "1. Creating service principals..."

# HMS principals (both short and FQDN for compatibility)
kadmin.local -q "addprinc -randkey hive/hive-metastore@${REALM}" 2>/dev/null || true
kadmin.local -q "addprinc -randkey hive/hive-metastore.test.local@${REALM}" 2>/dev/null || true

# HiveServer2 principal
kadmin.local -q "addprinc -randkey hive/hive-server@${REALM}" 2>/dev/null || true

# Test user principal for client connections
kadmin.local -q "addprinc -randkey testuser@${REALM}" 2>/dev/null || true

# HTTP principals for web UIs
kadmin.local -q "addprinc -randkey HTTP/hive-metastore@${REALM}" 2>/dev/null || true
kadmin.local -q "addprinc -randkey HTTP/hive-server@${REALM}" 2>/dev/null || true

echo "   ✓ Principals created"

echo ""
echo "2. Exporting keytabs..."

# Hive service keytab (used by HMS and HiveServer2)
kadmin.local -q "ktadd -k ${KEYTAB_DIR}/hive.keytab hive/hive-metastore@${REALM}"
kadmin.local -q "ktadd -k ${KEYTAB_DIR}/hive.keytab hive/hive-metastore.test.local@${REALM}"
kadmin.local -q "ktadd -k ${KEYTAB_DIR}/hive.keytab hive/hive-server@${REALM}"

# HTTP keytab
kadmin.local -q "ktadd -k ${KEYTAB_DIR}/http.keytab HTTP/hive-metastore@${REALM}"
kadmin.local -q "ktadd -k ${KEYTAB_DIR}/http.keytab HTTP/hive-server@${REALM}"

# Test user keytab
kadmin.local -q "ktadd -k ${KEYTAB_DIR}/testuser.keytab testuser@${REALM}"

# Set permissions (readable by all containers sharing the volume)
chmod 644 ${KEYTAB_DIR}/*.keytab

echo "   ✓ Keytabs exported"

echo ""
echo "3. Verification..."
echo ""
echo "Keytab files:"
ls -la ${KEYTAB_DIR}/

echo ""
echo "Hive keytab contents:"
klist -kt ${KEYTAB_DIR}/hive.keytab

echo ""
echo "Testuser keytab contents:"
klist -kt ${KEYTAB_DIR}/testuser.keytab

echo ""
echo "=============================================="
echo "✅ Kerberos initialization complete!"
echo "=============================================="
