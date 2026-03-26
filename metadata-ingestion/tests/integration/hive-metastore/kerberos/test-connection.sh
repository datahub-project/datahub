#!/bin/bash
# =============================================================================
# Test Kerberos HMS Connection
# =============================================================================
# This script verifies the Kerberos setup is working correctly.
# Run this from the kerberos-client container.
#
# Usage:
#   docker exec kerberos-client /test-connection.sh
# =============================================================================

set -e

echo "=============================================="
echo "Kerberos HMS Connection Test"
echo "=============================================="

# Step 1: Authenticate
echo ""
echo "1. Authenticating with Kerberos..."
kinit -kt /keytabs/testuser.keytab testuser@TEST.LOCAL
echo "   ✓ Authenticated as:"
klist | grep "Default principal"

# Step 2: Test connectivity
echo ""
echo "2. Testing HMS port connectivity..."
if nc -z hive-metastore 9083 2>/dev/null; then
    echo "   ✓ HMS port 9083 is reachable"
else
    echo "   ✗ HMS port 9083 is NOT reachable"
    exit 1
fi

# Step 3: Test Thrift connection
echo ""
echo "3. Testing HMS Thrift connection..."
python3 << 'EOF'
import sys
try:
    from thrift.transport import TSocket
    from thrift.protocol import TBinaryProtocol
    from thrift_sasl import TSaslClientTransport
    from pyhive.sasl_compat import PureSASLClient
    from pymetastore.hive_metastore import ThriftHiveMetastore

    socket = TSocket.TSocket('hive-metastore', 9083)
    
    def sasl_factory():
        return PureSASLClient('hive-metastore', service='hive', mechanism='GSSAPI')
    
    transport = TSaslClientTransport(sasl_factory, 'GSSAPI', socket)
    transport.open()
    
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = ThriftHiveMetastore.Client(protocol)
    
    # List databases
    databases = client.get_all_databases()
    print(f"   ✓ Connected! Found {len(databases)} database(s): {databases}")
    
    # List tables in db1 if it exists
    if 'db1' in databases:
        tables = client.get_all_tables('db1')
        print(f"   ✓ db1 has {len(tables)} table(s): {tables}")
        
        # Show one complex type sample
        if 'struct_test' in tables:
            t = client.get_table('db1', 'struct_test')
            cols = [(c.name, c.type) for c in t.sd.cols]
            print(f"   ✓ struct_test columns: {cols}")
    
    transport.close()
    
except Exception as e:
    print(f"   ✗ Connection failed: {e}")
    sys.exit(1)
EOF

echo ""
echo "=============================================="
echo "✅ All tests passed!"
echo "=============================================="
