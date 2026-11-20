"""TLS material for the SQL Server fixture.

Lives under the test directory, not a temp dir: the in-image suite bind-mounts
the repo at the same path the host daemon sees, and a path that exists only
inside the test container cannot be mounted.
"""

import os
from pathlib import Path

from tests.test_helpers.self_signed_cert import write_self_signed_cert


def prepare_mssql_tls(test_resources_dir: Path) -> Path:
    """Generate the cert the container and the pytds client both use.

    Sets ``MSSQL_TLS_DIR`` (compose bind-mount) and ``MSSQL_CAFILE`` (recipe
    ``connect_args.cafile``). Returns the certificate path.
    """
    tls_dir = test_resources_dir / ".tls"
    ca_path = write_self_signed_cert(tls_dir)
    os.environ["MSSQL_TLS_DIR"] = str(tls_dir)
    os.environ["MSSQL_CAFILE"] = str(ca_path)
    return ca_path
