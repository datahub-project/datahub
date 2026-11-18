"""DataHub integration with Unstructured.io for processing unstructured documents."""

import os

# unstructured>=0.26 sends a telemetry ping on import (and >=0.27 one per partition call)
# unless opted out; keep it off, as on 0.24.x. >=0.27 also logs a commercial-offering
# WARNING per process. Every source that uses unstructured imports this package first.
# setdefault keeps any value the user set themselves.
os.environ.setdefault("SCARF_NO_ANALYTICS", "1")
os.environ.setdefault("UNSTRUCTURED_DISABLE_NOTICE", "1")
