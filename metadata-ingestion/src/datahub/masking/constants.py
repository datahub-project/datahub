"""Shared constants for the secret-masking framework.

``REDACTED_FORMAT`` lives here so both ``secret_registry`` (validation at
registration time) and ``masking_filter`` (the masking callback) reference
one definition. A duplicate would let them drift and re-open the route-A
hole (a marker-shaped value passing registration because the registry's
format string differs from the filter's).
"""

REDACTED_FORMAT = "***REDACTED:{name}***"
