"""Stands in for ingestion code a probe reuses: it quotes its input on failure.

Lives in its own module so the framework sees the raise as foreign, exactly as
it sees source_connectors.py or glue.py.
"""

SENTINEL = "PLANTED-foreign-secret"


def parse_url(url: str) -> None:
    raise ValueError(f"Missing database name in JDBC URL: {url}")


def connect() -> None:
    class TransportError(Exception):
        pass

    raise TransportError(
        f"token endpoint said: {{'error': 'invalid', 'sub': '{SENTINEL}'}}"
    )
