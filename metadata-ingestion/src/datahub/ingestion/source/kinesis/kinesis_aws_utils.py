"""Tiny shared utilities for the Kinesis connector's AWS error handling.

Kept deliberately small: the connector catches `(ClientError, BotoCoreError)`
in 6 spots across kinesis.py / kinesis_stream.py / kinesis_firehose.py, and
each one repeats the same isinstance discriminator. This module exposes one
helper, ``aws_error_code``, so those callsites read as a single expression.
"""

from typing import Union

from botocore.exceptions import BotoCoreError, ClientError


def aws_error_code(e: Union[ClientError, BotoCoreError]) -> str:
    """Return a short human-readable code for an AWS SDK exception.

    For ``ClientError`` (the structured API error: 4xx/5xx with a wire-format
    response body), returns ``e.response["Error"]["Code"]`` —
    e.g. ``AccessDeniedException``, ``ThrottlingException``,
    ``ValidationException``.

    For ``BotoCoreError`` (the unstructured client-side errors:
    ``NoCredentialsError``, ``EndpointConnectionError``, ``ConnectTimeoutError``,
    etc., which don't have a ``.response`` attribute), returns the exception's
    class name, since that's the only stable identifier those exceptions
    expose.

    The returned string goes into report warning messages and skip-reason
    records, so it must be safe to log without further sanitization.
    """
    if isinstance(e, ClientError):
        return e.response.get("Error", {}).get("Code", "")
    # BotoCoreError subclasses don't carry a structured code; the class name
    # (e.g. "NoCredentialsError") is the next-best stable identifier and is
    # what users see in their stack traces when these fire untrapped.
    return type(e).__name__
