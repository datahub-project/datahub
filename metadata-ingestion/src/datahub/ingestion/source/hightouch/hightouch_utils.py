import logging

logger = logging.getLogger(__name__)

# Exceptions that signal a bug in our own code rather than an operational or
# environmental failure. These are re-raised (fail fast) instead of degraded.
_PROGRAMMING_ERRORS = (AttributeError, TypeError, KeyError, ValueError)


def normalize_column_name(name: str) -> str:
    return name.lower().replace("_", "").replace("-", "")


def reraise_if_programming_error(e: Exception, context: str) -> None:
    """Re-raise ``e`` (after logging) if it looks like a programming bug.

    Optional/best-effort code paths wrap their body in ``except Exception`` and
    call this first, so genuine bugs fail fast with a stack trace while
    operational failures fall through to the caller's degrade handling.
    """
    if isinstance(e, _PROGRAMMING_ERRORS):
        logger.error(
            f"Programming error {context}: {type(e).__name__}: {e}",
            exc_info=True,
        )
        raise e
