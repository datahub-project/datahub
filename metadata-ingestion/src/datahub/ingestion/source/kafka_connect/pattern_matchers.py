"""Pattern matching utilities for Kafka Connect connectors.

This module provides pluggable pattern matching for different connector types.
Each connector type (Debezium, Snowflake, etc.) uses a different pattern syntax,
so this abstraction allows each to implement its own matcher while sharing
common filtering logic.
"""

import fnmatch
import logging
from typing import List, Protocol

logger = logging.getLogger(__name__)


class PatternMatcher(Protocol):
    """Protocol for pattern matching strategies.

    Different Kafka Connect connectors use different pattern syntaxes:
    - Debezium connectors: Full Java regex (character classes, quantifiers, etc.)
    - Snowflake connectors: Simple wildcards (* and ?)

    This protocol allows each connector to provide its own implementation.
    """

    def matches(self, pattern: str, text: str) -> bool:
        """Check if text matches the given pattern.

        Args:
            pattern: The pattern to match against (syntax depends on implementation)
            text: The text to check for a match

        Returns:
            True if text matches pattern, False otherwise
        """
        ...

    def filter_matches(self, patterns: List[str], texts: List[str]) -> List[str]:
        """Filter a list of texts to only those matching at least one pattern.

        Args:
            patterns: List of patterns to match against
            texts: List of texts to filter

        Returns:
            List of texts that match at least one pattern
        """
        ...


class JavaRegexMatcher:
    """Pattern matcher using Java regex syntax.

    Used by Debezium connectors (PostgresCDCV2, etc.) which rely on Java's
    java.util.regex.Pattern for table.include.list and table.exclude.list.

    Java regex supports full regex features:
    - Character classes: [abc], [^abc], [a-zA-Z]
    - Quantifiers: *, +, ?, {n}, {n,}, {n,m}
    - Alternation: (pattern1|pattern2)
    - Anchors: ^, $
    - Escaping: \\. for literal dots, etc.

    Examples:
        "public\\.(users|orders).*" matches tables starting with users or orders in public schema
        ".*\\.fact_.*" matches all fact_ tables in any schema
        "reporting\\.dim_[0-9]+" matches dim_1, dim_2, etc. in reporting schema

    Note: Requires JPype and Java runtime. If Java is unavailable, matching will fail
    and log warnings rather than silently producing incorrect results.
    """

    def matches(self, pattern: str, text: str) -> bool:
        """Check if text matches Java regex pattern.

        Args:
            pattern: Java regex pattern
            text: Text to match against

        Returns:
            True if text matches pattern, False if no match or Java unavailable
        """
        try:
            from java.util.regex import Pattern as JavaPattern

            regex_pattern = JavaPattern.compile(pattern)
            return bool(regex_pattern.matcher(text).matches())

        except (ImportError, RuntimeError) as e:
            logger.warning(
                f"Java regex library not available for pattern matching: {e}. "
                f"Cannot match pattern '{pattern}' against '{text}'. "
                f"Debezium uses Java regex and Python regex is not compatible."
            )
            return False
        except Exception as e:
            logger.warning(
                f"Failed to compile or match Java regex pattern '{pattern}': {e}"
            )
            return False

    def filter_matches(self, patterns: List[str], texts: List[str]) -> List[str]:
        """Filter texts by matching against Java regex patterns.

        Args:
            patterns: List of Java regex patterns
            texts: List of texts to filter

        Returns:
            List of texts matching at least one pattern, or empty list if Java unavailable
        """
        try:
            from java.util.regex import Pattern as JavaPattern
        except (ImportError, RuntimeError) as e:
            logger.warning(
                f"Java regex library not available for pattern matching: {e}. "
                f"Cannot filter texts using patterns {patterns}. "
                f"Debezium uses Java regex and Python regex is not compatible. "
                f"Returning empty list."
            )
            return []

        matched_texts_set = set()

        for pattern in patterns:
            try:
                regex_pattern = JavaPattern.compile(pattern)

                for text in texts:
                    if regex_pattern.matcher(text).matches():
                        matched_texts_set.add(text)

            except Exception as e:
                logger.warning(
                    f"Failed to compile or match Java regex pattern '{pattern}': {e}"
                )

        return list(matched_texts_set)


class WildcardMatcher:
    """Pattern matcher using simple wildcard syntax.

    Used by Snowflake Source connectors which use simple shell-style wildcards
    for table.include.list and table.exclude.list.

    Supported wildcards:
    - "*" matches any sequence of characters (zero or more)
    - "?" matches any single character

    This is MUCH SIMPLER than Java regex - no character classes, quantifiers,
    or alternation. Just basic wildcards.

    Examples:
        "ANALYTICS.PUBLIC.*" matches all tables in ANALYTICS.PUBLIC schema
        "*.PUBLIC.TABLE1" matches TABLE1 in PUBLIC schema across all databases
        "DB.SCHEMA.USER?" matches USER1, USERS, etc.

    Implementation uses Python's fnmatch module which provides shell-style
    wildcard matching without full regex complexity.
    """

    def matches(self, pattern: str, text: str) -> bool:
        """Check if text matches wildcard pattern.

        Args:
            pattern: Wildcard pattern using * and ?
            text: Text to match against

        Returns:
            True if text matches pattern, False otherwise
        """
        return fnmatch.fnmatch(text, pattern)

    def filter_matches(self, patterns: List[str], texts: List[str]) -> List[str]:
        """Filter texts by matching against wildcard patterns.

        Args:
            patterns: List of wildcard patterns
            texts: List of texts to filter

        Returns:
            List of texts matching at least one pattern
        """
        matched_texts_set = set()

        for pattern in patterns:
            for text in texts:
                if fnmatch.fnmatch(text, pattern):
                    matched_texts_set.add(text)

        return list(matched_texts_set)
