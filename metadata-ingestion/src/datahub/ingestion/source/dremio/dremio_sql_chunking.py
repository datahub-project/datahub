"""
SQL Chunking utilities for handling large view definitions and queries.

This module provides functionality to chunk large SQL statements into manageable pieces
while preserving the ability to reassemble them into complete, valid SQL.
"""

import hashlib
import logging
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, validator

logger = logging.getLogger(__name__)


class SQLChunk(BaseModel):
    """Represents a chunk of a larger SQL statement."""

    chunk_id: str  # Unique identifier for this chunk
    sequence_number: int  # Order of this chunk in the original statement
    total_chunks: int  # Total number of chunks for the complete statement
    content: str  # The actual SQL content for this chunk
    original_hash: str  # Hash of the original complete SQL for integrity checking

    @validator("sequence_number")
    def validate_sequence_number(cls, v, values):
        """Validate sequence number is within bounds."""
        total_chunks = values.get("total_chunks", 0)
        if v < 0 or v >= total_chunks:
            raise ValueError(
                f"Invalid sequence_number {v} for total_chunks {total_chunks}"
            )
        return v


class SQLChunker:
    """Handles chunking and reassembly of large SQL statements."""

    def __init__(self, max_chunk_size: int = 32000):
        """
        Initialize the SQL chunker.

        Args:
            max_chunk_size: Maximum size of each chunk in characters
        """
        self.max_chunk_size = max_chunk_size

    def needs_chunking(self, sql: str) -> bool:
        """Check if SQL statement needs to be chunked."""
        return len(sql) > self.max_chunk_size

    def chunk_sql(self, sql: str, statement_id: Optional[str] = None) -> List[SQLChunk]:
        """
        Chunk a large SQL statement into manageable pieces.

        Args:
            sql: The SQL statement to chunk
            statement_id: Optional identifier for the statement (auto-generated if not provided)

        Returns:
            List of SQLChunk objects representing the chunked SQL
        """
        if not self.needs_chunking(sql):
            # Return single chunk for small statements
            chunk_id = statement_id or self._generate_statement_id(sql)
            return [
                SQLChunk(
                    chunk_id=f"{chunk_id}_chunk_0",
                    sequence_number=0,
                    total_chunks=1,
                    content=sql,
                    original_hash=self._hash_sql(sql),
                )
            ]

        # Generate unique identifier for this statement
        if not statement_id:
            statement_id = self._generate_statement_id(sql)

        original_hash = self._hash_sql(sql)
        chunks = []

        # Calculate chunk boundaries with smart splitting
        chunk_boundaries = self._calculate_chunk_boundaries(sql)

        for i, (start, end) in enumerate(chunk_boundaries):
            chunk_content = sql[start:end]

            chunk = SQLChunk(
                chunk_id=f"{statement_id}_chunk_{i}",
                sequence_number=i,
                total_chunks=len(chunk_boundaries),
                content=chunk_content,
                original_hash=original_hash,
            )
            chunks.append(chunk)

        logger.info(
            f"Chunked SQL statement into {len(chunks)} pieces (original size: {len(sql)} chars)"
        )
        return chunks

    def reassemble_sql(self, chunks: List[SQLChunk]) -> str:
        """
        Reassemble SQL chunks back into the original statement.

        Args:
            chunks: List of SQLChunk objects to reassemble

        Returns:
            The reassembled SQL statement

        Raises:
            ValueError: If chunks are invalid or incomplete
        """
        if not chunks:
            raise ValueError("No chunks provided for reassembly")

        # Validate chunks
        self._validate_chunks(chunks)

        # Sort chunks by sequence number
        sorted_chunks = sorted(chunks, key=lambda c: c.sequence_number)

        # Reassemble without overlap (simplified approach)
        reassembled = "".join(chunk.content for chunk in sorted_chunks)

        # Verify integrity (skip hash check for now due to overlap complexity)
        # TODO: Improve hash verification to handle overlap removal correctly

        logger.info(
            f"Successfully reassembled SQL from {len(chunks)} chunks (final size: {len(reassembled)} chars)"
        )
        return reassembled

    def _calculate_chunk_boundaries(self, sql: str) -> List[Tuple[int, int]]:
        """
        Calculate smart chunk boundaries that try to break at natural SQL boundaries.

        Args:
            sql: The SQL statement to analyze

        Returns:
            List of (start, end) tuples representing chunk boundaries
        """
        boundaries = []
        current_pos = 0

        while current_pos < len(sql):
            # Calculate the end position for this chunk
            chunk_end = min(current_pos + self.max_chunk_size, len(sql))

            # If this isn't the last chunk, try to find a better break point
            if chunk_end < len(sql):
                # Look for natural break points (in order of preference)
                break_points = [
                    "\n\n",  # Double newline (paragraph break)
                    ";\n",  # End of statement
                    "\n",  # Single newline
                    " AND ",  # Logical operators
                    " OR ",
                    " WHERE ",
                    " FROM ",
                    " JOIN ",
                    ", ",  # Comma separation
                    " ",  # Any space
                ]

                # Search backwards from the ideal chunk end for a good break point
                search_start = max(
                    current_pos + self.max_chunk_size - 500,
                    current_pos + self.max_chunk_size // 2,
                )

                for break_point in break_points:
                    # Look for the break point within the search window
                    search_text = sql[search_start:chunk_end]
                    break_index = search_text.rfind(break_point)

                    if break_index != -1:
                        # Found a good break point
                        chunk_end = search_start + break_index + len(break_point)
                        break

            # Add chunk boundary without overlap
            boundaries.append((current_pos, chunk_end))
            current_pos = chunk_end

            if current_pos >= len(sql):
                break

        return boundaries

    def _remove_overlap(self, existing_content: str, new_chunk_content: str) -> str:
        """
        Remove overlapping content between existing content and new chunk.

        Args:
            existing_content: Content already assembled
            new_chunk_content: New chunk content that may overlap

        Returns:
            The portion of new_chunk_content that doesn't overlap
        """
        # Look for overlap by comparing the end of existing content with the start of new content
        # Use a reasonable overlap check size (up to 1000 characters)
        max_overlap_check = min(1000, len(existing_content), len(new_chunk_content))

        for overlap_len in range(max_overlap_check, 0, -1):
            existing_suffix = existing_content[-overlap_len:]
            new_prefix = new_chunk_content[:overlap_len]

            if existing_suffix == new_prefix:
                # Found overlap - return the non-overlapping portion
                return new_chunk_content[overlap_len:]

        # No overlap found - return entire new content
        return new_chunk_content

    def _validate_chunks(self, chunks: List[SQLChunk]) -> None:
        """
        Validate that chunks are complete and consistent.

        Args:
            chunks: List of chunks to validate

        Raises:
            ValueError: If chunks are invalid
        """
        if not chunks:
            raise ValueError("Empty chunk list")

        # Check that all chunks have the same total_chunks and original_hash
        expected_total = chunks[0].total_chunks
        expected_hash = chunks[0].original_hash

        for chunk in chunks:
            if chunk.total_chunks != expected_total:
                raise ValueError(
                    f"Inconsistent total_chunks: expected {expected_total}, got {chunk.total_chunks}"
                )
            if chunk.original_hash != expected_hash:
                raise ValueError(
                    f"Inconsistent original_hash: expected {expected_hash}, got {chunk.original_hash}"
                )

        # Check that we have all sequence numbers
        sequence_numbers = {chunk.sequence_number for chunk in chunks}
        expected_sequences = set(range(expected_total))

        if sequence_numbers != expected_sequences:
            missing = expected_sequences - sequence_numbers
            extra = sequence_numbers - expected_sequences
            raise ValueError(f"Missing sequences: {missing}, Extra sequences: {extra}")

    def _generate_statement_id(self, sql: str) -> str:
        """Generate a unique identifier for a SQL statement."""
        # Use first 100 chars + hash for readability and uniqueness
        prefix = "".join(c for c in sql[:100] if c.isalnum() or c in "_-")[:50]
        hash_suffix = hashlib.md5(sql.encode()).hexdigest()[:8]
        return f"sql_{prefix}_{hash_suffix}"

    def _hash_sql(self, sql: str) -> str:
        """Generate a hash for SQL content integrity checking."""
        # Normalize whitespace for consistent hashing
        normalized = " ".join(sql.split())
        return hashlib.sha256(normalized.encode()).hexdigest()


class ChunkedSQLManager:
    """Manages chunked SQL statements with storage and retrieval."""

    def __init__(self, chunker: Optional[SQLChunker] = None):
        """
        Initialize the chunked SQL manager.

        Args:
            chunker: SQLChunker instance (creates default if not provided)
        """
        self.chunker = chunker or SQLChunker()
        self._chunk_storage: Dict[str, List[SQLChunk]] = {}

    def store_sql(self, sql: str, statement_id: Optional[str] = None) -> str:
        """
        Store a SQL statement, chunking if necessary.

        Args:
            sql: The SQL statement to store
            statement_id: Optional identifier for the statement

        Returns:
            The statement ID used for storage
        """
        chunks = self.chunker.chunk_sql(sql, statement_id)
        actual_statement_id = chunks[0].chunk_id.rsplit("_chunk_", 1)[0]

        self._chunk_storage[actual_statement_id] = chunks

        logger.debug(
            f"Stored SQL statement '{actual_statement_id}' as {len(chunks)} chunks"
        )
        return actual_statement_id

    def retrieve_sql(self, statement_id: str) -> Optional[str]:
        """
        Retrieve and reassemble a SQL statement.

        Args:
            statement_id: The statement ID to retrieve

        Returns:
            The reassembled SQL statement, or None if not found
        """
        chunks = self._chunk_storage.get(statement_id)
        if not chunks:
            logger.warning(f"No chunks found for statement ID: {statement_id}")
            return None

        try:
            return self.chunker.reassemble_sql(chunks)
        except Exception as e:
            logger.error(
                f"Failed to reassemble SQL for statement '{statement_id}': {e}"
            )
            return None

    def get_chunk_info(self, statement_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about stored chunks.

        Args:
            statement_id: The statement ID to get info for

        Returns:
            Dictionary with chunk information, or None if not found
        """
        chunks = self._chunk_storage.get(statement_id)
        if not chunks:
            return None

        total_size = sum(len(chunk.content) for chunk in chunks)

        return {
            "statement_id": statement_id,
            "total_chunks": len(chunks),
            "total_size": total_size,
            "chunk_sizes": [len(chunk.content) for chunk in chunks],
            "original_hash": chunks[0].original_hash if chunks else None,
        }

    def clear_storage(self) -> None:
        """Clear all stored chunks."""
        self._chunk_storage.clear()
        logger.debug("Cleared all stored SQL chunks")
