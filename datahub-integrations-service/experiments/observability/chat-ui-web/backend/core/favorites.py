"""
Favorites Database - SQLite-based storage for bookmarked conversations.

Provides persistent storage for favoriting/starring conversations across sessions.
Favorites are partitioned by profile (customer instance) to avoid URN collisions.
"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from loguru import logger
from pydantic import BaseModel


class FavoriteConversation(BaseModel):
    """A favorited conversation record."""

    profile_id: str  # Profile identifier (e.g., profile name or GMS URL hash)
    urn: str
    created_at: str  # ISO format timestamp
    notes: Optional[str] = None


class FavoritesDB:
    """SQLite database for managing favorite conversations."""

    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize the favorites database.

        Args:
            db_path: Path to SQLite database file. Defaults to ~/.datahub/chat_admin/favorites.db
        """
        if db_path is None:
            db_path = Path.home() / ".datahub" / "chat_admin" / "favorites.db"

        self.db_path = db_path
        self._ensure_db_exists()

    def _ensure_db_exists(self) -> None:
        """Create the database and tables if they don't exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with self._get_connection() as conn:
            # Composite primary key: (profile_id, urn) to partition by customer
            conn.execute("""
                CREATE TABLE IF NOT EXISTS favorites (
                    profile_id TEXT NOT NULL,
                    urn TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    notes TEXT,
                    PRIMARY KEY (profile_id, urn)
                )
            """)
            # Index for efficient lookups by profile
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_favorites_profile
                ON favorites (profile_id)
            """)
            conn.commit()
            logger.debug(f"Favorites database initialized at {self.db_path}")

    @contextmanager
    def _get_connection(self):
        """Get a database connection with context manager."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def add_favorite(
        self, profile_id: str, urn: str, notes: Optional[str] = None
    ) -> FavoriteConversation:
        """
        Add a conversation to favorites.

        Args:
            profile_id: Profile/customer identifier (e.g., profile name)
            urn: Conversation URN
            notes: Optional notes about this favorite

        Returns:
            The created favorite record
        """
        created_at = datetime.now(timezone.utc).isoformat()

        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO favorites (profile_id, urn, created_at, notes)
                VALUES (?, ?, ?, ?)
                """,
                (profile_id, urn, created_at, notes),
            )
            conn.commit()

        logger.info(f"Added favorite: {profile_id}/{urn}")
        return FavoriteConversation(
            profile_id=profile_id, urn=urn, created_at=created_at, notes=notes
        )

    def remove_favorite(self, profile_id: str, urn: str) -> bool:
        """
        Remove a conversation from favorites.

        Args:
            profile_id: Profile/customer identifier
            urn: Conversation URN

        Returns:
            True if removed, False if not found
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                "DELETE FROM favorites WHERE profile_id = ? AND urn = ?",
                (profile_id, urn),
            )
            conn.commit()
            deleted = cursor.rowcount > 0

        if deleted:
            logger.info(f"Removed favorite: {profile_id}/{urn}")
        else:
            logger.debug(f"Favorite not found: {profile_id}/{urn}")

        return deleted

    def is_favorite(self, profile_id: str, urn: str) -> bool:
        """
        Check if a conversation is favorited.

        Args:
            profile_id: Profile/customer identifier
            urn: Conversation URN

        Returns:
            True if favorited, False otherwise
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT 1 FROM favorites WHERE profile_id = ? AND urn = ?",
                (profile_id, urn),
            )
            return cursor.fetchone() is not None

    def get_favorite(self, profile_id: str, urn: str) -> Optional[FavoriteConversation]:
        """
        Get a favorite by profile and URN.

        Args:
            profile_id: Profile/customer identifier
            urn: Conversation URN

        Returns:
            FavoriteConversation if found, None otherwise
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT profile_id, urn, created_at, notes FROM favorites WHERE profile_id = ? AND urn = ?",
                (profile_id, urn),
            )
            row = cursor.fetchone()

            if row:
                return FavoriteConversation(
                    profile_id=row["profile_id"],
                    urn=row["urn"],
                    created_at=row["created_at"],
                    notes=row["notes"],
                )
            return None

    def list_favorites(self, profile_id: str) -> List[FavoriteConversation]:
        """
        List all favorited conversations for a profile.

        Args:
            profile_id: Profile/customer identifier

        Returns:
            List of FavoriteConversation records, ordered by created_at desc
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT profile_id, urn, created_at, notes FROM favorites WHERE profile_id = ? ORDER BY created_at DESC",
                (profile_id,),
            )
            rows = cursor.fetchall()

            return [
                FavoriteConversation(
                    profile_id=row["profile_id"],
                    urn=row["urn"],
                    created_at=row["created_at"],
                    notes=row["notes"],
                )
                for row in rows
            ]

    def list_all_favorites(self) -> List[FavoriteConversation]:
        """
        List all favorited conversations across all profiles.

        Returns:
            List of FavoriteConversation records, ordered by created_at desc
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT profile_id, urn, created_at, notes FROM favorites ORDER BY created_at DESC"
            )
            rows = cursor.fetchall()

            return [
                FavoriteConversation(
                    profile_id=row["profile_id"],
                    urn=row["urn"],
                    created_at=row["created_at"],
                    notes=row["notes"],
                )
                for row in rows
            ]

    def get_favorite_urns(self, profile_id: str) -> set[str]:
        """
        Get all favorite URNs for a profile as a set for quick lookup.

        Args:
            profile_id: Profile/customer identifier

        Returns:
            Set of favorited conversation URNs
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT urn FROM favorites WHERE profile_id = ?", (profile_id,)
            )
            return {row["urn"] for row in cursor.fetchall()}

    def update_notes(self, profile_id: str, urn: str, notes: Optional[str]) -> bool:
        """
        Update notes for a favorite.

        Args:
            profile_id: Profile/customer identifier
            urn: Conversation URN
            notes: New notes (or None to clear)

        Returns:
            True if updated, False if not found
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                "UPDATE favorites SET notes = ? WHERE profile_id = ? AND urn = ?",
                (notes, profile_id, urn),
            )
            conn.commit()
            return cursor.rowcount > 0

    def count(self, profile_id: Optional[str] = None) -> int:
        """
        Get the total number of favorites.

        Args:
            profile_id: Optional profile to filter by

        Returns:
            Count of favorites
        """
        with self._get_connection() as conn:
            if profile_id:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM favorites WHERE profile_id = ?", (profile_id,)
                )
            else:
                cursor = conn.execute("SELECT COUNT(*) FROM favorites")
            return cursor.fetchone()[0]


# Singleton instance
_favorites_db: Optional[FavoritesDB] = None


def get_favorites_db() -> FavoritesDB:
    """Get or create the singleton FavoritesDB instance."""
    global _favorites_db
    if _favorites_db is None:
        _favorites_db = FavoritesDB()
    return _favorites_db
