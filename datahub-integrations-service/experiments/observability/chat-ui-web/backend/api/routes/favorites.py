"""
Favorites Routes - API endpoints for managing bookmarked conversations.

Provides CRUD operations for favoriting conversations, partitioned by profile.
"""

import sys
from pathlib import Path
from typing import List, Optional
from urllib.parse import unquote

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger
from pydantic import BaseModel

# Add paths for imports
backend_dir = Path(__file__).parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from core.favorites import FavoriteConversation, FavoritesDB, get_favorites_db

router = APIRouter(prefix="/api/favorites", tags=["favorites"])


# Request/Response Models


class AddFavoriteRequest(BaseModel):
    """Request to add a favorite."""

    urn: str
    notes: Optional[str] = None


class UpdateNotesRequest(BaseModel):
    """Request to update favorite notes."""

    notes: Optional[str] = None


class FavoriteResponse(BaseModel):
    """Single favorite response."""

    profile_id: str
    urn: str
    created_at: str
    notes: Optional[str] = None


class FavoritesListResponse(BaseModel):
    """List of favorites response."""

    favorites: List[FavoriteResponse]
    count: int
    profile_id: str


class FavoriteUrnsResponse(BaseModel):
    """Set of favorite URNs for quick lookup."""

    urns: List[str]
    count: int
    profile_id: str


class FavoriteStatusResponse(BaseModel):
    """Check if a conversation is favorited."""

    urn: str
    is_favorite: bool
    profile_id: str


# Dependency to get current profile ID
def get_current_profile_id() -> str:
    """
    Get the current active profile ID.

    This reads from the connection manager to get the active profile name.
    Falls back to 'default' if no profile is active.
    """
    try:
        from connection_manager import ConnectionManager

        manager = ConnectionManager()
        config = manager.load_active_config()
        profile_name = manager.active_profile_name

        if profile_name:
            return profile_name

        # Fallback: use GMS URL as identifier if no profile name
        if config and config.gms_url:
            # Create a short identifier from the GMS URL
            return config.gms_url.replace("https://", "").replace("http://", "").split("/")[0]

        return "default"
    except Exception as e:
        logger.warning(f"Could not determine profile ID: {e}, using 'default'")
        return "default"


# Endpoints


@router.get("", response_model=FavoritesListResponse)
async def list_favorites(
    profile_id: Optional[str] = Query(None, description="Profile ID (uses active profile if not provided)"),
    db: FavoritesDB = Depends(get_favorites_db),
):
    """
    List all favorited conversations for the current profile.

    Returns:
        List of favorites with metadata
    """
    if not profile_id:
        profile_id = get_current_profile_id()

    favorites = db.list_favorites(profile_id)

    return FavoritesListResponse(
        favorites=[
            FavoriteResponse(
                profile_id=f.profile_id,
                urn=f.urn,
                created_at=f.created_at,
                notes=f.notes,
            )
            for f in favorites
        ],
        count=len(favorites),
        profile_id=profile_id,
    )


@router.get("/urns", response_model=FavoriteUrnsResponse)
async def get_favorite_urns(
    profile_id: Optional[str] = Query(None, description="Profile ID (uses active profile if not provided)"),
    db: FavoritesDB = Depends(get_favorites_db),
):
    """
    Get all favorite URNs as a list for quick filtering.

    This is optimized for the frontend to quickly check which conversations
    in a list are favorited without making individual requests.

    Returns:
        List of favorited URNs
    """
    if not profile_id:
        profile_id = get_current_profile_id()

    urns = db.get_favorite_urns(profile_id)

    return FavoriteUrnsResponse(
        urns=list(urns),
        count=len(urns),
        profile_id=profile_id,
    )


@router.get("/check/{urn:path}", response_model=FavoriteStatusResponse)
async def check_favorite(
    urn: str,
    profile_id: Optional[str] = Query(None, description="Profile ID (uses active profile if not provided)"),
    db: FavoritesDB = Depends(get_favorites_db),
):
    """
    Check if a specific conversation is favorited.

    Args:
        urn: Conversation URN (URL-encoded)
        profile_id: Profile ID (optional)

    Returns:
        Favorite status
    """
    if not profile_id:
        profile_id = get_current_profile_id()

    decoded_urn = unquote(urn)
    is_favorite = db.is_favorite(profile_id, decoded_urn)

    return FavoriteStatusResponse(
        urn=decoded_urn,
        is_favorite=is_favorite,
        profile_id=profile_id,
    )


@router.post("", response_model=FavoriteResponse)
async def add_favorite(
    request: AddFavoriteRequest,
    profile_id: Optional[str] = Query(None, description="Profile ID (uses active profile if not provided)"),
    db: FavoritesDB = Depends(get_favorites_db),
):
    """
    Add a conversation to favorites.

    Args:
        request: Contains URN and optional notes
        profile_id: Profile ID (optional)

    Returns:
        The created favorite
    """
    if not profile_id:
        profile_id = get_current_profile_id()

    favorite = db.add_favorite(profile_id, request.urn, request.notes)

    logger.info(f"Added favorite: {profile_id}/{request.urn}")

    return FavoriteResponse(
        profile_id=favorite.profile_id,
        urn=favorite.urn,
        created_at=favorite.created_at,
        notes=favorite.notes,
    )


@router.delete("/{urn:path}")
async def remove_favorite(
    urn: str,
    profile_id: Optional[str] = Query(None, description="Profile ID (uses active profile if not provided)"),
    db: FavoritesDB = Depends(get_favorites_db),
):
    """
    Remove a conversation from favorites.

    Args:
        urn: Conversation URN (URL-encoded)
        profile_id: Profile ID (optional)

    Returns:
        Success message
    """
    if not profile_id:
        profile_id = get_current_profile_id()

    decoded_urn = unquote(urn)
    removed = db.remove_favorite(profile_id, decoded_urn)

    if not removed:
        raise HTTPException(
            status_code=404,
            detail=f"Favorite not found: {decoded_urn}",
        )

    logger.info(f"Removed favorite: {profile_id}/{decoded_urn}")

    return {"success": True, "message": f"Removed favorite: {decoded_urn}"}


@router.patch("/{urn:path}/notes")
async def update_favorite_notes(
    urn: str,
    request: UpdateNotesRequest,
    profile_id: Optional[str] = Query(None, description="Profile ID (uses active profile if not provided)"),
    db: FavoritesDB = Depends(get_favorites_db),
):
    """
    Update notes for a favorited conversation.

    Args:
        urn: Conversation URN (URL-encoded)
        request: New notes
        profile_id: Profile ID (optional)

    Returns:
        Success message
    """
    if not profile_id:
        profile_id = get_current_profile_id()

    decoded_urn = unquote(urn)
    updated = db.update_notes(profile_id, decoded_urn, request.notes)

    if not updated:
        raise HTTPException(
            status_code=404,
            detail=f"Favorite not found: {decoded_urn}",
        )

    return {"success": True, "message": "Notes updated"}


@router.get("/count")
async def get_favorites_count(
    profile_id: Optional[str] = Query(None, description="Profile ID (uses active profile if not provided)"),
    db: FavoritesDB = Depends(get_favorites_db),
):
    """
    Get the count of favorites for a profile.

    Returns:
        Count of favorites
    """
    if not profile_id:
        profile_id = get_current_profile_id()

    count = db.count(profile_id)

    return {"count": count, "profile_id": profile_id}
