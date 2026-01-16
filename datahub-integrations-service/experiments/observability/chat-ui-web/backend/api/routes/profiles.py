"""
Profile Management Routes - API endpoints for managing DataHub instance profiles.

Profiles represent WHERE to connect (DataHub instance), separate from HOW to connect (connection mode).
"""

import base64
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import yaml
from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

# Import from parent directory
parent_dir = Path(__file__).parent.parent.parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Add backend directory to path
backend_dir = Path(__file__).parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from api.dependencies import get_connection_manager
from api.models import CreateProfileRequest, ProfileModel
from connection_manager import ConnectionManager

router = APIRouter(prefix="/api/profiles", tags=["profiles"])


def _decode_jwt_expiration(token: Optional[str]) -> Tuple[Optional[datetime], bool, bool]:
    """
    Decode JWT token and extract expiration info.

    Args:
        token: JWT token string

    Returns:
        Tuple of (expiration_datetime, is_expired, is_expiring_soon)
    """
    if not token:
        return None, False, False

    try:
        # JWT format: header.payload.signature
        parts = token.split('.')
        if len(parts) != 3:
            return None, False, False

        # Decode payload (second part)
        payload = parts[1]
        # Add padding if needed
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += '=' * padding

        decoded = json.loads(base64.urlsafe_b64decode(payload))

        # Extract expiration timestamp
        exp_timestamp = decoded.get('exp')
        if not exp_timestamp:
            return None, False, False

        exp_datetime = datetime.fromtimestamp(exp_timestamp)
        now = datetime.now()

        is_expired = now > exp_datetime
        is_expiring_soon = not is_expired and (exp_datetime - now) < timedelta(hours=24)

        return exp_datetime, is_expired, is_expiring_soon

    except Exception as e:
        logger.debug(f"Failed to decode JWT token: {e}")
        return None, False, False


def _read_datahubenv_profile() -> Optional[ProfileModel]:
    """
    Read profile from ~/.datahubenv file if it exists.

    Returns:
        ProfileModel from .datahubenv or None if file doesn't exist or is invalid
    """
    datahubenv_path = Path.home() / ".datahubenv"

    if not datahubenv_path.exists():
        return None

    try:
        with open(datahubenv_path, 'r') as f:
            config = yaml.safe_load(f)

        # Extract gms config
        gms_config = config.get('gms', {})
        gms_url = gms_config.get('server')
        gms_token = gms_config.get('token')

        if not gms_url:
            logger.warning("~/.datahubenv exists but has no gms.server configured")
            return None

        logger.info(f"Loaded profile from ~/.datahubenv: {gms_url}")

        # Decode token expiration
        exp_datetime, is_expired, is_expiring_soon = _decode_jwt_expiration(gms_token)

        return ProfileModel(
            name="datahubenv",
            description="From ~/.datahubenv file",
            gms_url=gms_url,
            gms_token=gms_token,
            is_active=False,  # Will be updated later if active
            is_readonly=True,
            source="datahubenv",
            token_expires_at=exp_datetime.isoformat() if exp_datetime else None,
            token_expired=is_expired,
            token_expiring_soon=is_expiring_soon,
        )

    except Exception as e:
        logger.error(f"Failed to read ~/.datahubenv: {e}")
        return None


@router.get("", response_model=List[ProfileModel])
async def list_profiles(manager: ConnectionManager = Depends(get_connection_manager)):
    """
    List all saved DataHub instance profiles.

    Returns:
        List of profiles
    """
    try:
        profile_names = manager.list_profiles()
        profiles = []
        active_profile_name = manager.get_active_profile()

        # Add regular profiles
        for name in profile_names:
            profile = manager.get_profile(name)
            if profile:
                # Decode token expiration
                exp_datetime, is_expired, is_expiring_soon = _decode_jwt_expiration(profile.config.gms_token)

                profiles.append(
                    ProfileModel(
                        name=profile.name,
                        description=profile.config.description,
                        gms_url=profile.config.gms_url,
                        gms_token=profile.config.gms_token,
                        kube_context=profile.config.kube_context,
                        kube_namespace=profile.config.kube_namespace,
                        is_active=(profile.name == active_profile_name),
                        is_readonly=False,
                        source="user",
                        token_expires_at=exp_datetime.isoformat() if exp_datetime else None,
                        token_expired=is_expired,
                        token_expiring_soon=is_expiring_soon,
                        created_at=profile.created_at,
                        updated_at=profile.updated_at,
                    )
                )

        # Add profile from ~/.datahubenv if it exists
        datahubenv_profile = _read_datahubenv_profile()
        if datahubenv_profile:
            datahubenv_profile.is_active = (active_profile_name == "datahubenv")
            profiles.append(datahubenv_profile)

        logger.info(f"Listed {len(profiles)} profiles (active: {active_profile_name})")
        return profiles

    except Exception as e:
        logger.error(f"Failed to list profiles: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list profiles: {str(e)}")


@router.get("/{profile_name}", response_model=ProfileModel)
async def get_profile(
    profile_name: str,
    manager: ConnectionManager = Depends(get_connection_manager),
):
    """
    Get a specific profile by name.

    Args:
        profile_name: Name of the profile

    Returns:
        Profile details
    """
    try:
        profile = manager.get_profile(profile_name)
        if not profile:
            raise HTTPException(status_code=404, detail=f"Profile '{profile_name}' not found")

        active_profile_name = manager.get_active_profile()

        # Decode token expiration
        exp_datetime, is_expired, is_expiring_soon = _decode_jwt_expiration(profile.config.gms_token)

        return ProfileModel(
            name=profile.name,
            description=profile.config.description,
            gms_url=profile.config.gms_url,
            gms_token=profile.config.gms_token,
            kube_context=profile.config.kube_context,
            kube_namespace=profile.config.kube_namespace,
            is_active=(profile.name == active_profile_name),
            is_readonly=(profile.name == "datahubenv"),
            source="datahubenv" if profile.name == "datahubenv" else "user",
            token_expires_at=exp_datetime.isoformat() if exp_datetime else None,
            token_expired=is_expired,
            token_expiring_soon=is_expiring_soon,
            created_at=profile.created_at,
            updated_at=profile.updated_at,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get profile: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get profile: {str(e)}")


@router.post("", response_model=ProfileModel)
async def create_or_update_profile(
    request: CreateProfileRequest,
    manager: ConnectionManager = Depends(get_connection_manager),
):
    """
    Create or update a DataHub instance profile.

    Args:
        request: Profile creation request

    Returns:
        Created/updated profile
    """
    try:
        from connection_manager import ConnectionConfig, ConnectionMode, ConnectionProfile

        # Create config with profile data (mode doesn't matter for profiles)
        config = ConnectionConfig(
            mode=ConnectionMode.QUICKSTART,  # Default, actual mode chosen separately
            gms_url=request.gms_url,
            gms_token=request.gms_token,
            kube_context=request.kube_context,
            kube_namespace=request.kube_namespace,
            description=request.description,
        )

        # Create profile
        profile = ConnectionProfile(
            name=request.name,
            config=config,
        )

        # Save profile
        if manager.save_profile(profile):
            # Reload to get timestamps
            saved_profile = manager.get_profile(request.name)
            if saved_profile:
                logger.info(f"Saved profile: {request.name}")
                return ProfileModel(
                    name=saved_profile.name,
                    description=saved_profile.config.description,
                    gms_url=saved_profile.config.gms_url,
                    gms_token=saved_profile.config.gms_token,
                    kube_context=saved_profile.config.kube_context,
                    kube_namespace=saved_profile.config.kube_namespace,
                    created_at=saved_profile.created_at,
                    updated_at=saved_profile.updated_at,
                )

        raise HTTPException(status_code=500, detail="Failed to save profile")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create/update profile: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to create/update profile: {str(e)}")


@router.delete("/{profile_name}")
async def delete_profile(
    profile_name: str,
    manager: ConnectionManager = Depends(get_connection_manager),
):
    """
    Delete a profile.

    Args:
        profile_name: Name of the profile to delete

    Returns:
        Success message
    """
    try:
        if manager.delete_profile(profile_name):
            logger.info(f"Deleted profile: {profile_name}")
            return {"success": True, "message": f"Deleted profile: {profile_name}"}
        else:
            raise HTTPException(status_code=404, detail=f"Profile '{profile_name}' not found")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete profile: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete profile: {str(e)}")


@router.post("/{profile_name}/test")
async def test_profile(profile_name: str, manager: ConnectionManager = Depends(get_connection_manager)):
    """
    Test if a profile's credentials are still valid.

    Args:
        profile_name: Name of the profile to test

    Returns:
        Test result
    """
    try:
        profile = manager.get_profile(profile_name)
        if not profile:
            raise HTTPException(status_code=404, detail=f"Profile '{profile_name}' not found")

        logger.info(f"Testing profile: {profile_name}")

        # Test DataHub connection
        from datahub.ingestion.graph.client import DataHubGraph
        from datahub.ingestion.graph.config import DatahubClientConfig

        graph_config = DatahubClientConfig(
            server=profile.config.gms_url,
            token=profile.config.gms_token,
        )
        graph = DataHubGraph(graph_config)

        # Try a simple query to verify connection
        _ = graph.get_config()

        logger.info(f"Profile '{profile_name}' test successful")
        return {
            "success": True,
            "message": f"Successfully connected to {profile.config.gms_url}",
            "profile": profile_name,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Profile test failed: {e}")
        return {
            "success": False,
            "error": f"Connection test failed: {str(e)}",
            "profile": profile_name,
        }


@router.post("/{profile_name}/activate")
async def activate_profile(
    profile_name: str,
    manager: ConnectionManager = Depends(get_connection_manager),
):
    """
    Set a profile as the active profile.

    Args:
        profile_name: Name of the profile to activate

    Returns:
        Success message
    """
    try:
        if manager.set_active_profile(profile_name):
            logger.info(f"Activated profile: {profile_name}")
            return {"success": True, "message": f"Activated profile: {profile_name}", "active_profile": profile_name}
        else:
            raise HTTPException(status_code=404, detail=f"Profile '{profile_name}' not found")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to activate profile: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to activate profile: {str(e)}")


@router.post("/{profile_name}/refresh-token")
async def refresh_profile_token(
    profile_name: str,
    force: bool = False,
    manager: ConnectionManager = Depends(get_connection_manager),
):
    """
    Refresh the GMS token for a profile using kubectl.

    Args:
        profile_name: Name of the profile
        force: Force regeneration even if cached token is still valid

    Returns:
        Updated profile with new token
    """
    try:
        profile = manager.get_profile(profile_name)
        if not profile:
            raise HTTPException(status_code=404, detail=f"Profile '{profile_name}' not found")

        # Check if kubectl info is available
        if not profile.config.kube_context or not profile.config.kube_namespace:
            raise HTTPException(
                status_code=400,
                detail="Profile does not have kubectl context/namespace configured. Cannot refresh token.",
            )

        logger.info(f"Refreshing token for profile: {profile_name}")

        # Import kubectl manager
        from kubectl_manager import KubectlManager

        kubectl = KubectlManager()

        # Extract frontend URL from gms_url
        frontend_url = profile.config.gms_url.replace("/api/gms", "").replace("/gms", "")
        logger.info(f"Extracted frontend URL: {frontend_url}")

        # Generate token
        token = kubectl.get_datahub_token(
            profile.config.kube_namespace,
            frontend_url,
            profile.config.kube_context,
            force_regenerate=force,
        )

        if token:
            # Update profile with new token
            profile.config.gms_token = token
            if manager.save_profile(profile):
                logger.info(f"Token refreshed for profile: {profile_name}")

                # Reload profile
                updated_profile = manager.get_profile(profile_name)
                if updated_profile:
                    return ProfileModel(
                        name=updated_profile.name,
                        description=updated_profile.config.description,
                        gms_url=updated_profile.config.gms_url,
                        gms_token=updated_profile.config.gms_token,
                        kube_context=updated_profile.config.kube_context,
                        kube_namespace=updated_profile.config.kube_namespace,
                        created_at=updated_profile.created_at,
                        updated_at=updated_profile.updated_at,
                    )

        raise HTTPException(status_code=500, detail="Failed to refresh token")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Token refresh failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

        # Check if it's an SSO error
        error_msg = str(e)
        if "Token has expired" in error_msg or ("sso" in error_msg.lower() and "refresh failed" in error_msg.lower()):
            raise HTTPException(
                status_code=401,
                detail=f"AWS SSO token expired. Please run AWS SSO login first. Details: {error_msg}",
            )

        raise HTTPException(status_code=500, detail=f"Token refresh failed: {str(e)}")
