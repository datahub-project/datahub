"""
Kubectl Routes - API endpoints for kubectl operations.

Provides endpoints for listing kubectl contexts and namespaces.
"""

import sys
from pathlib import Path
from typing import List

from fastapi import APIRouter, HTTPException, Query
from loguru import logger
from pydantic import BaseModel

# Import from parent directory
parent_dir = Path(__file__).parent.parent.parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from kubectl_manager import KubectlManager

router = APIRouter(prefix="/api/kubectl", tags=["kubectl"])


class DiscoverRequest(BaseModel):
    """Request model for profile discovery."""

    context: str
    namespace: str


@router.get("/contexts", response_model=List[str])
async def list_contexts():
    """
    Get list of available kubectl contexts.

    Returns:
        List of context names
    """
    try:
        kubectl = KubectlManager()
        contexts = kubectl.get_contexts()
        logger.info(f"Listed {len(contexts)} kubectl contexts")
        return contexts

    except Exception as e:
        logger.error(f"Failed to list kubectl contexts: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list contexts: {str(e)}")


@router.get("/namespaces", response_model=List[str])
async def list_namespaces(context: str = Query(None, description="Kubectl context to use")):
    """
    Get list of namespaces in the cluster.

    Args:
        context: Optional kubectl context to use (default: current context)

    Returns:
        List of namespace names
    """
    try:
        kubectl = KubectlManager()
        namespaces = kubectl.get_namespaces(context=context)
        logger.info(f"Listed {len(namespaces)} namespaces for context: {context or 'current'}")
        return namespaces

    except Exception as e:
        logger.error(f"Failed to list namespaces: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list namespaces: {str(e)}")


@router.post("/discover")
async def discover_profile(request: DiscoverRequest):
    """
    Auto-discover GMS URL and generate token from kubectl context and namespace.

    Args:
        request: Discovery request with context and namespace

    Returns:
        Discovered GMS URL and generated token
    """
    try:
        kubectl = KubectlManager()

        # Discover GMS URL
        gms_url = kubectl.get_gms_url_from_namespace(request.namespace, request.context)
        if not gms_url:
            raise HTTPException(
                status_code=404,
                detail=f"Could not discover GMS URL in namespace '{request.namespace}'. Make sure DataHub is deployed.",
            )

        # Extract frontend URL from GMS URL for token generation
        frontend_url = gms_url.replace("/api/gms", "").replace("/gms", "")

        # Generate token
        token = kubectl.get_datahub_token(request.namespace, frontend_url, request.context)
        if not token:
            raise HTTPException(status_code=500, detail="Failed to generate token")

        logger.info(f"Auto-discovered profile: {gms_url} (context={request.context}, namespace={request.namespace})")

        return {"gms_url": gms_url, "gms_token": token, "context": request.context, "namespace": request.namespace}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to discover profile: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to discover profile: {str(e)}")
