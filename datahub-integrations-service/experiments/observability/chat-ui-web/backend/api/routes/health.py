"""
Health Check Routes - API endpoints for service health monitoring.
"""

import sys
from pathlib import Path
from datetime import datetime

from fastapi import APIRouter, Depends
from loguru import logger

# Import from parent directory
parent_dir = Path(__file__).parent.parent.parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Add backend directory to path
backend_dir = Path(__file__).parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from api.dependencies import get_config
from api.models import DataHubHealthResponse, HealthResponse
from api.url_validator import validate_url
from connection_manager import ConnectionConfig

router = APIRouter(prefix="/api/health", tags=["health"])


@router.get("", response_model=HealthResponse)
async def health_check():
    """
    Basic health check endpoint.

    Returns:
        Health status
    """
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(),
        details={"service": "datahub-chat-api", "version": "1.0.0"},
    )


@router.get("/datahub", response_model=DataHubHealthResponse)
async def datahub_health(config: ConnectionConfig = Depends(get_config)):
    """
    Check DataHub connection health.

    Args:
        config: Connection configuration (injected)

    Returns:
        DataHub connection status
    """
    try:
        # Try to create a simple test connection
        import requests

        # Simple health check - try to get server config
        # Use direct HTTP request since the DataHub SDK may manipulate the URL
        try:
            # Validate URL to prevent SSRF
            validate_url(config.gms_url)

            headers = {
                'Authorization': f'Bearer {config.gms_token}',
                'Content-Type': 'application/json'
            }

            response = requests.get(
                f'{config.gms_url}/config',
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                connected = True
                error = None
            elif response.status_code == 401:
                connected = False
                error = 'Unauthorized - token may be expired'
            else:
                connected = False
                error = f'HTTP {response.status_code}: {response.text[:100]}'

        except Exception as e:
            connected = False
            error = str(e)

        return DataHubHealthResponse(
            connected=connected,
            gms_url=config.gms_url,
            error=error,
            timestamp=datetime.now(),
        )

    except Exception as e:
        logger.error(f"DataHub health check failed: {e}")
        return DataHubHealthResponse(
            connected=False,
            gms_url=config.gms_url,
            error=str(e),
            timestamp=datetime.now(),
        )
