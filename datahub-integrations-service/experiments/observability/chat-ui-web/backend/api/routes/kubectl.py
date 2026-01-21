"""
Kubectl Routes - API endpoints for kubectl operations.

Provides endpoints for listing kubectl contexts and namespaces.
"""

import json
import re
import sys
import time
from pathlib import Path
from typing import List, Literal, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from loguru import logger
from pydantic import BaseModel

# Import from parent directory
parent_dir = Path(__file__).parent.parent.parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from kubectl_manager import KubectlManager

# Import models
sys.path.insert(0, str(Path(__file__).parent.parent))
from api.models import ClusterIndexResponse, ClusterInfo

router = APIRouter(prefix="/api/kubectl", tags=["kubectl"])

# Hardcoded cluster contexts from .aws-accounts.yaml
CLOUD_CUSTOMER_CONTEXTS = [
    "arn:aws:eks:us-west-2:795586375822:cluster/usw2-saas-01-prod",
    "arn:aws:eks:us-west-2:795586375822:cluster/usw2-saas-01-staging",
    "arn:aws:eks:us-east-1:795586375822:cluster/use1-saas-01-prod",
    "arn:aws:eks:us-east-1:795586375822:cluster/use1-saas-01-poc",
    "arn:aws:eks:ap-southeast-2:795586375822:cluster/apse2-saas-01-prod",
    "arn:aws:eks:eu-central-1:795586375822:cluster/euc1-saas-01-prod",
]

FREE_TRIAL_CONTEXTS = [
    "arn:aws:eks:us-west-2:243536687406:cluster/usw2-trials-01-dmz",
    "arn:aws:eks:eu-central-1:243536687406:cluster/euc1-trials-01-dmz",
]

# Global cache for cluster index
_cluster_index_cache: Optional[Tuple[List[ClusterInfo], float, Optional[str], bool]] = (
    None
)
_CLUSTER_INDEX_TTL = 3600  # 1 hour for successful discoveries
_ERROR_CACHE_TTL = 300  # 5 minutes for errors (VPN failures, etc.)
_CACHE_VERSION = (
    4  # Increment when cache format changes (v4: detect AWS SSO authentication errors)
)

# Cache file location
_CACHE_DIR = Path.home() / ".datahub" / "chat_admin"
_CACHE_FILE = _CACHE_DIR / "k8s_cluster_cache.json"

# Ensure cache directory exists on module load
try:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Cluster cache directory: {_CACHE_DIR}")
except Exception as e:
    logger.warning(f"Failed to create cache directory: {e}")


class DiscoverRequest(BaseModel):
    """Request model for profile discovery."""

    context: str
    namespace: str


def _ensure_cache_dir() -> None:
    """Ensure cache directory exists (should already exist from module load)."""
    if not _CACHE_DIR.exists():
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _load_cache_from_disk() -> Optional[
    Tuple[List[ClusterInfo], float, Optional[str], bool]
]:
    """Load cluster cache from disk."""
    try:
        if not _CACHE_FILE.exists():
            return None

        with open(_CACHE_FILE, "r") as f:
            data = json.load(f)

        # Check cache version
        cache_version = data.get("version", 1)
        if cache_version != _CACHE_VERSION:
            logger.info(
                f"Disk cache version mismatch (found {cache_version}, expected {_CACHE_VERSION}), invalidating cache"
            )
            return None

        # Parse cached data
        clusters = [ClusterInfo(**cluster) for cluster in data.get("clusters", [])]
        cached_time = data.get("cached_time", 0)
        error_message = data.get("error_message")
        vpn_required = data.get("vpn_required", False)

        logger.info(
            f"Loaded {len(clusters)} clusters from disk cache (age: {time.time() - cached_time:.0f}s)"
        )
        return (clusters, cached_time, error_message, vpn_required)

    except Exception as e:
        logger.warning(f"Failed to load cluster cache from disk: {e}")
        return None


def _save_cache_to_disk(
    clusters: List[ClusterInfo],
    cached_time: float,
    error_message: Optional[str],
    vpn_required: bool,
) -> None:
    """Save cluster cache to disk."""
    try:
        _ensure_cache_dir()

        data = {
            "version": _CACHE_VERSION,
            "clusters": [cluster.model_dump() for cluster in clusters],
            "cached_time": cached_time,
            "error_message": error_message,
            "vpn_required": vpn_required,
        }

        with open(_CACHE_FILE, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(
            f"Saved {len(clusters)} clusters to disk cache (version {_CACHE_VERSION})"
        )

    except Exception as e:
        logger.warning(f"Failed to save cluster cache to disk: {e}")


def extract_customer_name(namespace: str) -> Optional[str]:
    """
    Extract customer name from namespace.

    Examples:
      "18ctce7lp6-chime" -> "chime"
      "figma-workspace" -> "figma"
      "abc123xyz" -> None (no recognizable name)
    """
    if "-" in namespace:
        parts = namespace.split("-")
        # If first part looks like a random ID (alphanumeric, 8+ chars), return rest
        if re.match(r"^[a-z0-9]{8,}$", parts[0]) and len(parts) > 1:
            return "-".join(parts[1:])

    # Return namespace as-is for now
    return namespace


def build_cluster_index(
    mode: str = "all",
) -> Tuple[List[ClusterInfo], Optional[str], bool]:
    """
    Build searchable index of clusters from hardcoded contexts.

    Steps:
    1. Select contexts based on mode (cloud vs trials)
    2. For each context:
       - Extract metadata (region, cluster name) from ARN
       - Get all namespaces in context
       - Parse namespace for customer name
    3. Return sorted list with error info

    Returns:
        Tuple of (clusters, error_message, vpn_required)
    """
    clusters = []
    vpn_timeout_count = 0
    context_not_found_count = 0
    sso_auth_error_count = 0
    total_contexts = 0

    # Select contexts based on mode
    if mode == "trials":
        contexts = FREE_TRIAL_CONTEXTS
    else:
        # "all" mode includes both cloud customers and free trials
        contexts = CLOUD_CUSTOMER_CONTEXTS + FREE_TRIAL_CONTEXTS

    total_contexts = len(contexts)
    kubectl = KubectlManager()

    for context in contexts:
        # Extract cluster info from ARN
        # ARN format: arn:aws:eks:{region}:{account}:cluster/{cluster_name}
        match = re.match(r"arn:aws:eks:([^:]+):\d+:cluster/(.+)", context)
        if not match:
            logger.warning(f"Failed to parse context ARN: {context}")
            continue

        region = match.group(1)
        cluster_name = match.group(2)

        # Get all namespaces
        try:
            namespaces = kubectl.get_namespaces(context=context)
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"Failed to get namespaces for {context}: {e}")

            # Track error types
            if "timed out" in error_msg and "VPN" in error_msg:
                vpn_timeout_count += 1
            elif "context was not found" in error_msg:
                context_not_found_count += 1
            elif "SSO session" in error_msg and "expired" in error_msg:
                sso_auth_error_count += 1

            continue

        for namespace in namespaces:
            # Parse namespace for customer name
            customer_name = extract_customer_name(namespace)

            # Determine environment and trial status
            is_trial = "trials" in cluster_name
            if "prod" in cluster_name:
                cluster_env = "prod"
            elif "staging" in cluster_name:
                cluster_env = "staging"
            elif "poc" in cluster_name:
                cluster_env = "poc"
            else:
                cluster_env = None

            clusters.append(
                ClusterInfo(
                    context=context,
                    context_name=cluster_name,
                    namespace=namespace,
                    customer_name=customer_name,
                    cluster_region=region,
                    cluster_env=cluster_env,
                    is_trial=is_trial,
                )
            )

    # Sort by customer name, then namespace
    clusters.sort(key=lambda c: (c.customer_name or c.namespace, c.namespace))

    # Determine error message
    error_message = None
    vpn_required = False

    if len(clusters) == 0:
        if vpn_timeout_count >= total_contexts // 2:  # More than half failed due to VPN
            error_message = (
                "⚠️ Not connected to VPN. Please connect to VPN to discover clusters."
            )
            vpn_required = True
        elif (
            sso_auth_error_count >= total_contexts // 2
        ):  # More than half failed due to SSO
            error_message = "⚠️ AWS SSO session expired. Run 'aws sso login' for the required AWS profile."
            vpn_required = False
        elif vpn_timeout_count > 0:
            error_message = f"⚠️ Some clusters unavailable ({vpn_timeout_count} contexts timed out). Please connect to VPN."
            vpn_required = True
        elif sso_auth_error_count > 0:
            error_message = f"⚠️ AWS SSO session expired for {sso_auth_error_count} contexts. Run 'aws sso login'."
            vpn_required = False
        elif context_not_found_count > 0:
            error_message = f"⚠️ {context_not_found_count} kubectl contexts not configured. Run 'aws eks update-kubeconfig' to add them."
        else:
            error_message = "No clusters found. Check your kubectl configuration."

    return clusters, error_message, vpn_required


def get_cluster_index_cached(
    mode: str = "all",
    force_refresh: bool = False,
) -> Tuple[List[ClusterInfo], bool, Optional[int], Optional[str], bool]:
    """
    Get cluster index with caching (in-memory + disk).

    Success caches for 1 hour, errors cache for 5 minutes.
    Only successful discoveries are persisted to disk.

    Returns:
        Tuple of (clusters, cached, cache_age_seconds, error_message, vpn_required)
    """
    global _cluster_index_cache

    current_time = time.time()

    # Skip cache if force refresh requested
    if force_refresh:
        logger.info("Force refresh requested, bypassing cache")
    # Try in-memory cache first
    elif _cluster_index_cache is not None:
        clusters, cached_time, error_message, vpn_required = _cluster_index_cache
        age = current_time - cached_time

        # Use shorter TTL for errors (VPN failures, etc.)
        cache_ttl = _ERROR_CACHE_TTL if error_message else _CLUSTER_INDEX_TTL

        if age < cache_ttl:
            # Cache hit - filter by mode
            filtered = [
                c
                for c in clusters
                if mode == "all" or (mode == "trials" and c.is_trial)
            ]

            # If filtered result is empty but full cache isn't, generate mode-specific error
            filtered_error = error_message
            if len(filtered) == 0 and len(clusters) > 0 and mode == "trials":
                filtered_error = "⚠️ AWS SSO session expired for trial clusters. Run 'aws sso login' with the trial AWS profile."

            return filtered, True, int(age), filtered_error, vpn_required

    # In-memory cache miss - try loading from disk (only for successful discoveries)
    # Skip disk cache if force refresh requested
    disk_cache = None if force_refresh else _load_cache_from_disk()
    if disk_cache is not None:
        clusters, cached_time, error_message, vpn_required = disk_cache
        age = current_time - cached_time

        # Only use disk cache if it's a successful discovery (has clusters)
        if len(clusters) > 0 and age < _CLUSTER_INDEX_TTL:
            # Disk cache is still valid - load into memory
            logger.info(f"Using disk cache (age: {age:.0f}s, {len(clusters)} clusters)")
            _cluster_index_cache = disk_cache

            filtered = [
                c
                for c in clusters
                if mode == "all" or (mode == "trials" and c.is_trial)
            ]

            # If filtered result is empty but full cache isn't, generate mode-specific error
            filtered_error = error_message
            if len(filtered) == 0 and len(clusters) > 0 and mode == "trials":
                filtered_error = "⚠️ AWS SSO session expired for trial clusters. Run 'aws sso login' with the trial AWS profile."

            return filtered, True, int(age), filtered_error, vpn_required

    # Both caches expired or missing - rebuild index
    logger.info("Building cluster index (cache miss or expired)")
    clusters, error_message, vpn_required = build_cluster_index(mode="all")
    _cluster_index_cache = (clusters, current_time, error_message, vpn_required)

    # Only save successful discoveries to disk (not errors)
    if len(clusters) > 0:
        _save_cache_to_disk(clusters, current_time, error_message, vpn_required)
        logger.info(f"Saved {len(clusters)} clusters to disk cache")
    else:
        logger.info(f"Not saving to disk - empty result (error: {error_message})")

    filtered = [
        c for c in clusters if mode == "all" or (mode == "trials" and c.is_trial)
    ]

    # If filtered result is empty but full cache isn't, generate mode-specific error
    filtered_error = error_message
    if len(filtered) == 0 and len(clusters) > 0 and mode == "trials":
        filtered_error = "⚠️ AWS SSO session expired for trial clusters. Run 'aws sso login' with the trial AWS profile."

    return filtered, False, 0, filtered_error, vpn_required


def search_clusters(clusters: List[ClusterInfo], query: str) -> List[ClusterInfo]:
    """
    Filter clusters by search query.

    Searches:
    - Customer name (fuzzy match)
    - Namespace (contains)
    - Context name (contains)
    """
    if not query:
        return clusters

    query_lower = query.lower()

    def matches(cluster: ClusterInfo) -> bool:
        # Search customer name
        if cluster.customer_name and query_lower in cluster.customer_name.lower():
            return True

        # Search namespace
        if query_lower in cluster.namespace.lower():
            return True

        # Search context name
        if query_lower in cluster.context_name.lower():
            return True

        return False

    return [c for c in clusters if matches(c)]


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
        raise HTTPException(
            status_code=500, detail=f"Failed to list contexts: {str(e)}"
        )


@router.get("/namespaces", response_model=List[str])
async def list_namespaces(
    context: str = Query(None, description="Kubectl context to use"),
):
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
        logger.info(
            f"Listed {len(namespaces)} namespaces for context: {context or 'current'}"
        )
        return namespaces

    except Exception as e:
        logger.error(f"Failed to list namespaces: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to list namespaces: {str(e)}"
        )


@router.get("/clusters", response_model=ClusterIndexResponse)
async def list_all_clusters(
    mode: Literal["all", "trials"] = Query(
        "all", description="all = cloud customers, trials = free trials only"
    ),
    search: Optional[str] = Query(
        None, description="Search query (namespace or customer name)"
    ),
    force_refresh: bool = Query(False, description="Force refresh, bypass cache"),
) -> ClusterIndexResponse:
    """
    List all clusters across all contexts and namespaces.

    Builds a searchable index of clusters with metadata.
    Results are cached for 1 hour to avoid expensive kubectl operations.

    Args:
        mode: Filter mode - "all" for cloud customers, "trials" for free trials only
        search: Optional search query to filter results

    Returns:
        List of clusters with context, namespace, and metadata
    """
    try:
        # Get clusters from cache
        clusters, cached, cache_age, error_message, vpn_required = (
            get_cluster_index_cached(mode=mode, force_refresh=force_refresh)
        )

        # Apply search filter if provided
        if search:
            clusters = search_clusters(clusters, search)

        logger.info(
            f"Listed {len(clusters)} clusters (mode={mode}, search={search}, cached={cached}, cache_age={cache_age}s)"
        )

        return ClusterIndexResponse(
            clusters=clusters,
            total=len(clusters),
            mode=mode,
            cached=cached,
            cache_age_seconds=cache_age if cached else None,
            error=error_message,
            vpn_required=vpn_required,
        )

    except Exception as e:
        logger.error(f"Failed to list clusters: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to list clusters: {str(e)}"
        )


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
        token = kubectl.get_datahub_token(
            request.namespace, frontend_url, request.context
        )
        if not token:
            raise HTTPException(status_code=500, detail="Failed to generate token")

        logger.info(
            f"Auto-discovered profile: {gms_url} (context={request.context}, namespace={request.namespace})"
        )

        return {
            "gms_url": gms_url,
            "gms_token": token,
            "context": request.context,
            "namespace": request.namespace,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to discover profile: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to discover profile: {str(e)}"
        )
