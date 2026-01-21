"""
Configuration Routes - API endpoints for managing connection configuration.
"""

import json
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

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

from api.dependencies import (
    get_chat_engine,
    get_config,
    get_connection_manager,
    update_chat_engine_config,
)
from api.models import (
    ConnectionConfigModel,
    ConnectionModeEnum,
)
from api.url_validator import validate_url
from connection_manager import (
    ConnectionConfig,
    ConnectionManager,
    ConnectionMode,
    ConnectionProfile,
)
from core.chat_engine import ChatEngine

router = APIRouter(prefix="/api/config", tags=["config"])


def convert_config_to_model(config: ConnectionConfig) -> ConnectionConfigModel:
    """
    Convert ConnectionConfig to ConnectionConfigModel.

    Args:
        config: ConnectionConfig instance

    Returns:
        ConnectionConfigModel for API response
    """
    return ConnectionConfigModel(
        mode=ConnectionModeEnum(config.mode.value),
        integrations_url=config.integrations_url,
        gms_url=config.gms_url,
        gms_token=config.gms_token,
        kube_namespace=config.kube_namespace,
        kube_context=config.kube_context,
        pod_name=config.pod_name,
        pod_label_selector=config.pod_label_selector,
        local_port=config.local_port,
        remote_port=config.remote_port,
        aws_region=config.aws_region,
        aws_profile=config.aws_profile,
        name=config.name,
        description=config.description,
    )


def convert_model_to_config(model: ConnectionConfigModel) -> ConnectionConfig:
    """
    Convert ConnectionConfigModel to ConnectionConfig.

    Args:
        model: ConnectionConfigModel from API request

    Returns:
        ConnectionConfig instance
    """
    return ConnectionConfig(
        mode=ConnectionMode(model.mode.value),
        integrations_url=model.integrations_url,
        gms_url=model.gms_url,
        gms_token=model.gms_token,
        kube_namespace=model.kube_namespace,
        kube_context=model.kube_context,
        pod_name=model.pod_name,
        pod_label_selector=model.pod_label_selector,
        local_port=model.local_port,
        remote_port=model.remote_port,
        aws_region=model.aws_region,
        aws_profile=model.aws_profile,
        name=model.name,
        description=model.description,
    )


@router.get("", response_model=ConnectionConfigModel)
async def get_current_config(config: ConnectionConfig = Depends(get_config)):
    """
    Get the current connection configuration.

    Args:
        config: Current configuration (injected)

    Returns:
        Current configuration
    """
    return convert_config_to_model(config)


@router.post("", response_model=ConnectionConfigModel)
async def update_config(
    config_model: ConnectionConfigModel,
    manager: ConnectionManager = Depends(get_connection_manager),
):
    """
    Update the connection configuration.

    Args:
        config_model: New configuration
        manager: ConnectionManager instance (injected)

    Returns:
        Updated configuration
    """
    new_config = convert_model_to_config(config_model)

    # Save global connection mode separately (not part of profile)
    manager.set_connection_mode(new_config.mode.value)
    logger.info(f"Set global connection mode: {new_config.mode}")

    # Save configuration to disk using ConnectionManager
    from connection_manager import ConnectionProfile

    profile = ConnectionProfile(
        name="default",  # Use a default profile name for web UI
        config=new_config,
    )

    if manager.save_profile(profile):
        # Set as active profile
        manager.set_active_profile("default")
        logger.info("Saved configuration to profile 'default'")
    else:
        logger.warning("Failed to save configuration profile")

    # Update the global chat engine config
    update_chat_engine_config(new_config)

    logger.info(f"Updated configuration to mode: {new_config.mode}")

    return config_model


@router.post("/test")
async def test_connection(config: ConnectionConfig = Depends(get_config)):
    """
    Test the current connection configuration.

    Args:
        config: Current configuration (injected)

    Returns:
        Test result
    """
    try:
        logger.info(
            f"Testing connection for mode: {config.mode} (type: {type(config.mode)}, repr: {repr(config.mode)})"
        )
        logger.info(
            f"ConnectionMode.EMBEDDED = {ConnectionMode.EMBEDDED} (type: {type(ConnectionMode.EMBEDDED)})"
        )
        logger.info(f"Comparison result: {config.mode == ConnectionMode.EMBEDDED}")

        # Test based on mode
        if config.mode == ConnectionMode.EMBEDDED:
            # Test DataHub connection using direct HTTP request
            # (DataHub SDK may manipulate the URL incorrectly)
            import requests

            # Validate URL to prevent SSRF
            validate_url(config.gms_url)

            headers = {
                "Authorization": f"Bearer {config.gms_token}",
                "Content-Type": "application/json",
            }

            response = requests.get(
                f"{config.gms_url}/config", headers=headers, timeout=10
            )

            if response.status_code == 200:
                return {
                    "success": True,
                    "message": f"Successfully connected to {config.gms_url}",
                }
            elif response.status_code == 401:
                return {
                    "success": False,
                    "error": "Unauthorized - token may be expired. Please regenerate token.",
                }
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text[:200]}",
                }

        elif config.mode in [
            ConnectionMode.QUICKSTART,
            ConnectionMode.LOCAL_SERVICE,
            ConnectionMode.REMOTE,
            ConnectionMode.LOCAL,
            ConnectionMode.CUSTOM,
        ]:
            # Test HTTP connection to integrations service
            import requests

            if not config.integrations_url:
                return {
                    "success": False,
                    "error": "Integrations service URL is not configured",
                }

            logger.info(
                f"Testing connection to integrations service: {config.integrations_url}"
            )

            # Validate URL to prevent SSRF
            validate_url(config.integrations_url)

            response = requests.get(
                f"{config.integrations_url}/ping",
                timeout=5,
            )

            if response.status_code == 200:
                return {
                    "success": True,
                    "message": f"Successfully connected to integrations service at {config.integrations_url}",
                }
            else:
                return {
                    "success": False,
                    "error": f"Integrations service returned status {response.status_code}",
                }

        elif config.mode == ConnectionMode.GRAPHQL_DIRECT:
            return {
                "success": False,
                "error": "GraphQL Direct mode is not yet implemented",
            }

        else:
            return {
                "success": False,
                "error": f"Unknown connection mode: {config.mode}",
            }

    except Exception as e:
        logger.error(f"Connection test failed: {e}", exc_info=True)
        return {
            "success": False,
            "error": f"Connection test failed: {str(e)}",
        }


@router.get("/discover/contexts")
async def list_kubectl_contexts():
    """
    List available kubectl contexts.

    Returns:
        List of contexts or error
    """
    try:
        # Import kubectl manager
        parent_dir = Path(__file__).parent.parent.parent.parent.parent
        if str(parent_dir) not in sys.path:
            sys.path.insert(0, str(parent_dir))

        from kubectl_manager import KubectlManager

        # Create kubectl manager
        kubectl = KubectlManager()

        # Get all contexts
        contexts = kubectl.get_contexts()
        if not contexts:
            return {"error": "No kubectl contexts available"}

        # Get current context
        current_context = kubectl.get_current_context()

        logger.info(
            f"Found {len(contexts)} kubectl contexts, current: {current_context}"
        )
        return {"contexts": contexts, "current_context": current_context}

    except Exception as e:
        logger.error(f"Failed to list contexts: {e}")
        return {"error": f"Failed to list contexts: {str(e)}"}


@router.post("/discover/namespaces")
async def list_kubectl_namespaces(request: dict = None):
    """
    List available kubectl namespaces for discovery.

    Args:
        request: Optional dict with context field to list namespaces from specific context

    Returns:
        List of namespaces or error
    """
    try:
        # Import kubectl manager
        parent_dir = Path(__file__).parent.parent.parent.parent.parent
        if str(parent_dir) not in sys.path:
            sys.path.insert(0, str(parent_dir))

        from kubectl_manager import KubectlManager

        # Create kubectl manager
        kubectl = KubectlManager()

        # Check if specific context was requested
        requested_context = None
        if request:
            requested_context = request.get("context")

        # Get context to use
        if requested_context:
            context = requested_context
            logger.info(f"Listing namespaces for requested context: {context}")
        else:
            context = kubectl.get_current_context()
            if not context:
                return {"error": "No kubectl context available"}
            logger.info(f"Listing namespaces for current context: {context}")

        # Get namespaces
        namespaces = kubectl.get_namespaces(context)
        if not namespaces:
            return {"error": f"No namespaces found in context {context}"}

        logger.info(f"Found {len(namespaces)} namespaces in context {context}")
        return {"namespaces": namespaces, "context": context}

    except Exception as e:
        logger.error(f"Failed to list namespaces: {e}")
        return {"error": f"Failed to list namespaces: {str(e)}"}


@router.post("/discover")
async def discover_gms_url(request: dict = None):
    """
    Discover the DataHub GMS URL from kubectl.

    Args:
        request: Optional dict with context and namespace fields

    Returns:
        Discovery result with GMS URL or error
    """
    try:
        # Import kubectl manager
        parent_dir = Path(__file__).parent.parent.parent.parent.parent
        if str(parent_dir) not in sys.path:
            sys.path.insert(0, str(parent_dir))

        from kubectl_manager import KubectlManager

        # Create kubectl manager
        kubectl = KubectlManager()

        # Check if specific context was requested
        requested_context = None
        requested_namespace = None
        if request:
            requested_context = request.get("context")
            requested_namespace = request.get("namespace")

        # Get context to use
        if requested_context:
            context = requested_context
            logger.info(f"Using requested context: {context}")
        else:
            context = kubectl.get_current_context()
            if not context:
                return {"error": "No kubectl context available"}
            logger.info(f"Using current context: {context}")

        if requested_namespace:
            # Try specific namespace
            logger.info(f"Discovering GMS URL from namespace: {requested_namespace}")
            gms_url = kubectl.get_gms_url_from_namespace(requested_namespace, context)
            if gms_url:
                logger.info(
                    f"Discovered GMS URL from namespace {requested_namespace}: {gms_url}"
                )
                return {
                    "gms_url": gms_url,
                    "namespace": requested_namespace,
                    "context": context,
                }
            return {
                "error": f"Could not discover GMS URL from namespace {requested_namespace} in context {context}"
            }
        else:
            # Try all namespaces (legacy behavior)
            namespaces = kubectl.get_namespaces(context)
            if not namespaces:
                return {"error": f"No namespaces found in context {context}"}

            # Try each namespace to find GMS URL
            for namespace in namespaces:
                gms_url = kubectl.get_gms_url_from_namespace(namespace, context)
                if gms_url:
                    logger.info(
                        f"Discovered GMS URL from namespace {namespace}: {gms_url}"
                    )
                    return {
                        "gms_url": gms_url,
                        "namespace": namespace,
                        "context": context,
                    }

            return {
                "error": f"Could not discover GMS URL from any namespace in context {context}"
            }

    except Exception as e:
        logger.error(f"GMS URL discovery failed: {e}")
        return {"error": f"Discovery failed: {str(e)}"}


@router.post("/token/generate")
async def generate_token(request: dict):
    """
    Generate a DataHub token using kubectl.

    Args:
        request: Request with gms_url field

    Returns:
        Generated token or error
    """
    try:
        gms_url = request.get("gms_url")
        if not gms_url:
            logger.error("Token generation: gms_url is required")
            return {"error": "gms_url is required"}

        logger.info(f"Token generation request for GMS URL: {gms_url}")

        # Import kubectl manager
        parent_dir = Path(__file__).parent.parent.parent.parent.parent
        if str(parent_dir) not in sys.path:
            sys.path.insert(0, str(parent_dir))

        from kubectl_manager import KubectlManager

        # Create kubectl manager
        kubectl = KubectlManager()

        # Get current context
        context = kubectl.get_current_context()
        if not context:
            logger.error("Token generation: No kubectl context available")
            return {"error": "No kubectl context available"}

        logger.info(f"Using kubectl context: {context}")

        # Extract frontend URL from gms_url
        # GMS URL is like https://example.com/api/gms -> frontend is https://example.com
        frontend_url = gms_url.replace("/api/gms", "").replace("/gms", "")
        logger.info(f"Extracted frontend URL: {frontend_url}")

        # Try to find namespace from context
        namespaces = kubectl.get_namespaces(context)
        if not namespaces:
            logger.error("Token generation: No namespaces found in current context")
            return {"error": "No namespaces found in current context"}

        logger.info(f"Available namespaces: {namespaces}")

        # Try to find the correct namespace from frontend URL
        # Frontend URL format: https://dev02.usw2-saas-01-staging.acryl.io
        # Namespace format: c1b8405833-dev02
        # Extract the environment identifier (e.g., "dev02", "dev01", etc.)
        import re

        url_match = re.search(r"https?://([^.]+)", frontend_url)
        if url_match:
            env_id = url_match.group(1)  # e.g., "dev02"
            logger.info(f"Extracted environment ID from URL: {env_id}")

            # Find namespace that contains this environment ID
            matching_namespace = None
            for ns in namespaces:
                if env_id in ns:
                    matching_namespace = ns
                    logger.info(f"Found matching namespace: {ns}")
                    break

            if matching_namespace:
                namespace = matching_namespace
            else:
                logger.warning(
                    f"Could not find namespace matching {env_id}, using first namespace"
                )
                namespace = namespaces[0]
        else:
            logger.warning(f"Could not extract environment ID from URL: {frontend_url}")
            namespace = namespaces[0]

        logger.info(f"Attempting token generation for namespace: {namespace}")

        token = kubectl.get_datahub_token(namespace, frontend_url, context)

        if token:
            logger.info("Successfully generated token")
            return {"token": token}
        else:
            logger.error("Token generation returned None")
            return {
                "error": "Could not generate token - check backend logs for details"
            }

    except Exception as e:
        logger.error(f"Token generation failed with exception: {e}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")

        # Check if it's an SSO error that should be surfaced
        error_msg = str(e)
        if "Token has expired" in error_msg or (
            "sso" in error_msg.lower() and "refresh failed" in error_msg.lower()
        ):
            return {
                "error": f"AWS SSO token expired. Please click 'Open AWS SSO Login' button first, then try again. Details: {error_msg}"
            }

        return {"error": f"Token generation failed: {error_msg}"}


@router.post("/test-transport")
async def test_transport(request: dict):
    """
    Test transport connectivity to integrations service.

    Args:
        request: Dict with 'integrations_url' and 'mode' fields

    Returns:
        Transport test result
    """
    try:
        integrations_url = request.get("integrations_url")
        mode = request.get("mode")

        if not integrations_url:
            return {"success": False, "error": "integrations_url is required"}

        logger.info(
            f"Testing transport connection to {integrations_url} (mode: {mode})"
        )

        # For EMBEDDED mode, there's no HTTP transport to test
        if mode == "EMBEDDED":
            return {
                "success": True,
                "message": "EMBEDDED mode uses direct Python agent (no HTTP transport)",
            }

        # Test connection with ping endpoint
        import requests

        # Validate URL to prevent SSRF
        validate_url(integrations_url)

        response = requests.get(
            f"{integrations_url}/ping",
            timeout=5,
        )

        if response.status_code == 200:
            return {
                "success": True,
                "message": f"Transport connection successful! ({mode} mode, {integrations_url})",
            }
        else:
            return {
                "success": False,
                "error": f"Transport connection failed: HTTP {response.status_code} from {integrations_url}",
            }

    except requests.exceptions.Timeout:
        logger.error(f"Transport test timeout for {integrations_url}")
        return {
            "success": False,
            "error": f"Transport connection timeout. Make sure integrations service is running at {integrations_url}",
        }
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Transport test connection error: {e}")
        return {
            "success": False,
            "error": f"Transport connection failed: {str(e)}. Make sure integrations service is running at {integrations_url}",
        }
    except Exception as e:
        logger.error(f"Transport test failed: {e}", exc_info=True)
        return {"success": False, "error": f"Transport connection failed: {str(e)}"}


@router.post("/sso/login")
async def sso_login(
    request: dict = None, engine: ChatEngine = Depends(get_chat_engine)
):
    """
    Trigger AWS SSO login with browser handoff.
    Also saves the AWS profile to config for future use.

    Args:
        request: Optional dict with 'profile' field to specify which profile to use

    Returns:
        Result of SSO login trigger
    """
    import os
    import subprocess

    try:
        # Get profile from request body if provided
        profile_name = None
        if request:
            profile_name = request.get("profile")

        # Fall back to environment variable
        if not profile_name:
            profile_name = os.environ.get("AWS_PROFILE")

        # Fall back to auto-detection
        if not profile_name:
            # Try to find available profiles
            try:
                result = subprocess.run(
                    ["aws", "configure", "list-profiles"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0 and result.stdout.strip():
                    profiles = result.stdout.strip().split("\n")
                    # Look for acryl-related profiles first
                    acryl_profiles = [p for p in profiles if "acryl" in p.lower()]
                    if acryl_profiles:
                        profile_name = acryl_profiles[0]
                        logger.info(f"Found Acryl profile: {profile_name}")
                    else:
                        profile_name = profiles[0]
                        logger.info(f"Using first available profile: {profile_name}")
                else:
                    profile_name = "default"
            except Exception:
                profile_name = "default"

        logger.info(f"Starting AWS SSO login for profile: {profile_name}")

        # Start AWS SSO login process
        # Don't capture stdout/stderr - allow browser interaction
        proc = subprocess.Popen(
            ["aws", "sso", "login", "--profile", profile_name],
            start_new_session=True,
        )

        logger.info(f"Started AWS SSO login process with PID: {proc.pid}")

        # Save AWS profile to config for future use
        current_config = engine.config
        current_config.aws_profile = profile_name

        # Save to config manager
        manager = get_connection_manager()
        profile = ConnectionProfile(
            name="default",
            config=current_config,
        )

        success = manager.save_profile(profile)
        if success:
            manager.set_active_profile("default")
            logger.info(f"Saved AWS profile '{profile_name}' to config")

        # Update the global chat engine config
        update_chat_engine_config(current_config)

        return {
            "success": True,
            "message": f"AWS SSO login started for profile: {profile_name}",
            "profile": profile_name,
            "pid": proc.pid,
        }

    except FileNotFoundError:
        return {
            "success": False,
            "error": "AWS CLI not found. Please install AWS CLI first.",
        }
    except Exception as e:
        logger.error(f"SSO login failed: {e}")
        return {"success": False, "error": f"SSO login failed: {str(e)}"}


@router.get("/aws/profiles")
async def list_aws_profiles():
    """
    List available AWS profiles from ~/.aws/config.

    Returns:
        List of AWS profile names
    """
    try:
        result = subprocess.run(
            ["aws", "configure", "list-profiles"],
            capture_output=True,
            text=True,
            timeout=5,
        )

        if result.returncode == 0 and result.stdout.strip():
            profiles = result.stdout.strip().split("\n")
            logger.info(f"Found {len(profiles)} AWS profiles")
            return {
                "success": True,
                "profiles": profiles,
            }
        else:
            logger.warning("No AWS profiles found")
            return {
                "success": True,
                "profiles": [],
            }

    except FileNotFoundError:
        return {
            "success": False,
            "error": "AWS CLI not found. Please install AWS CLI first.",
            "profiles": [],
        }
    except Exception as e:
        logger.error(f"Failed to list AWS profiles: {e}")
        return {
            "success": False,
            "error": f"Failed to list AWS profiles: {str(e)}",
            "profiles": [],
        }


@router.get("/aws/profile-for-context")
async def detect_profile_for_context(context: str):
    """
    Detect which AWS profile is needed for a kubectl context.

    Extracts the AWS account ID from the cluster ARN and finds matching profiles.

    Args:
        context: Kubernetes context name (may be cluster ARN)

    Returns:
        Detection result with matching profiles and setup guidance
    """
    try:
        # Import AWS profile manager
        from core.aws_profile_manager import AwsProfileManager

        manager = AwsProfileManager()
        result = manager.detect_profile_for_context(context)

        response = {
            "account_id": result.account_id,
            "region": result.region,
            "matching_profiles": result.matching_profiles,
            "setup_needed": result.setup_needed,
            "recommended_profile": result.recommended_profile,
        }

        # Add account info if available
        if result.account_info:
            response["account_info"] = {
                "name": result.account_info.name,
                "description": result.account_info.description,
                "sso_start_url": result.account_info.sso_start_url,
                "sso_region": result.account_info.sso_region,
                "default_role": result.account_info.default_role,
                "suggested_profile_name": result.account_info.suggested_profile_name,
            }

        logger.info(
            f"Profile detection for context {context}: "
            f"account={result.account_id}, matches={len(result.matching_profiles)}, "
            f"setup_needed={result.setup_needed}"
        )

        return response

    except Exception as e:
        logger.error(f"Profile detection failed: {e}")
        return {
            "error": f"Profile detection failed: {str(e)}",
            "account_id": None,
            "matching_profiles": [],
            "setup_needed": False,
        }


@router.post("/aws/setup-profile")
async def setup_aws_profile(request: dict):
    """
    Guide user through AWS profile setup.

    Args:
        request: Dict with profile configuration:
            - profile_name: Name for the new profile
            - account_id: AWS account ID
            - sso_start_url: SSO start URL
            - sso_region: SSO region
            - role_name: IAM role name
            - region: Default AWS region (optional, defaults to us-west-2)

    Returns:
        Setup instructions or error
    """
    try:
        profile_name = request.get("profile_name")
        account_id = request.get("account_id")
        sso_start_url = request.get("sso_start_url")
        sso_region = request.get("sso_region")
        role_name = request.get("role_name")
        region = request.get("region", "us-west-2")

        if not all([profile_name, account_id, sso_start_url, sso_region, role_name]):
            return {
                "success": False,
                "error": "Missing required fields: profile_name, account_id, sso_start_url, sso_region, role_name",
            }

        # Import AWS profile manager
        from core.aws_profile_manager import AwsProfileManager

        manager = AwsProfileManager()
        success, instructions = manager.setup_profile(
            profile_name=profile_name,
            account_id=account_id,
            sso_start_url=sso_start_url,
            sso_region=sso_region,
            role_name=role_name,
            region=region,
        )

        if success:
            logger.info(f"Generated setup instructions for profile: {profile_name}")
            return {
                "success": True,
                "instructions": instructions,
                "profile_name": profile_name,
            }
        else:
            return {
                "success": False,
                "error": instructions,
            }

    except Exception as e:
        logger.error(f"Profile setup failed: {e}")
        return {
            "success": False,
            "error": f"Profile setup failed: {str(e)}",
        }


@router.post("/aws/validate-profile")
async def validate_aws_profile(request: dict):
    """
    Validate that an AWS profile exists and has valid credentials.

    Args:
        request: Dict with 'profile' field

    Returns:
        Validation result
    """
    try:
        profile_name = request.get("profile")
        if not profile_name:
            return {
                "valid": False,
                "error": "profile field is required",
            }

        # Import AWS profile manager
        from core.aws_profile_manager import AwsProfileManager

        manager = AwsProfileManager()
        valid, error = manager.validate_profile(profile_name)

        if valid:
            logger.info(f"Profile '{profile_name}' validated successfully")
            return {
                "valid": True,
                "profile": profile_name,
            }
        else:
            logger.warning(f"Profile '{profile_name}' validation failed: {error}")
            return {
                "valid": False,
                "error": error,
                "profile": profile_name,
            }

    except Exception as e:
        logger.error(f"Profile validation failed: {e}")
        return {
            "valid": False,
            "error": f"Profile validation failed: {str(e)}",
        }


# Session status cache: {profile_name: (status_dict, timestamp)}
_session_status_cache: dict[str, tuple[dict, datetime]] = {}
_cache_ttl = timedelta(minutes=5)


@router.get("/aws/session-status")
async def get_aws_session_status(profile: Optional[str] = None):
    """
    Check AWS SSO session status for a profile.

    Uses 5-minute caching to avoid excessive AWS STS calls.

    Args:
        profile: AWS profile name (optional, defaults to AWS_PROFILE env or auto-detect)

    Returns:
        Session status with account info, validity, and expiration
    """
    import os

    try:
        # Determine which profile to check
        profile_name = profile
        if not profile_name:
            profile_name = os.environ.get("AWS_PROFILE")

        # Auto-detect if still not set
        if not profile_name:
            try:
                result = subprocess.run(
                    ["aws", "configure", "list-profiles"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0 and result.stdout.strip():
                    profiles = result.stdout.strip().split("\n")
                    # Look for acryl-related profiles first
                    acryl_profiles = [p for p in profiles if "acryl" in p.lower()]
                    if acryl_profiles:
                        profile_name = acryl_profiles[0]
                    else:
                        profile_name = profiles[0]
            except Exception:
                profile_name = "default"

        if not profile_name:
            return {
                "valid": False,
                "error": "No AWS profile configured",
                "profile": None,
            }

        # Check cache first
        now = datetime.now()
        if profile_name in _session_status_cache:
            cached_status, cached_time = _session_status_cache[profile_name]
            if now - cached_time < _cache_ttl:
                age_seconds = (now - cached_time).total_seconds()
                logger.debug(
                    f"Using cached session status for {profile_name} (age: {age_seconds:.1f}s)"
                )
                cached_status["from_cache"] = True
                cached_status["cache_age_seconds"] = age_seconds
                return cached_status

        # Call AWS STS to get caller identity
        logger.debug(f"Checking AWS session status for profile: {profile_name}")
        result = subprocess.run(
            ["aws", "sts", "get-caller-identity", "--profile", profile_name],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0:
            # Parse the JSON response
            identity = json.loads(result.stdout)

            status = {
                "valid": True,
                "profile": profile_name,
                "account": identity.get("Account"),
                "user_id": identity.get("UserId"),
                "arn": identity.get("Arn"),
                "checked_at": now.isoformat(),
                "from_cache": False,
            }

            # Cache the successful result
            _session_status_cache[profile_name] = (status.copy(), now)
            logger.info(
                f"AWS session valid for profile '{profile_name}' (account: {status['account']})"
            )

            return status
        else:
            error = result.stderr.strip()
            logger.warning(f"AWS session invalid for profile '{profile_name}': {error}")

            # Check if it's an expired token error
            is_expired = "expired" in error.lower() or "token" in error.lower()

            status = {
                "valid": False,
                "profile": profile_name,
                "error": error,
                "expired": is_expired,
                "checked_at": now.isoformat(),
                "from_cache": False,
            }

            # Don't cache errors (they might be transient)
            return status

    except subprocess.TimeoutExpired:
        return {
            "valid": False,
            "error": "AWS CLI timeout - took too long to respond",
            "profile": profile_name,
        }
    except FileNotFoundError:
        return {
            "valid": False,
            "error": "AWS CLI not found. Please install AWS CLI first.",
            "profile": profile_name,
        }
    except Exception as e:
        logger.error(f"Session status check failed: {e}")
        return {
            "valid": False,
            "error": f"Session status check failed: {str(e)}",
            "profile": profile_name,
        }
