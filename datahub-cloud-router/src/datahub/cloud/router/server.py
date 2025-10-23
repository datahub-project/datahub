"""
FastAPI server for DataHub Cloud Router.
"""

import asyncio
import json
import logging
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import aiohttp
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .core import MultiTenantRouter
from .db.models import DeploymentType
from .security import SecurityManager

logger = logging.getLogger(__name__)


class ProductUpdatesService:
    """Service to download and cache product updates from multiple sources."""

    def __init__(self, cache_ttl: int = 600):  # 10 minutes default
        """Initialize the product updates service."""
        self._cache_ttl = cache_ttl
        self._cache: dict[str, dict[str, Any]] = {}  # Store multiple cached datasets
        self._last_fetch: dict[
            str, datetime
        ] = {}  # Track last fetch time for each source

        # Define update sources
        self._sources = {
            "oss": {
                "url": "https://raw.githubusercontent.com/datahub-project/datahub/master/metadata-service/configuration/src/main/resources/product-update.json",
                "fallback": {
                    "enabled": True,
                    "id": "v1.2.1",
                    "title": "What's New In DataHub",
                    "description": "Explore version v1.2.1",
                    "image": "https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/sample-product-update-image.png",
                    "ctaText": "Read updates",
                    "ctaLink": "https://docs.datahub.com/docs/releases#v1-2-1",
                },
            },
            "saas": {
                "url": "https://raw.githubusercontent.com/datahub-project/datahub/master/metadata-service/configuration/src/main/resources/product-update-saas.json",
                "fallback": {
                    "enabled": True,
                    "id": "v1.2.1",
                    "title": "What's New In DataHub",
                    "description": "Explore version v1.2.1",
                    "image": "https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/sample-product-update-image.png",
                    "ctaText": "Read updates",
                    "ctaLink": "https://docs.datahub.com/docs/releases#v1-2-1",
                },
            },
        }
        logger.info(f"Product updates service initialized with cache TTL: {cache_ttl}s")

    async def get_product_updates(self, source: str) -> dict[str, Any]:
        """Get product updates for a specific source with caching."""
        if source not in self._sources:
            raise ValueError(
                f"Unknown source: {source}. Available sources: {list(self._sources.keys())}"
            )

        now = datetime.now(timezone.utc)

        # Return cached data if still fresh
        if (
            source in self._cache
            and source in self._last_fetch
            and (now - self._last_fetch[source]).total_seconds() < self._cache_ttl
        ):
            logger.debug(f"Returning cached {source} product updates")
            return self._cache[source]

        # Download fresh data
        logger.info(f"Downloading fresh {source} product updates from GitHub")
        return await self._download_updates(source)

    async def _download_updates(self, source: str) -> dict[str, Any]:
        """Download product updates from GitHub for a specific source."""
        source_config = self._sources[source]

        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            ) as session:
                async with session.get(str(source_config["url"])) as response:
                    if response.status == 200:
                        try:
                            # Get text content first, then parse as JSON
                            text_content = await response.text()
                            data = json.loads(text_content)
                            # Ensure data is a dict
                            if isinstance(data, dict):
                                self._cache[source] = data
                                self._last_fetch[source] = datetime.now(timezone.utc)
                                logger.info(
                                    f"{source.upper()} product updates downloaded successfully"
                                )
                                return data
                            else:
                                logger.warning(
                                    f"Invalid JSON format from {source} updates"
                                )
                                return self._get_fallback_data(source)
                        except json.JSONDecodeError as json_error:
                            logger.warning(
                                f"Invalid JSON from {source} updates: {json_error}"
                            )
                            logger.debug(
                                f"Response content (first 200 chars): {text_content[:200]}"
                            )
                            return self._get_fallback_data(source)
                        except Exception as parse_error:
                            logger.warning(
                                f"Failed to parse {source} updates: {parse_error}"
                            )
                            return self._get_fallback_data(source)
                    else:
                        logger.warning(
                            f"Failed to download {source} updates: HTTP {response.status}"
                        )
                        return self._get_fallback_data(source)
        except asyncio.TimeoutError:
            logger.error(f"⏱️ Timeout downloading {source} product updates")
            return self._get_fallback_data(source)
        except Exception as e:
            logger.error(f"Error downloading {source} product updates: {e}")
            return self._get_fallback_data(source)

    def _get_fallback_data(self, source: str) -> dict[str, Any]:
        """Return fallback data when download fails."""
        logger.warning(f"Using fallback {source} product updates data")
        fallback = self._sources[source]["fallback"]
        # Ensure fallback is a dict
        if isinstance(fallback, dict):
            return fallback
        else:
            # This should never happen, but provide a safe fallback
            return {"error": "Invalid fallback data", "source": source}


class DataHubMultiTenantRouter:
    """FastAPI server for platform webhook handling and router management."""

    def __init__(
        self,
        router: MultiTenantRouter,
        admin_auth_enabled: bool = True,
        admin_api_key: Optional[str] = None,
        webhook_auth_enabled: bool = True,
        teams_app_id: Optional[str] = None,
        teams_app_password: Optional[str] = None,
        slack_signing_secret: Optional[str] = None,
        oauth_secret: Optional[str] = None,
    ):
        """Initialize the server with a router instance and security configuration."""
        self.router = router
        self.admin_auth_enabled = admin_auth_enabled
        self.admin_api_key = admin_api_key
        self.webhook_auth_enabled = webhook_auth_enabled

        # Generate a random API key if auth is enabled but no key provided
        if self.admin_auth_enabled and not self.admin_api_key:
            self.admin_api_key = secrets.token_urlsafe(32)
            logger.warning(
                f"🔑 Generated admin API key (set DATAHUB_ROUTER_ADMIN_API_KEY to persist): {self.admin_api_key}"
            )

        # Set up HTTP Bearer security for admin endpoints
        self.security = (
            HTTPBearer(auto_error=False) if self.admin_auth_enabled else None
        )

        # Create dependency for admin auth that can be used in route defaults
        self.admin_auth_dependency = (
            Depends(self.security) if self.security else Depends(lambda: None)
        )

        # Initialize security manager
        self.security_manager = SecurityManager(
            oauth_secret=oauth_secret, enable_rate_limiting=True
        )

        # Initialize product updates service
        self.product_updates_service = ProductUpdatesService(
            cache_ttl=600
        )  # 10 minutes

        # Register integration validators if credentials provided
        if webhook_auth_enabled:
            if teams_app_id and teams_app_password:
                try:
                    self.security_manager.register_teams_validator(
                        teams_app_id, teams_app_password
                    )
                except Exception as e:
                    logger.error(f"Failed to register Teams validator: {e}")
                    logger.warning("🚨 Teams webhooks will NOT be validated!")

            if slack_signing_secret:
                try:
                    self.security_manager.register_slack_validator(slack_signing_secret)
                except Exception as e:
                    logger.error(f"Failed to register Slack validator: {e}")
                    logger.warning("🚨 Slack webhooks will NOT be validated!")
        else:
            logger.warning("🚨 Webhook authentication is DISABLED!")

        self.app = FastAPI(
            title="DataHub Multi-Tenant Router",
            description="Multi-tenant platform integration router for DataHub",
            version="0.1.0",
        )
        self._setup_routes()
        self._setup_admin_routes()

    def _require_admin_auth(
        self, credentials: Optional[HTTPAuthorizationCredentials] = None
    ) -> None:
        """Dependency function to require admin authentication."""
        if not self.admin_auth_enabled:
            return  # Auth disabled, allow access

        if not credentials:
            raise HTTPException(
                status_code=401,
                detail="Admin API key required. Provide as: Authorization: Bearer <api_key>",
                headers={"WWW-Authenticate": "Bearer"},
            )

        if credentials.credentials != self.admin_api_key:
            raise HTTPException(
                status_code=401,
                detail="Invalid admin API key",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def _setup_routes(self):
        """Setup the main application routes."""

        @self.app.get("/health")
        async def health_check():
            """Health check endpoint."""
            # Get some basic stats
            instances = self.router.db.list_instances()
            unknown_tenants = self.router.db.get_unknown_tenants()

            return {
                "status": "healthy",
                "server": "datahub-multi-tenant-router",
                "version": "0.1.0",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "stats": {
                    "total_instances": len(instances),
                    "active_instances": len([i for i in instances if i.is_active]),
                    "unknown_tenants": len(unknown_tenants),
                    "default_target": self.router.default_target_url,
                },
            }

        @self.app.get("/update-oss/")
        async def get_oss_updates():
            """Serve OSS product updates from GitHub with caching."""
            try:
                updates = await self.product_updates_service.get_product_updates("oss")
                return JSONResponse(content=updates)
            except Exception as e:
                logger.error(f"Error serving OSS product updates: {e}")
                return JSONResponse(
                    status_code=500,
                    content={"error": "Failed to load OSS product updates"},
                )

        @self.app.get("/update-saas/")
        async def get_saas_updates():
            """Serve SaaS product updates from GitHub with caching."""
            try:
                updates = await self.product_updates_service.get_product_updates("saas")
                return JSONResponse(content=updates)
            except Exception as e:
                logger.error(f"Error serving SaaS product updates: {e}")
                return JSONResponse(
                    status_code=500,
                    content={"error": "Failed to load SaaS product updates"},
                )

        @self.app.get("/")
        async def root():
            """Root endpoint with server information."""
            default_instance = self.router.db.get_default_instance()

            return {
                "name": "DataHub Multi-Tenant Router",
                "version": "0.1.0",
                "description": "Multi-tenant platform integration router for DataHub",
                "status": "running",
                "default_target": self.router.default_target_url,
                "default_instance": {
                    "id": default_instance.id if default_instance else None,
                    "name": default_instance.name if default_instance else None,
                    "url": default_instance.url if default_instance else None,
                },
                "endpoints": {
                    "health": "/health",
                    "oss_updates": "/update-oss/",
                    "saas_updates": "/update-saas/",
                    "webhook": "/public/teams/webhook",
                    "bot_framework": "/api/messages",
                    "events": "/events",
                    "admin": "/admin/",
                },
                "admin_endpoints": {
                    "instances": "/admin/instances",
                    "tenant_mapping": "/admin/tenants/{tenant_id}/mapping",
                    "unknown_tenants": "/admin/unknown-tenants",
                    "routing_stats": "/admin/routing-stats",
                },
            }

        @self.app.post("/public/teams/webhook")
        async def teams_webhook(request: Request):
            """Handle Teams webhook events with security validation."""
            try:
                # Get raw body for validation
                body = await request.body()

                # Validate webhook using security manager
                if self.webhook_auth_enabled:
                    await self.security_manager.validate_webhook("teams", request, body)

                # Parse JSON after validation
                body_json = json.loads(body.decode())
                logger.debug(
                    f"Teams webhook received with payload keys: {list(body_json.keys())}"
                )

                # Extract relevant headers (especially Authorization for Bot Framework)
                auth_header = request.headers.get(
                    "Authorization", request.headers.get("authorization", "")
                )
                content_type = request.headers.get("Content-Type", "application/json")

                logger.debug(
                    f"Request headers - Authorization: {'present' if auth_header else 'missing'}, Content-Type: {content_type}"
                )

                # Prepare headers to forward
                forward_headers = {"Content-Type": content_type}
                if auth_header:
                    forward_headers["Authorization"] = auth_header

                # Extract tenant and team information
                tenant_id = body_json.get("tenantId") or body_json.get("tenant_id")

                # For Bot Framework messages, tenant ID might be in conversation
                if not tenant_id and "conversation" in body_json:
                    tenant_id = body_json["conversation"].get("tenantId")

                team_id = None

                # Try to extract team ID from various possible locations
                if "channelData" in body_json:
                    team_id = body_json["channelData"].get("team", {}).get("id")
                elif "team" in body_json:
                    team_id = body_json["team"].get("id")
                elif (
                    "conversation" in body_json
                    and "channelData" in body_json["conversation"]
                ):
                    team_id = (
                        body_json["conversation"]["channelData"]
                        .get("team", {})
                        .get("id")
                    )

                logger.info(f"Extracted tenant_id={tenant_id}, team_id={team_id}")

                if not tenant_id:
                    logger.warning(
                        f"Missing tenant ID in webhook payload with keys: {list(body_json.keys())}"
                    )
                    return JSONResponse(
                        status_code=400,
                        content={
                            "error": "Missing tenant ID in webhook payload",
                            "payload_keys": list(body_json.keys()),
                        },
                    )

                # Route the event with headers
                result = await self.router.route_event(
                    body_json, tenant_id, team_id, forward_headers
                )

                return JSONResponse(content=result)

            except Exception as e:
                return JSONResponse(
                    status_code=500,
                    content={"error": f"Error processing webhook: {str(e)}"},
                )

        @self.app.post("/api/messages")
        async def bot_framework_messages(request: Request):
            """Handle Bot Framework messages with security validation."""
            try:
                # Get raw body for validation
                body = await request.body()

                # Validate webhook using security manager
                if self.webhook_auth_enabled:
                    await self.security_manager.validate_webhook("teams", request, body)

                # Parse JSON after validation
                body_json = json.loads(body.decode())

                # Extract tenant and team information
                tenant_id = body_json.get("conversation", {}).get("tenantId")
                team_id = (
                    body_json.get("conversation", {})
                    .get("channelData", {})
                    .get("team", {})
                    .get("id")
                )

                if not tenant_id:
                    return JSONResponse(
                        status_code=400,
                        content={"error": "Missing tenant ID in Bot Framework message"},
                    )

                # Route the message
                result = await self.router.route_event(body_json, tenant_id, team_id)

                return JSONResponse(content=result)

            except Exception as e:
                return JSONResponse(
                    status_code=500,
                    content={
                        "error": f"Error processing Bot Framework message: {str(e)}"
                    },
                )

        @self.app.get("/events")
        async def view_events():
            """View recent routing events."""
            # This would typically read from a log file or database
            return {
                "message": "Event logging not implemented in this version",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        @self.app.get("/public/teams/oauth/callback")
        async def oauth_callback(request: Request):
            """Handle OAuth callback from Microsoft and route to correct DataHub instance"""
            try:
                # Extract query parameters from OAuth callback
                query_params = dict(request.query_params)

                print("\n🔐 OAUTH CALLBACK RECEIVED")
                print(f"{'=' * 60}")
                print(f"URL: {request.url}")
                print(f"Query parameters: {query_params}")

                # Check for OAuth error
                if "error" in query_params:
                    error = query_params.get("error")
                    error_description = query_params.get(
                        "error_description", "No description provided"
                    )
                    print(f"❌ OAuth error: {error} - {error_description}")

                    return JSONResponse(
                        {
                            "error": "oauth_error",
                            "oauth_error": error,
                            "oauth_error_description": error_description,
                            "message": f"OAuth authorization failed: {error}",
                        },
                        status_code=400,
                    )

                # Extract authorization code and state
                auth_code = query_params.get("code")
                state = query_params.get("state")

                if not auth_code:
                    print("❌ No authorization code in OAuth callback")
                    return JSONResponse(
                        {
                            "error": "missing_authorization_code",
                            "message": "No authorization code received from Microsoft",
                        },
                        status_code=400,
                    )

                if not state:
                    print("❌ No state parameter in OAuth callback")
                    return JSONResponse(
                        {
                            "error": "missing_state",
                            "message": "No state parameter received - cannot route callback",
                        },
                        status_code=400,
                    )

                # Decode and validate state using security manager
                try:
                    decoded_state = self.security_manager.validate_oauth_callback(state)
                    print(f"📍 Decoded state: {decoded_state}")

                    target_url = decoded_state.url
                    # In V1 model, use target_url as instance identifier since 'id' field doesn't exist
                    instance_id = target_url

                    if not target_url:
                        print("❌ No target URL in OAuth state")
                        return JSONResponse(
                            {
                                "error": "invalid_state",
                                "message": "OAuth state does not contain target URL",
                            },
                            status_code=400,
                        )

                except HTTPException as e:
                    print(f"❌ OAuth state validation failed: {e.detail}")
                    return JSONResponse(
                        {
                            "error": "invalid_state",
                            "message": e.detail,
                        },
                        status_code=e.status_code,
                    )

                # Forward OAuth callback to target DataHub instance
                try:
                    print(f"🔄 Forwarding OAuth callback to: {target_url}")

                    # Determine which endpoint to use based on flow type
                    flow_type = decoded_state.flow_type

                    # Both flow types go to integration service, but with different parameters
                    if flow_type == "personal_notifications":
                        # For personal notifications: route to integration service with personal flow params
                        # Integration service will extract user info and forward to frontend
                        # Replace port 3000 with 9003 for integration service
                        target_oauth_url = f"{target_url.rstrip('/').replace(':3000', ':9003')}/integrations/teams/oauth/complete"

                        # Extract additional parameters for personal notifications
                        user_urn = decoded_state.user_urn
                        redirect_path = decoded_state.redirect_path

                        oauth_params = {
                            "code": auth_code,
                            "state": state,
                            "flow_type": flow_type,
                        }

                        if user_urn:
                            oauth_params["user_urn"] = user_urn
                        if redirect_path:
                            oauth_params["redirect_url"] = (
                                f"{target_url.rstrip('/')}{redirect_path}"
                            )

                        print(
                            f"📱 Personal notifications OAuth - routing to integration service: {target_oauth_url}"
                        )
                        print(f"   User URN: {user_urn}")
                        print(f"   Redirect URL: {oauth_params.get('redirect_url')}")

                    else:
                        # For platform integration: route to integration service (existing behavior)
                        target_oauth_url = f"{target_url.rstrip('/').replace(':3000', ':9003')}/integrations/teams/oauth/complete"

                        # Build router registration URL for DataHub to call after successful OAuth
                        router_register_url = f"{request.url.scheme}://{request.url.netloc}/api/instances/register"

                        oauth_params = {
                            "code": auth_code,
                            "state": state,  # Pass the original encoded state, not just instance_id
                            "router_register_url": router_register_url,
                        }

                        print(
                            f"🏢 Platform integration OAuth - routing to integration service: {target_oauth_url}"
                        )

                    # Forward the OAuth callback parameters
                    async with aiohttp.ClientSession(
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as session:
                        async with (
                            session.get(
                                target_oauth_url,
                                params=oauth_params,
                                allow_redirects=False,  # Don't follow redirects automatically
                            ) as response
                        ):
                            response_text = await response.text()

                            print(
                                f"📡 Forwarded to {target_oauth_url} -> Status: {response.status}"
                            )

                            if response.status == 200 or response.status == 302:
                                print("✅ OAuth callback successfully forwarded")

                                # Auto-register tenant mapping after successful OAuth (only for platform integration)
                                if flow_type != "personal_notifications":
                                    tenant_id = decoded_state.tenant_id
                                    if tenant_id and instance_id:
                                        await self.auto_register_tenant_after_oauth(
                                            target_url, instance_id, tenant_id
                                        )
                                    else:
                                        print(
                                            "⚠️ No tenant ID found in OAuth state - skipping auto-registration"
                                        )

                                # Determine redirect URL based on flow type
                                if flow_type == "personal_notifications":
                                    # For personal notifications: extract teams_user_id from integration service response
                                    teams_user_id = None
                                    if response.status == 302:
                                        # Integration service returned a redirect - extract teams_user_id from Location header
                                        location_header = response.headers.get(
                                            "Location"
                                        )
                                        if (
                                            location_header
                                            and "teams_user_id=" in location_header
                                        ):
                                            from urllib.parse import parse_qs, urlparse

                                            parsed_url = urlparse(location_header)
                                            redirect_query_params = parse_qs(
                                                parsed_url.query
                                            )  # type: dict[str, list[str]]
                                            teams_user_id = redirect_query_params.get(
                                                "teams_user_id", [None]
                                            )[0]
                                            print(
                                                f"🎯 Extracted teams_user_id from integration service redirect: {teams_user_id}"
                                            )

                                    # Construct final redirect URL with teams_user_id if available
                                    redirect_path = (
                                        decoded_state.redirect_path
                                        or "/settings/personal-notifications"
                                    )
                                    redirect_url = f"{target_url.rstrip('/')}{redirect_path}?teams_oauth=success"
                                    if teams_user_id:
                                        redirect_url += (
                                            f"&teams_user_id={teams_user_id}"
                                        )
                                    print(
                                        f"📱 Personal notifications OAuth completed - redirecting to: {redirect_url}"
                                    )
                                else:
                                    # For platform integration: use the original Teams settings page
                                    redirect_url = f"{target_url.rstrip('/')}/settings/integrations/microsoft-teams?oauth=success"
                                    print(
                                        f"🏢 Platform integration OAuth completed - redirecting to: {redirect_url}"
                                    )

                                return RedirectResponse(
                                    url=redirect_url, status_code=302
                                )
                            else:
                                print(
                                    f"⚠️ Target instance returned error: {response.status} - {response_text}"
                                )

                                # Redirect back to appropriate settings page with error notification
                                error_message = f"OAuth configuration failed (HTTP {response.status})"

                                if flow_type == "personal_notifications":
                                    redirect_path = (
                                        decoded_state.redirect_path
                                        or "/settings/personal-notifications"
                                    )
                                    error_redirect_url = f"{target_url.rstrip('/')}{redirect_path}?teams_oauth=error&message={quote(error_message)}"
                                else:
                                    error_redirect_url = f"{target_url.rstrip('/')}/settings/integrations/microsoft-teams?oauth=error&message={quote(error_message)}"

                                print(
                                    f"🔄 Redirecting user back with error: {error_redirect_url}"
                                )

                                return RedirectResponse(
                                    url=error_redirect_url, status_code=302
                                )

                except asyncio.TimeoutError:
                    print(f"⏱️ Timeout forwarding OAuth callback to {target_url}")

                    # Redirect back to appropriate settings page with timeout error
                    error_message = "OAuth configuration timed out - DataHub instance not responding"

                    if flow_type == "personal_notifications":
                        redirect_path = (
                            decoded_state.redirect_path
                            or "/settings/personal-notifications"
                        )
                        timeout_redirect_url = f"{target_url.rstrip('/')}{redirect_path}?teams_oauth=error&message={quote(error_message)}"
                    else:
                        timeout_redirect_url = f"{target_url.rstrip('/')}/settings/integrations/microsoft-teams?oauth=error&message={quote(error_message)}"

                    print(
                        f"🔄 Redirecting user back with timeout error: {timeout_redirect_url}"
                    )

                    return RedirectResponse(url=timeout_redirect_url, status_code=302)
                except Exception as e:
                    print(f"❌ Error forwarding OAuth callback to {target_url}: {e}")

                    # Redirect back to appropriate settings page with connection error
                    error_message = (
                        f"OAuth configuration failed - connection error: {str(e)}"
                    )

                    if flow_type == "personal_notifications":
                        redirect_path = (
                            decoded_state.redirect_path
                            or "/settings/personal-notifications"
                        )
                        connection_error_url = f"{target_url.rstrip('/')}{redirect_path}?teams_oauth=error&message={quote(error_message)}"
                    else:
                        connection_error_url = f"{target_url.rstrip('/')}/settings/integrations/microsoft-teams?oauth=error&message={quote(error_message)}"

                    print(
                        f"🔄 Redirecting user back with connection error: {connection_error_url}"
                    )

                    return RedirectResponse(url=connection_error_url, status_code=302)

            except Exception as e:
                print(f"❌ Error handling OAuth callback: {e}")
                return JSONResponse(
                    {
                        "error": "internal_error",
                        "message": f"Internal server error: {e}",
                    },
                    status_code=500,
                )

    def _get_admin_credentials(self):
        """Get admin credentials from request, handling both auth enabled and disabled cases."""
        if self.security:
            # Use FastAPI dependency injection when auth is enabled
            return Depends(self.security)
        else:
            # Return None when auth is disabled
            return None

    def _setup_admin_routes(self):
        """Setup admin API routes."""

        @self.app.get("/admin/instances")
        async def get_all_instances(
            credentials: Optional[
                HTTPAuthorizationCredentials
            ] = self.admin_auth_dependency,
        ):
            """Get all DataHub instances."""
            self._require_admin_auth(credentials)
            instances = self.router.db.list_instances()
            return {
                "instances": [
                    {
                        "id": instance.id,
                        "name": instance.name,
                        "deployment_type": instance.deployment_type.value,
                        "url": instance.url,
                        "connection_id": instance.connection_id,
                        "api_token_present": bool(
                            instance.api_token
                        ),  # 🔒 Don't expose actual token
                        "health_check_url": instance.health_check_url,
                        "is_active": instance.is_active,
                        "is_default": instance.is_default,
                        "max_concurrent_requests": instance.max_concurrent_requests,
                        "timeout_seconds": instance.timeout_seconds,
                        "health_status": instance.health_status.value,
                        "created_at": instance.created_at,
                        "updated_at": instance.updated_at,
                        "last_health_check": instance.last_health_check,
                    }
                    for instance in instances
                ],
                "count": len(instances),
            }

        @self.app.post("/admin/instances")
        async def create_datahub_instance(
            request: Request,
            credentials: Optional[
                HTTPAuthorizationCredentials
            ] = self.admin_auth_dependency,
        ):
            """Create a new DataHub instance."""
            self._require_admin_auth(credentials)
            try:
                data = await request.json()

                instance = self.router.db.create_instance(
                    name=data["name"],
                    url=data["url"],
                    deployment_type=DeploymentType(
                        data.get("deployment_type", "cloud")
                    ),
                    connection_id=data.get("connection_id"),
                    api_token=data.get("api_token"),
                    health_check_url=data.get("health_check_url"),
                    is_active=data.get("is_active", True),
                    is_default=data.get("is_default", False),
                    max_concurrent_requests=data.get("max_concurrent_requests", 100),
                    timeout_seconds=data.get("timeout_seconds", 30),
                )

                return {
                    "status": "success",
                    "instance": {
                        "id": instance.id,
                        "name": instance.name,
                        "url": instance.url,
                        "deployment_type": instance.deployment_type.value,
                    },
                }

            except Exception as e:
                return JSONResponse(
                    status_code=400,
                    content={"error": f"Error creating instance: {str(e)}"},
                )

        @self.app.get("/admin/tenants/{tenant_id}/mapping")
        async def get_tenant_mapping(
            tenant_id: str,
            credentials: Optional[
                HTTPAuthorizationCredentials
            ] = self.admin_auth_dependency,
        ):
            """Get tenant mapping for a specific tenant."""
            self._require_admin_auth(credentials)
            mapping = self.router.db.get_mapping(tenant_id)
            if mapping:
                instance = self.router.db.get_instance(mapping.instance_id)
                return {
                    "tenant_id": tenant_id,
                    "mappings": [
                        {
                            "id": mapping.id,
                            "instance_id": mapping.instance_id,
                            "instance_name": instance.name if instance else "Unknown",
                            "team_id": mapping.team_id,
                            "created_by": mapping.created_by,
                            "created_at": mapping.created_at,
                        }
                    ],
                    "count": 1,
                }
            else:
                return {"tenant_id": tenant_id, "mappings": [], "count": 0}

        @self.app.post("/admin/tenants/{tenant_id}/mapping")
        async def create_tenant_mapping(
            tenant_id: str,
            request: Request,
            credentials: Optional[
                HTTPAuthorizationCredentials
            ] = self.admin_auth_dependency,
        ):
            """Create a tenant mapping."""
            self._require_admin_auth(credentials)
            try:
                data = await request.json()

                mapping = self.router.db.create_mapping(
                    tenant_id=tenant_id,
                    instance_id=data["instance_id"],
                    team_id=data.get("team_id"),
                    created_by=data.get("created_by", "admin"),
                )

                return {
                    "status": "success",
                    "mapping": {
                        "id": mapping.id,
                        "tenant_id": mapping.tenant_id,
                        "instance_id": mapping.instance_id,
                        "team_id": mapping.team_id,
                    },
                }

            except Exception as e:
                return JSONResponse(
                    status_code=400,
                    content={"error": f"Error creating mapping: {str(e)}"},
                )

        @self.app.get("/admin/unknown-tenants")
        async def get_unknown_tenants(
            credentials: Optional[
                HTTPAuthorizationCredentials
            ] = self.admin_auth_dependency,
        ):
            """Get unknown tenants for review."""
            self._require_admin_auth(credentials)
            unknown_tenants = self.router.db.get_unknown_tenants()
            return {
                "unknown_tenants": [
                    {
                        "id": tenant.id,
                        "tenant_id": tenant.tenant_id,
                        "team_id": tenant.team_id,
                        "first_seen": tenant.first_seen,
                        "last_seen": tenant.last_seen,
                        "event_count": tenant.event_count,
                    }
                    for tenant in unknown_tenants
                ],
                "count": len(unknown_tenants),
            }

        @self.app.get("/admin/routing-stats")
        async def get_routing_stats(
            credentials: Optional[
                HTTPAuthorizationCredentials
            ] = self.admin_auth_dependency,
        ):
            """Get routing statistics."""
            self._require_admin_auth(credentials)
            return self.router.get_routing_stats()

        @self.app.get("/status")
        async def get_status():
            """Get detailed server status and configuration."""
            instances = self.router.db.list_instances()
            unknown_tenants = self.router.db.get_unknown_tenants()
            default_instance = self.router.db.get_default_instance()

            return {
                "server": {
                    "name": "DataHub Cloud Router",
                    "version": "0.1.0",
                    "status": "running",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                "configuration": {
                    "default_target": self.router.default_target_url,
                    "default_instance": {
                        "id": default_instance.id if default_instance else None,
                        "name": default_instance.name if default_instance else None,
                        "url": default_instance.url if default_instance else None,
                        "is_active": (
                            default_instance.is_active if default_instance else None
                        ),
                    },
                },
                "statistics": {
                    "total_instances": len(instances),
                    "active_instances": len([i for i in instances if i.is_active]),
                    "unknown_tenants": len(unknown_tenants),
                    "database_type": "sqlite",  # This could be made dynamic
                },
                "endpoints": {
                    "health": "/health",
                    "status": "/status",
                    "webhook": "/public/teams/webhook",
                    "bot_framework": "/api/messages",
                    "admin": "/admin/",
                },
            }

    # OAuth state decoding is now handled by SecurityManager

    async def auto_register_tenant_after_oauth(
        self, datahub_url: str, instance_id: str, tenant_id: str
    ):
        """Auto-register tenant mapping after successful OAuth"""
        try:
            print(
                f"🔄 Auto-registering tenant {tenant_id} after OAuth success for {datahub_url}"
            )

            # Check if instance already exists
            instance = self.router.db.get_instance(instance_id)
            if not instance:
                # TODO: Need to finalize what should be stored in this URL - this port change is hacky
                # Convert frontend URL to integrations service URL for webhook forwarding
                # Frontend runs on :3000, integrations service on :9003
                integrations_url = datahub_url.replace(":3000", ":9003")

                # Create new instance pointing to integrations service
                instance = self.router.db.create_instance(
                    name=f"DataHub Instance ({datahub_url})",
                    url=integrations_url,
                    deployment_type=DeploymentType.CLOUD,
                    is_active=True,
                    is_default=False,
                )
                instance_id = instance.id
                print(f"✅ Created DataHub instance: {instance_id}")
            else:
                print(f"♾️ Using existing DataHub instance: {instance_id}")

            # Create tenant mapping
            mapping = self.router.db.create_mapping(
                tenant_id=tenant_id,
                instance_id=instance_id,
                created_by="oauth_auto_registration",
            )

            # Remove from unknown tenants if it exists
            self.router.db.remove_unknown_tenant(tenant_id)

            print(f"✅ Created tenant mapping: {tenant_id} -> {instance.url}")
            print(f"   Mapping ID: {mapping.id}")

        except Exception as e:
            print(f"❌ Error during auto-registration: {e}")
            # Don't fail the OAuth flow if auto-registration fails

    def run_server(self, host: str = "0.0.0.0", port: int = 9005):
        """Run the FastAPI server."""
        print("🚀 Starting Teams Multi-Tenant Router & Installation Test Server")
        print("   Mode: routing")
        print(f"   Host: {host}")
        print(f"   Port: {port}")
        print(f"   Webhook URL: http://{host}:{port}/public/teams/webhook")
        print(f"   Bot Framework URL: http://{host}:{port}/api/messages")
        print(f"   Events Log: {Path.cwd() / 'installation_events.log'}")
        print(f"   Health Check: http://{host}:{port}/health")
        print(f"   View Events: http://{host}:{port}/events")
        print(f"   Default Target: {self.router.default_target_url}")
        print(f"   Admin API: http://{host}:{port}/admin/")

        # Security configuration
        if self.admin_auth_enabled:
            print("   🔒 Admin Auth: ENABLED")
            if self.admin_api_key:
                key_preview = (
                    f"{self.admin_api_key[:8]}..."
                    if len(self.admin_api_key) > 8
                    else "***"
                )
                print(f"   🔑 Admin API Key: {key_preview}")
                print(
                    f"   📡 Admin API Usage: curl -H 'Authorization: Bearer {self.admin_api_key}' http://{host}:{port}/admin/instances"
                )
            else:
                print("   ⚠️ Admin API Key: NOT SET")
        else:
            print("   🚨 Admin Auth: DISABLED (INSECURE)")
            print("   ⚠️ Warning: Admin endpoints are unprotected!")

        # Webhook security configuration
        if self.webhook_auth_enabled:
            registered_integrations = (
                self.security_manager.get_registered_integrations()
            )
            print("   🔒 Webhook Auth: ENABLED")
            if registered_integrations:
                print(
                    f"   ✅ Protected Integrations: {', '.join(registered_integrations)}"
                )
            else:
                print("   🚨 No integration validators registered!")
            print("   🔐 OAuth State: Cryptographically secured")
        else:
            print("   🚨 Webhook Auth: DISABLED (INSECURE)")
            print("   ⚠️ Warning: All webhook endpoints are unprotected!")

        print()
        print("🔍 Waiting for Teams events...")
        print(
            "🔄 Multi-tenant routing enabled - events will be routed to configured DataHub instances"
        )
        print("📋 Install a Teams app with bot to see events here")
        print("⏹️  Press Ctrl+C to stop")
        print()

        uvicorn.run(
            self.app,
            host=host,
            port=port,
            log_level="info",
        )
