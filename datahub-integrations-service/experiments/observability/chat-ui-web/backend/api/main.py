"""
Main FastAPI Application - DataHub Chat UI Backend.

This application provides a REST API with Server-Sent Events (SSE) streaming
for real-time chat functionality. It serves as the backend for both the React
and Streamlit frontends.
"""

import os
import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from loguru import logger

# Add backend directory to path
backend_dir = Path(__file__).parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from api.routes import (
    archived_conversations,
    auto_chat,
    chat,
    config,
    health,
    kubectl,
    profiles,
    telemetry,
)

# Detect if we're in development mode
DEV_MODE = os.getenv("DEV_MODE", "false").lower() == "true"
VITE_DEV_SERVER = os.getenv("VITE_DEV_SERVER", "http://localhost:5173")

# Create FastAPI app
app = FastAPI(
    title="DataHub Chat API",
    description="Backend API for DataHub Chat UI with streaming support",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# Configure CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # React dev server
        "http://localhost:5173",  # Vite dev server
        "http://localhost:8501",  # Streamlit default
        "http://localhost:8502",  # Streamlit alternate
        "http://localhost:8765",  # Backend server (for dev mode)
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router)
app.include_router(config.router)
app.include_router(profiles.router)
app.include_router(kubectl.router)
app.include_router(chat.router)
app.include_router(auto_chat.router)
app.include_router(archived_conversations.router)
app.include_router(telemetry.router)

# Serve React app static files (production mode)
frontend_dist = backend_dir.parent / "frontend" / "dist"

if not DEV_MODE:
    # Production mode: Serve static files
    if frontend_dist.exists():
        # Mount static assets (JS, CSS, etc.)
        app.mount("/assets", StaticFiles(directory=str(frontend_dist / "assets")), name="assets")
        logger.info(f"Serving React app from {frontend_dist}")
    else:
        logger.warning(f"Frontend dist directory not found: {frontend_dist}")
else:
    # Development mode: Proxy to Vite dev server
    logger.info(f"DEV_MODE enabled: Will proxy frontend requests to {VITE_DEV_SERVER}")


@app.on_event("startup")
async def startup_event():
    """Initialize application on startup."""
    logger.info("Starting DataHub Chat API server")
    logger.info("API documentation available at /api/docs")

    if DEV_MODE:
        logger.info(f"🔥 HOT-RELOAD MODE: Frontend proxied from {VITE_DEV_SERVER}")
        logger.info("   Start Vite dev server: cd frontend && npm run dev")
    elif frontend_dist.exists():
        logger.info("React UI available at http://localhost:8765/")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("Shutting down DataHub Chat API server")

    # Clean up integrations service if we started it
    from api.dependencies import cleanup_integrations_service
    cleanup_integrations_service()


# Catch-all route to serve React app for client-side routing
@app.get("/{full_path:path}")
async def serve_react_app(full_path: str):
    """
    Serve React app for all non-API routes.

    In production: Serves static files from dist/
    In development: Proxies to Vite dev server for hot-reload

    This allows React Router to handle client-side routing.
    API routes are handled by the routers above.
    """
    # If the path starts with /api, it should have been handled by API routers
    # FastAPI strips leading slash, so check for "api/"
    if full_path.startswith("api/"):
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Not found")

    if DEV_MODE:
        # Development mode: Proxy to Vite dev server
        import httpx

        vite_url = f"{VITE_DEV_SERVER}/{full_path}" if full_path else VITE_DEV_SERVER

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    vite_url,
                    follow_redirects=True,
                    timeout=30.0,
                )

                # Return the response from Vite
                from fastapi import Response
                return Response(
                    content=response.content,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                )
        except Exception as e:
            logger.error(f"Failed to proxy to Vite dev server: {e}")
            from fastapi import HTTPException
            raise HTTPException(
                status_code=503,
                detail=f"Vite dev server not available at {VITE_DEV_SERVER}. Run: cd frontend && npm run dev"
            )
    else:
        # Production mode: Serve static files
        index_file = frontend_dist / "index.html"
        if index_file.exists():
            return FileResponse(str(index_file))

        return {
            "service": "DataHub Chat API",
            "version": "1.0.0",
            "docs": "/api/docs",
            "health": "/api/health",
            "error": "React app not built. Run 'cd frontend && npm run build'",
        }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8765,
        reload=True,
        log_level="info",
    )
