"""
Main FastAPI Application - DataHub Chat UI Backend.

This application provides a REST API with Server-Sent Events (SSE) streaming
for real-time chat functionality. It serves as the backend for both the React
and Streamlit frontends.
"""

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

from api.routes import auto_chat, chat, config, health, kubectl, profiles

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

# Serve React app static files
frontend_dist = backend_dir.parent / "frontend" / "dist"
if frontend_dist.exists():
    # Mount static assets (JS, CSS, etc.)
    app.mount("/assets", StaticFiles(directory=str(frontend_dist / "assets")), name="assets")

    logger.info(f"Serving React app from {frontend_dist}")
else:
    logger.warning(f"Frontend dist directory not found: {frontend_dist}")


@app.on_event("startup")
async def startup_event():
    """Initialize application on startup."""
    logger.info("Starting DataHub Chat API server")
    logger.info("API documentation available at /api/docs")
    if frontend_dist.exists():
        logger.info("React UI available at http://localhost:8001/")


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

    This allows React Router to handle client-side routing.
    API routes are handled by the routers above.
    """
    # If the path starts with /api, it should have been handled by API routers
    if full_path.startswith("api/"):
        return {"error": "Not found"}, 404

    # Serve index.html for all other routes (React handles routing)
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
        port=8000,
        reload=True,
        log_level="info",
    )
