#!/usr/bin/env python3
"""
Simple app.py file for running the DataHub Multi-Tenant Router with uvicorn.
"""

import os
from datahub.cloud.router import DataHubMultiTenantRouter, MultiTenantRouter

# Create router and server instances
router = MultiTenantRouter()
server = DataHubMultiTenantRouter(
    router,
    teams_app_id=os.getenv("DATAHUB_TEAMS_APP_ID"),
    teams_app_password=os.getenv("DATAHUB_TEAMS_APP_PASSWORD")
)

# Export the FastAPI app for uvicorn
app = server.app

