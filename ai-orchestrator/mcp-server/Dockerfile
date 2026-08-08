FROM python:3.13-slim

# Pinned: `@latest` would let a new release change tool schemas between restarts.
ARG MCP_SERVER_DATAHUB_VERSION=0.6.0
RUN pip install --no-cache-dir "mcp-server-datahub==${MCP_SERVER_DATAHUB_VERSION}"

# 0.0.0.0 binds inside the container; compose restricts the published port.
ENV FASTMCP_HOST=0.0.0.0 \
    FASTMCP_PORT=8000 \
    DATAHUB_TELEMETRY_ENABLED=false

EXPOSE 8000

ENTRYPOINT ["mcp-server-datahub", "--transport", "http"]
