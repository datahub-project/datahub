#!/usr/bin/env python3
"""
Mock Hex API server for integration tests.
Handles all endpoints used by the hex connector.
"""

import http.server
import json
import re
import socketserver
from urllib.parse import parse_qs, urlparse

PORT = 8000

with open("/app/hex_projects_response.json", "r") as f:
    HEX_PROJECTS_RESPONSE = json.load(f)

# Extract project IDs from the mock data
PROJECT_IDS = [p["id"] for p in HEX_PROJECTS_RESPONSE.get("values", [])]

CONNECTIONS_RESPONSE = {
    "values": [
        {
            "id": "conn-snowflake-analytics",
            "name": "Analytics Hub",
            "type": "snowflake",
            "description": "Primary analytics warehouse",
        },
        {
            "id": "conn-snowflake-global",
            "name": "Global Hub",
            "type": "snowflake",
            "description": "Global analytics warehouse",
        },
    ],
    "pagination": {"after": None, "before": None},
}

# Minimal SQL cells per project (only first 2 projects get real cells)
_CELLS_BY_PROJECT: dict = {}
if len(PROJECT_IDS) >= 2:
    _CELLS_BY_PROJECT[PROJECT_IDS[0]] = {
        "values": [
            {
                "id": "cell-sql-1",
                "staticId": "cell-sql-1",
                "cellType": "SQL",
                "label": "Customer query",
                "dataConnectionId": "conn-snowflake-analytics",
                "projectId": PROJECT_IDS[0],
                "contents": {
                    "sqlCell": {
                        "source": "SELECT * FROM analytics.public.customers LIMIT 100"
                    },
                    "codeCell": None,
                    "markdownCell": None,
                },
            },
            {
                "id": "cell-md-1",
                "staticId": "cell-md-1",
                "cellType": "MARKDOWN",
                "label": "Overview",
                "dataConnectionId": None,
                "projectId": PROJECT_IDS[0],
                "contents": {
                    "sqlCell": None,
                    "codeCell": None,
                    "markdownCell": {"source": "# Overview\n\nThis is a test project."},
                },
            },
        ],
        "pagination": {"after": None},
    }
    _CELLS_BY_PROJECT[
        PROJECT_IDS[6]
    ] = {  # PlayNotebook — imports a component + its inlined SQL
        "values": [
            {
                "id": "cell-comp-import-1",
                "staticId": "cell-comp-import-1",
                "cellType": "COMPONENT_IMPORT",
                "label": None,
                "dataConnectionId": None,
                "projectId": PROJECT_IDS[6],
                "contents": {
                    "sqlCell": None,
                    "codeCell": None,
                    "markdownCell": None,
                },
            },
            {
                # SQL cell inlined from the imported component — no component ID visible
                "id": "cell-sql-play-1",
                "staticId": "cell-sql-play-1",
                "cellType": "SQL",
                "label": "Orders query",
                "dataConnectionId": "conn-snowflake-global",
                "projectId": PROJECT_IDS[6],
                "contents": {
                    "sqlCell": {
                        "source": "SELECT order_id, customer_id FROM global.public.orders"
                    },
                    "codeCell": None,
                    "markdownCell": None,
                },
            },
        ],
        "pagination": {"after": None},
    }

if len(PROJECT_IDS) >= 8:
    _CELLS_BY_PROJECT[PROJECT_IDS[7]] = {  # Cancelled Orders component
        "values": [
            {
                "id": "cell-sql-comp-1",
                "staticId": "cell-sql-comp-1",
                "cellType": "SQL",
                "label": "Cancelled orders query",
                "dataConnectionId": "conn-snowflake-analytics",
                "projectId": PROJECT_IDS[7],
                "contents": {
                    "sqlCell": {
                        "source": "SELECT order_id FROM analytics.public.orders WHERE status = 'cancelled'"
                    },
                    "codeCell": None,
                    "markdownCell": None,
                },
            },
        ],
        "pagination": {"after": None},
    }

EMPTY_CELLS = {"values": [], "pagination": {"after": None}}

# Export YAML responses — only for projects that import components.
# Shape mirrors POST /api/v1/projects/export response.
# The YAML lists only native SQL cells + COMPONENT_IMPORT entries (no inlined component SQL).
_EXPORT_BY_PROJECT: dict = {}
if len(PROJECT_IDS) >= 8:
    # PlayNotebook (PROJECT_IDS[6]) imports the Cancelled Orders component (PROJECT_IDS[7])
    _EXPORT_BY_PROJECT[PROJECT_IDS[6]] = {
        "filename": "PlayNotebook.yaml",
        "content": (
            "schemaVersion: 3\n"
            "meta:\n"
            f"  projectId: {PROJECT_IDS[6]}\n"
            "  title: PlayNotebook\n"
            "  hexType: PROJECT\n"
            "cells:\n"
            "  - cellType: COMPONENT_IMPORT\n"
            f"    cellId: cell-comp-import-1\n"
            "    cellLabel: null\n"
            "    config:\n"
            "      component:\n"
            f"        id: {PROJECT_IDS[7]}\n"
            '        version: "1"\n'
        ),
    }

# Runs for a couple of projects
_RUNS_BY_PROJECT: dict = {}
if len(PROJECT_IDS) >= 1:
    _RUNS_BY_PROJECT[PROJECT_IDS[0]] = {
        "runs": [
            {
                "projectId": PROJECT_IDS[0],
                "runId": "run-001",
                "status": "COMPLETED",
                "startTime": "2025-03-25T10:00:00.000Z",
                "endTime": "2025-03-25T10:01:00.000Z",
                "elapsedTime": 60000,
                "runTrigger": "SCHEDULED",
            }
        ]
    }


class MockHexAPIHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        if path == "/health":
            self._respond(200, "OK", content_type="text/plain")
            return

        # Data connections
        if path == "/api/v1/data-connections":
            self._respond_json(CONNECTIONS_RESPONSE)
            return

        # Cells — keyed by projectId query param
        if path == "/api/v1/cells":
            project_id = (params.get("projectId") or [None])[0]
            cells = _CELLS_BY_PROJECT.get(project_id, EMPTY_CELLS)
            self._respond_json(cells)
            return

        # queriedTables — always 403 (non-ENTERPRISE workspace in tests)
        m = re.match(r"^/api/v1/projects/([^/]+)/queriedTables$", path)
        if m:
            self._respond_json(
                {
                    "message": "This endpoint requires the workspace to be at least ENTERPRISE tier."
                },
                status=403,
            )
            return

        # Run history
        m = re.match(r"^/api/v1/projects/([^/]+)/runs$", path)
        if m:
            project_id = m.group(1)
            runs = _RUNS_BY_PROJECT.get(project_id, {"runs": []})
            self._respond_json(runs)
            return

        # Projects list
        if path.startswith("/api/v1/projects"):
            self._respond_json(HEX_PROJECTS_RESPONSE)
            return

        self._respond_json({"error": "Not found", "path": path}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        # Project export — returns YAML with native cells + COMPONENT_IMPORT references
        if path == "/api/v1/projects/export":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            project_id = body.get("projectId")
            export = _EXPORT_BY_PROJECT.get(project_id)
            if export:
                self._respond_json(export)
            else:
                # Project has no components — return minimal YAML with no cells
                self._respond_json(
                    {
                        "filename": f"{project_id}.yaml",
                        "content": (
                            "schemaVersion: 3\n"
                            f"meta:\n  projectId: {project_id}\n  hexType: PROJECT\n"
                            "cells: []\n"
                        ),
                    }
                )
            return

        self._respond_json({"error": "Not found", "path": path}, status=404)

    def do_HEAD(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._respond(200, "", content_type="text/plain")
        elif parsed.path.startswith("/api/v1/projects"):
            self._respond(200, "", content_type="application/json")
        else:
            self._respond(404, "", content_type="application/json")

    def _respond(self, status, body, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if body:
            self.wfile.write(body.encode() if isinstance(body, str) else body)

    def _respond_json(self, data, status=200):
        body = json.dumps(data)
        self._respond(status, body)

    def log_message(self, format, *args):
        pass  # suppress request logs


httpd = socketserver.TCPServer(("", PORT), MockHexAPIHandler)
print(f"Serving mock Hex API at port {PORT}")
httpd.serve_forever()
