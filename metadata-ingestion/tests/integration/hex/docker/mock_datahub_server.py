#!/usr/bin/env python3
"""
Mock DataHub server that handles both GET and POST requests and supports pagination
"""

import http.server
import json
import socketserver
from http import HTTPStatus
from urllib.parse import urlparse

PORT = 8010

# Load the mock response data
with open("/app/datahub_entities_v2_page1.json", "r") as f:
    ENTITIES_V2_PAGE1_RESPONSE = f.read()

with open("/app/datahub_entities_v2_page2.json", "r") as f:
    ENTITIES_V2_PAGE2_RESPONSE = f.read()

with open("/app/datahub_get_urns_by_filter_page1.json", "r") as f:
    URNS_BY_FILTER_PAGE1_RESPONSE = f.read()

with open("/app/datahub_get_urns_by_filter_page2.json", "r") as f:
    URNS_BY_FILTER_PAGE2_RESPONSE = f.read()

# Global state flag to track if first page has been requested
FIRST_ENTITIES_PAGE_REQUESTED = False


class MockDataHubAPIHandler(http.server.SimpleHTTPRequestHandler):
    # Global state flag to track if first page has been requested accross all instances; one instance per request
    first_entities_page_requested = False

    def do_GET(self):
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        # Health check endpoint
        if path == "/health":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        # Mock DataHub API endpoints
        if path.startswith("/config"):
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(dict(noCode="true")).encode())
            return

        # Default 404 response
        self.send_response(HTTPStatus.NOT_FOUND)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"error": "Not found", "path": self.path}).encode())

    def do_POST(self):
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        # Get request body
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length)
        request_body = json.loads(post_data)

        if path == "/openapi/v2/entity/batch/query":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            # Return the appropriate page of entity data
            if not MockDataHubAPIHandler.first_entities_page_requested:
                self.wfile.write(ENTITIES_V2_PAGE1_RESPONSE.encode())
                MockDataHubAPIHandler.first_entities_page_requested = True
            else:
                self.wfile.write(ENTITIES_V2_PAGE2_RESPONSE.encode())
            return

        if path == "/api/graphql":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            # Check if this is a scroll query with nextScrollId
            scroll_id = None
            if "variables" in request_body:
                scroll_id = request_body.get("variables", {}).get("scrollId")

            if scroll_id == "page_2_scroll_id":
                self.wfile.write(URNS_BY_FILTER_PAGE2_RESPONSE.encode())
            else:
                self.wfile.write(URNS_BY_FILTER_PAGE1_RESPONSE.encode())
            return

        # Default 404 response
        self.send_response(HTTPStatus.NOT_FOUND)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(
            json.dumps(
                {"error": "Not found", "path": self.path, "method": "POST"}
            ).encode()
        )


# Set up the server
handler = MockDataHubAPIHandler
httpd = socketserver.TCPServer(("", PORT), handler)

print(f"Serving mock DataHub API at port {PORT}")
httpd.serve_forever()
