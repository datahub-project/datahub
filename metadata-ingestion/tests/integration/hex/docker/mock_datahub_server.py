#!/usr/bin/env python3
"""
Simple HTTP server that returns the same JSON response for any request to /api/v1/projects
"""

import http.server
import json
import socketserver
from http import HTTPStatus
from urllib.parse import urlparse

PORT = 8010

# Load the mock response data
with open("/app/datahub_entities_v2.json", "r") as f:
    ENTITIES_V2_RESPONSE = f.read()

with open("/app/datahub_get_urns_by_filter.json", "r") as f:
    URNS_BY_FILTER_RESPONSE = f.read()


class MockDataHubAPIHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        print(f"GET {path}")  # !!!!!!!!!!

        # Health check endpoint
        if path == "/health":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        # Mock DataHub API endpoints
        if path == "/config":
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

        # Mock DataHub API endpoints
        if path == "/api/graphql":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(URNS_BY_FILTER_RESPONSE.encode())
            return
        if path == "/openapi/v2/entity/batch/query":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(ENTITIES_V2_RESPONSE.encode())
            return

        # Default 404 response
        self.send_response(HTTPStatus.NOT_FOUND)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"error": "Not found", "path": self.path}).encode())


# Set up the server
handler = MockDataHubAPIHandler
httpd = socketserver.TCPServer(("", PORT), handler)

print(f"Serving mock DataHub API at port {PORT}")
httpd.serve_forever()
