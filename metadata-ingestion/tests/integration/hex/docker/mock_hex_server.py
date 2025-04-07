#!/usr/bin/env python3
"""
Simple HTTP server that returns the same JSON response for any request to /api/v1/projects
"""

import http.server
import json
import socketserver
from http import HTTPStatus
from urllib.parse import urlparse

PORT = 8000

# Load the mock response data
with open("/app/hex_projects_response.json", "r") as f:
    HEX_PROJECTS_RESPONSE = f.read()


class MockHexAPIHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        """Handle GET requests"""
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        # Health check endpoint
        if path == "/health":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        # Mock Hex API endpoints
        if path.startswith("/api/v1/projects"):
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(HEX_PROJECTS_RESPONSE.encode())
            return

        # Default 404 response
        self.send_response(HTTPStatus.NOT_FOUND)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"error": "Not found", "path": self.path}).encode())


# Set up the server
handler = MockHexAPIHandler
httpd = socketserver.TCPServer(("", PORT), handler)

print(f"Serving mock Hex API at port {PORT}")
httpd.serve_forever()
