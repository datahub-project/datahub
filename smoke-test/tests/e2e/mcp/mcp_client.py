"""A minimal MCP client over plain HTTP, for smoke tests.

It speaks both protocol families datahub-ai-service serves at /mcp: the legacy
one (initialize, then requests) and the modern stateless one (every request
carries its protocol version and capabilities in ``_meta`` and in headers).
Responses may be plain JSON or a short SSE stream ending with the result.
"""

import json
from typing import Any, Dict, Optional

import requests

LEGACY_VERSION = "2025-11-25"
MODERN_VERSION = "2026-07-28"
CLIENT_INFO = {"name": "datahub-smoke-test", "version": "0.0.0"}


def parse_jsonrpc_response(response: requests.Response) -> Dict[str, Any]:
    """The JSON-RPC message in a JSON or SSE response body."""
    content_type = response.headers.get("Content-Type", "")
    if content_type.startswith("text/event-stream"):
        messages = [
            json.loads(line[len("data:") :])
            for line in response.text.splitlines()
            if line.startswith("data:")
        ]
        replies = [m for m in messages if "result" in m or "error" in m]
        assert replies, f"no JSON-RPC reply in SSE body: {response.text[:500]}"
        return replies[-1]
    return response.json()


class McpClient:
    def __init__(self, url: str, token: Optional[str] = None, modern: bool = False):
        self.url = url
        self.modern = modern
        self._next_id = 1
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            }
        )
        if token:
            self._session.headers["Authorization"] = f"Bearer {token}"

    def post(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        notification: bool = False,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        message: Dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if not notification:
            message["id"] = self._next_id
            self._next_id += 1
        extra_headers = dict(headers or {})
        if self.modern:
            params = dict(params or {})
            params["_meta"] = {
                "io.modelcontextprotocol/protocolVersion": MODERN_VERSION,
                "io.modelcontextprotocol/clientInfo": CLIENT_INFO,
                "io.modelcontextprotocol/clientCapabilities": {},
            }
            extra_headers.update(
                {"MCP-Protocol-Version": MODERN_VERSION, "Mcp-Method": method}
            )
            if method == "tools/call":
                extra_headers["Mcp-Name"] = params["name"]
        elif method != "initialize":
            extra_headers["MCP-Protocol-Version"] = LEGACY_VERSION
        if params is not None:
            message["params"] = params
        return self._session.post(
            self.url, json=message, headers=extra_headers, timeout=60
        )

    def request(self, method: str, params: Optional[Dict[str, Any]] = None) -> Any:
        response = self.post(method, params)
        assert response.status_code == 200, (
            f"{method}: HTTP {response.status_code} {response.text[:500]}"
        )
        reply = parse_jsonrpc_response(response)
        assert "error" not in reply, f"{method}: {reply['error']}"
        return reply["result"]

    def connect(self) -> Dict[str, Any]:
        """Start a session: initialize (legacy) or server/discover (modern)."""
        if self.modern:
            return self.request("server/discover")
        result = self.request(
            "initialize",
            {
                "protocolVersion": LEGACY_VERSION,
                "capabilities": {},
                "clientInfo": CLIENT_INFO,
            },
        )
        initialized = self.post("notifications/initialized", notification=True)
        assert initialized.status_code == 202, initialized.text[:500]
        return result

    def list_tool_names(self) -> set:
        return {tool["name"] for tool in self.request("tools/list")["tools"]}

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> Any:
        result = self.request(
            "tools/call", {"name": name, "arguments": arguments or {}}
        )
        assert not result.get("isError"), f"{name} failed: {result['content']}"
        if "structuredContent" in result:
            return result["structuredContent"]
        return json.loads(result["content"][0]["text"])
