import requests

from datahub.ingestion.agent.api_probe import infer_shape, probe_api


def _paths(fields):
    return {f.path: f for f in fields}


def test_infer_shape_records_types_not_values():
    data = {
        "id": 5,
        "name": "alice",
        "tags": ["x", "y"],
        "meta": {"active": True, "score": 1.5},
    }
    fields = infer_shape(data)
    paths = _paths(fields)
    assert paths["id"].json_type == "integer"
    assert paths["name"].json_type == "string"
    assert paths["tags"].json_type == "array"
    assert paths["tags[]"].json_type == "string"
    assert paths["meta"].json_type == "object"
    assert paths["meta.active"].json_type == "boolean"
    assert paths["meta.score"].json_type == "number"
    # The shape must never carry a value.
    blob = str([f.to_dict() for f in fields])
    assert "alice" not in blob
    assert "5" not in blob


def test_infer_shape_marks_nullable_across_array_items():
    fields = infer_shape({"rows": [{"x": 1}, {"x": None}]})
    x = _paths(fields)["rows[].x"]
    assert x.json_type == "integer"
    assert x.nullable is True


class _FakeResp:
    def __init__(self, status, content_type, body):
        self.status_code = status
        self.headers = {"Content-Type": content_type}
        self._body = body if isinstance(body, bytes) else body.encode()

    def iter_content(self, chunk_size=8192):
        yield self._body

    def close(self):
        pass


class _FakeSession:
    def __init__(self, routes):
        self._routes = routes
        self.headers = {}
        self.requested = []

    def get(self, url, timeout=None, verify=True, stream=False):
        self.requested.append(url)
        return self._routes[url]

    def close(self):
        pass


def _install(monkeypatch, routes):
    session = _FakeSession(routes)
    monkeypatch.setattr(requests, "Session", lambda: session)
    return session


def test_probe_api_returns_shape_per_endpoint(monkeypatch):
    routes = {
        "https://api.example.com/v1/orders": _FakeResp(
            200, "application/json", '{"id": 1, "total": 9.9}'
        )
    }
    session = _install(monkeypatch, routes)
    result = probe_api("https://api.example.com", ["/v1/orders"])
    assert result.supported is True
    ep = result.endpoints[0]
    assert ep.status == 200
    assert {"id", "total"} <= {f.path for f in ep.fields}
    # Only GET is ever issued.
    assert session.requested == ["https://api.example.com/v1/orders"]


def test_probe_api_budget_caps_requests(monkeypatch):
    routes = {
        f"https://api.example.com/{p}": _FakeResp(200, "application/json", "{}")
        for p in ("a", "b", "c")
    }
    session = _install(monkeypatch, routes)
    result = probe_api("https://api.example.com", ["a", "b", "c"], budget=2)
    assert len(session.requested) == 2
    assert any("budget" in w for w in result.warnings)


def test_probe_api_flags_non_json(monkeypatch):
    routes = {
        "https://api.example.com/page": _FakeResp(200, "text/html", "<html></html>")
    }
    _install(monkeypatch, routes)
    result = probe_api("https://api.example.com", ["page"])
    assert result.endpoints[0].error is not None
    assert not result.endpoints[0].fields


def test_probe_api_verify_ssl_off_warns(monkeypatch):
    _install(monkeypatch, {})
    result = probe_api("https://api.example.com", [], verify_ssl=False)
    assert any("verify_ssl" in w for w in result.warnings)
