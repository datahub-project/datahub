"""
Thin Collibra client skeleton for the DataHub `collibra` ingestion source.

Belongs at: metadata-ingestion/src/datahub/ingestion/source/collibra/client.py
Design: RFC "Collibra -> DataHub Governance Migrator", Page 4 (Engineering Design)
        + Page 6 (Extraction). Tracked in ING-3045.

Stack (no new deps): requests + urllib3.Retry + concurrent.futures + pydantic.

This skeleton keeps `requests` imports lazy and uses stdlib dataclasses for the
stub models so the __main__ self-check runs with plain `python3`, no network,
no third-party deps. In the connector:
  - swap the `Cfg` dataclass for CollibraSourceConfig (pydantic) in config.py
  - swap the stub dataclass models for pydantic BaseModel in models.py
  - secrets are SecretStr, injected programmatically (never os.environ)
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple

PAGE_SIZE = 1000  # DGC "Enable maximum paging limit" cap: 1000 elements / call


# --- config (skeleton) -------------------------------------------------------
# ponytail: real connector uses CollibraSourceConfig(StatefulIngestionConfigBase,
# PlatformInstanceConfigMixin, EnvConfigMixin) in config.py; secrets are SecretStr.
# This dataclass exists only so the file is standalone + self-checkable.
@dataclass
class Cfg:
    url: str
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    verify_ssl: bool = True
    ca_cert_path: Optional[str] = None
    max_workers: int = 4  # ponytail: ~4 matches Collibra's concurrent-job ceiling
    poll_interval_s: float = 2.0


# --- Step 1: auth + session + retry ------------------------------------------
def build_session(cfg: Cfg):
    """OAuth2 bearer + TLS + rate-aware retry/backoff, all on one Session."""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    class TokenAuth(requests.auth.AuthBase):
        def __init__(self, cfg: Cfg):
            self.cfg, self._tok = cfg, None

        def _fetch(self) -> None:
            r = requests.post(
                f"{self.cfg.url}/rest/oauth/v2/token",
                data={"grant_type": "client_credentials"},
                auth=(self.cfg.client_id, self.cfg.client_secret),
                verify=self.cfg.ca_cert_path or self.cfg.verify_ssl,
                timeout=30,
            )
            r.raise_for_status()
            self._tok = r.json()["access_token"]

        def __call__(self, req):
            if not self._tok:
                self._fetch()
            req.headers["Authorization"] = f"Bearer {self._tok}"
            return req

    s = requests.Session()
    # respect_retry_after_header=True => 429 Retry-After honored; no hand-rolled backoff.
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
        allowed_methods=None,  # retry POSTs too (reads are idempotent for us)
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.auth = TokenAuth(cfg) if cfg.client_id else None  # else BasicAuth/session
    s.verify = cfg.ca_cert_path or cfg.verify_ssl
    # ponytail: if not cfg.verify_ssl -> report.warning(...) in the connector.
    return s


# --- stub models -------------------------------------------------------------
# ponytail: model only the fields consumed; swap to pydantic BaseModel in models.py.
@dataclass
class Asset:
    id: str
    name: str
    type_id: str


# --- client ------------------------------------------------------------------
class CollibraClient:
    def __init__(self, cfg: Cfg, session=None):
        self.cfg = cfg
        self.session = session if session is not None else build_session(cfg)
        self._info: Optional[dict] = None

    # low-level
    def _get(self, path: str, params: Optional[dict] = None):
        r = self.session.get(self.cfg.url + path, params=params or {})
        r.raise_for_status()
        return r

    def _post(self, path: str, **kw):
        r = self.session.post(self.cfg.url + path, **kw)
        r.raise_for_status()
        return r

    # Step 2: capability probe
    def info(self) -> dict:
        if self._info is None:
            self._info = self._get("/rest/2.0/application/info").json()
        return self._info

    def use_graphql(self) -> bool:
        # ponytail: version gate; derive the threshold from /application/info in Phase 0.
        return True

    # Step 3: cursor paginator (the core primitive; sequential per collection)
    def paginate(self, path: str, params: Optional[dict] = None) -> Iterator[dict]:
        params = {**(params or {}), "limit": PAGE_SIZE, "countLimit": 0}
        cursor: Optional[str] = None
        while True:
            if cursor:
                params["cursor"] = cursor
            data = self._get(path, params).json()
            yield from data.get("results", [])
            cursor = data.get("nextCursor")  # VERIFY field name against env schema
            if not cursor:
                return

    # Step 4: REST reads (examples; add attributes/relations/domains/groups/roles/responsibilities)
    def asset_types(self) -> Iterator[dict]:
        return self.paginate("/rest/2.0/assetTypes")

    def communities(self) -> Iterator[dict]:
        return self.paginate("/rest/2.0/communities")

    def users(self) -> Iterator[dict]:
        return self.paginate("/rest/2.0/users")

    # Step 5: GraphQL (primary path for the governance graph) — skeleton
    def graphql(self, query: str, variables: dict) -> dict:
        return self._post(
            "/graphql/knowledgeGraph/v1",
            json={"query": query, "variables": variables},
        ).json()

    # Step 6: Output Module fallback (submit -> poll -> download)
    def output_export(self, view_config: dict) -> bytes:
        job = self._post(
            "/rest/2.0/outputModule/export/json-job", json=view_config
        ).json()
        while True:
            j = self._get(f"/rest/2.0/jobs/{job['id']}").json()
            if j["state"] in ("COMPLETED", "ERROR", "CANCELED"):
                break
            time.sleep(
                self.cfg.poll_interval_s
            )  # ponytail: fixed poll; jitter if jobs run long
        if j["state"] != "COMPLETED":
            raise RuntimeError(f"Output Module job {job['id']} -> {j['state']}")
        file_id = j["result"]["message"]["id"]
        return self._get(f"/rest/2.0/outputModule/files/{file_id}").content

    # Step 7: parallel partition runner
    # Parallelism is ACROSS partitions (cursor paging is sequential within one).
    # partitions: (path, params) per entity-kind / typeId / relationTypeId / community.
    def extract_parallel(self, partitions: List[Tuple[str, dict]]) -> Iterator[dict]:
        with ThreadPoolExecutor(max_workers=self.cfg.max_workers) as ex:
            futs = {
                ex.submit(lambda p: list(self.paginate(p[0], p[1])), p): p
                for p in partitions
            }
            for f in as_completed(futs):
                yield from f.result()


# --- self-check (no network, no third-party deps) ----------------------------
class _Resp:
    def __init__(self, payload=None, content=b""):
        self._p, self.content = payload, content

    def json(self):
        return self._p

    def raise_for_status(self):
        pass


class _FakeSession:
    """Stateless page routes keyed by (path, cursor) + a tiny job state machine."""

    def __init__(self, base: str):
        self.base = base
        self.pages: Dict[str, Dict[Optional[str], dict]] = {
            "/rest/2.0/assetTypes": {
                None: {"results": [{"id": "1"}, {"id": "2"}], "nextCursor": "c1"},
                "c1": {"results": [{"id": "3"}, {"id": "4"}], "nextCursor": "c2"},
                "c2": {"results": [{"id": "5"}, {"id": "6"}], "nextCursor": None},
            },
            "/a": {None: {"results": [{"id": "a1"}], "nextCursor": None}},
            "/b": {None: {"results": [{"id": "b1"}, {"id": "b2"}], "nextCursor": None}},
        }
        self._job_polls = 0

    def get(self, url, params=None):
        path = url[len(self.base) :]
        if path.startswith("/rest/2.0/jobs/"):
            self._job_polls += 1
            state = "COMPLETED" if self._job_polls >= 2 else "RUNNING"
            return _Resp({"state": state, "result": {"message": {"id": "file9"}}})
        if path.startswith("/rest/2.0/outputModule/files/"):
            return _Resp(content=b"BULK")
        cursor = (params or {}).get("cursor")
        return _Resp(self.pages[path][cursor])

    def post(self, url, **kw):
        path = url[len(self.base) :]
        if path.endswith("/json-job"):
            return _Resp({"id": "job1"})
        raise AssertionError(f"unexpected POST {path}")


def _selfcheck() -> None:
    base = "http://x"
    c = CollibraClient(Cfg(url=base, poll_interval_s=0), session=_FakeSession(base))

    # cursor pagination walks 3 pages and stops on a null cursor
    ids = [x["id"] for x in c.paginate("/rest/2.0/assetTypes")]
    assert ids == ["1", "2", "3", "4", "5", "6"], ids

    # parallel fan-out collects every partition (order-independent)
    got = sorted(x["id"] for x in c.extract_parallel([("/a", {}), ("/b", {})]))
    assert got == ["a1", "b1", "b2"], got

    # Output Module poll loop: RUNNING -> COMPLETED -> download bytes
    assert c.output_export({"any": "viewconfig"}) == b"BULK"

    print("client.py self-check: OK")


if __name__ == "__main__":
    _selfcheck()
