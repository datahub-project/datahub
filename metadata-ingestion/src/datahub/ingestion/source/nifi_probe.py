from dataclasses import dataclass
from typing import Dict, Iterator, List, Mapping, Optional, Tuple, Union
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    ProbeProviderBase,
    echoed,
    take,
)
from datahub.ingestion.source.common.subtypes import JobContainerSubTypes
from datahub.ingestion.source.nifi import (
    PG_ENDPOINT,
    NifiSource,
    NifiSourceConfig,
    NifiSourceReport,
    nifi_session,
)
from datahub.ingestion.source.nifi_selection import (
    ANCESTORS_ATTRIBUTE,
    encode_ancestors,
)

# Ingestion's session sets no timeout. A probe answers a person waiting on
# it, so an unresponsive server fails the command instead of hanging it.
_TIMEOUT_SECONDS = 30


class _DefaultTimeoutAdapter(HTTPAdapter):
    """Bounds a request sent without a timeout: NifiSource.authenticate posts
    SINGLE_USER and KERBEROS token requests with none, and a stalled token
    endpoint would otherwise hang the command."""

    def send(
        self,
        request: requests.PreparedRequest,
        stream: bool = False,
        timeout: Union[None, float, Tuple[float, float], Tuple[float, None]] = None,
        verify: Union[bool, str] = True,
        cert: Union[
            None, bytes, str, Tuple[Union[bytes, str], Union[bytes, str]]
        ] = None,
        proxies: Optional[Mapping[str, str]] = None,
    ) -> requests.Response:
        return super().send(
            request,
            stream=stream,
            timeout=_TIMEOUT_SECONDS if timeout is None else timeout,
            verify=verify,
            cert=cert,
            proxies=proxies,
        )


@dataclass(frozen=True)
class _Pending:
    group_id: str
    # The names from the root down to this group, itself included: the
    # ancestors of the groups inside it.
    path: Tuple[str, ...]


class NifiMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over NiFi's REST API.

    Signs in with NifiSource.authenticate on the session nifi_session builds,
    so every auth mode (NO_AUTH, SINGLE_USER, CLIENT_CERT, KERBEROS,
    BASIC_AUTH) and ca_file behave as in a run. NifiSource is built with
    __new__ and primed with only what authenticate reads: its __init__ sets up
    stateful ingestion, which a probe has no use for.

    No `api` passthrough: provenance endpoints carry flowfile attributes and
    content claims (row values), and processor configs carry connection
    properties. Only the process group tree is read.
    """

    def __init__(self, config: NifiSourceConfig) -> None:
        self._config = config
        self._report = NifiSourceReport()

    @classmethod
    def for_config(cls, config: NifiSourceConfig) -> "NifiMetadataProbe":
        return cls(config)

    @property
    def probe_report(self) -> object:
        return self._report

    def _source(self) -> NifiSource:
        return self._open_once(
            "source", self._signed_in, close=lambda source: source.session.close()
        )

    def _signed_in(self) -> NifiSource:
        source = NifiSource.__new__(NifiSource)
        source.config = self._config
        source.report = self._report
        source.session = nifi_session(self._config)
        # Mounted before authenticate, so CLIENT_CERT's more specific
        # SSLAdapter mount still takes the API base URL.
        for prefix in ("http://", "https://"):
            source.session.mount(prefix, _DefaultTimeoutAdapter())
        source.authenticate()
        return source

    def _flow(self, group_id: str) -> Mapping[str, object]:
        """One group's processGroupFlow, as ingestion's update_flow reads it.
        An HTTP error raises, for the caller's soft_listing to judge."""
        source = self._source()
        response = source.session.get(
            url=urljoin(source.rest_api_base_url, PG_ENDPOINT) + group_id,
            timeout=_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        body = response.json()
        flow = body.get("processGroupFlow") if isinstance(body, dict) else None
        return flow if isinstance(flow, dict) else {}

    @probe_method()
    def flow(self) -> Dict[str, object]:
        """The root process group: its name, which ingestion uses as the
        DataFlow's name and which process_group_pattern is matched against
        like any group's (deny it and nothing is ingested), and its id. Judge
        it with `probe filter --kind "Process Group" --name <name>`."""
        root = _breadcrumb(self._flow("root"))
        return {"name": root.get("name"), "id": root.get("id")}

    @probe_method(kind=JobContainerSubTypes.NIFI_PROCESS_GROUP, row_limit_param="limit")
    def process_groups(self, limit: int = 500) -> List[Dict[str, object]]:
        """Every process group below the root, nested ones included, by name
        (what process_group_pattern is matched on), with its id, its parent's
        id, and `ancestors`: the names above it, root first, as a JSON array.
        Ingestion stops walking at a group the pattern refuses, so a group
        inside an excluded one is excluded too; `probe filter --from-run`
        judges the ancestors from that field. For a bare name, pass each
        enclosing group, root first, as --parent. Groups the recipe would
        exclude are listed, and walked into, all the same.

        A walked group is emitted as a container only with
        emit_process_group_as_container, and only when it, or a group inside
        it, holds an ingress or egress component. As in ingestion, an HTTP
        error on a group below the root leaves it listed and its contents
        unread, with a warning, and one on the root fails the command."""
        return take(self._walk(), limit)

    def _walk(self) -> Iterator[Dict[str, object]]:
        # Ingestion's create_nifi_flow raises on any failed root read, so the
        # root is not degraded here either.
        root = self._flow("root")
        root_name = str(_breadcrumb(root).get("name") or "")
        stack = [_Pending("root", (root_name,))]
        unreadable = 0
        while stack:
            pending = stack.pop()
            flow = root if pending.group_id == "root" else self._child_flow(pending)
            ancestors = pending.path
            children: List[_Pending] = []
            for entity in _child_groups(flow):
                component = entity.get("component")
                group_id = entity.get("id")
                if not isinstance(component, dict) or not isinstance(group_id, str):
                    # NiFi omits the component of a group the credential
                    # may not read: there is no name to judge.
                    unreadable += 1
                    continue
                name = str(component.get("name") or "")
                yield {
                    "name": name,
                    "id": group_id,
                    "parent_id": component.get("parentGroupId"),
                    ANCESTORS_ATTRIBUTE: encode_ancestors(ancestors),
                }
                children.append(_Pending(group_id, (*ancestors, name)))
            # Reversed, so the walk visits siblings in NiFi's order.
            stack.extend(reversed(children))
        if unreadable:
            self._warn(
                f"{unreadable} process groups this credential cannot read "
                f"were left out; ingestion cannot read them either"
            )

    def _child_flow(self, pending: _Pending) -> Mapping[str, object]:
        """A group below the root. Ingestion's update_flow warns and moves on
        to the next sibling when such a read answers any error status (a
        connection failure still raises there), so the probe does the same."""
        try:
            return self._flow(pending.group_id)
        except requests.HTTPError as exc:
            status = getattr(exc.response, "status_code", None)
            self._warn(
                f"process group {echoed('/'.join(pending.path))} returned "
                f"HTTP {status}; treating it as empty."
            )
            return {}


def _breadcrumb(flow: Mapping[str, object]) -> Mapping[str, object]:
    outer = flow.get("breadcrumb")
    inner = outer.get("breadcrumb") if isinstance(outer, dict) else None
    return inner if isinstance(inner, dict) else {}


def _child_groups(flow: Mapping[str, object]) -> List[Mapping[str, object]]:
    contents = flow.get("flow")
    groups = contents.get("processGroups") if isinstance(contents, dict) else None
    if not isinstance(groups, list):
        return []
    return [g for g in groups if isinstance(g, dict)]
