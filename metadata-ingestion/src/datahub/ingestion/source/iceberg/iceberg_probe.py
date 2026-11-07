from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional

import pydantic
import requests
from botocore.exceptions import BotoCoreError, ClientError
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import (
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    RESTError,
)
from pyiceberg.table import Table
from pyiceberg.typedef import Identifier

from datahub.ingestion.agent.error_policy import name_foreign
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    ProbeProviderBase,
    echoed,
    resolve_name,
)
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
)
from datahub.ingestion.source.aws.aws_common import aws_probe_error_code
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.iceberg.iceberg_common import (
    FILE_IO_BAD_PATH,
    FILE_IO_FAILURE,
    IcebergSourceConfig,
    partition_spec_json,
    table_custom_properties,
)

# The failures a pyiceberg catalog call can raise that are the source's: REST
# and AWS errors, transport errors, storage (OSError), and ValueErrors such as
# Glue's "table is missing Parameters property". Anything else is a defect.
_SOURCE_ERRORS = (
    RESTError,
    ClientError,
    BotoCoreError,
    requests.RequestException,
    OSError,
    ValueError,
)

# What ingestion does when each call fails (iceberg.py), so a failure here
# says what it means for a run.
_NAMESPACES_EFFECT = "ingestion fails the whole run on this"
_NAMESPACE_EFFECT = "ingestion skips this namespace and every table in it"
_TABLE_EFFECT = "ingestion skips this table"


def _dotted(identifier: Identifier) -> str:
    # The spelling ingestion matches patterns against: it joins the
    # identifier tuple on ".".
    return ".".join(identifier)


class IcebergMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over a pyiceberg catalog.

    Reads catalog listings and table metadata files. Never scans a table,
    reads a data file or manifest, or returns catalog/FileIO properties
    (REST catalogs put vended storage credentials in the latter).
    """

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog
        self._on_exit(catalog.close)

    @classmethod
    def for_config(cls, config: IcebergSourceConfig) -> "IcebergMetadataProbe":
        # get_catalog, not load_catalog: it carries the Glue role-assumption
        # workaround and the REST retry/timeout adapter, so the probe
        # authenticates exactly as ingestion does.
        return cls(config.get_catalog())

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        """The code of a botocore ClientError (Glue catalogs pass them through
        raw); REST errors already carry their HTTP status."""
        return aws_probe_error_code(exc)

    @contextmanager
    def _calling(self, operation: str, effect: str) -> Iterator[None]:
        """Name the pyiceberg call that failed and what ingestion does on it.
        Only the label of the foreign error is shown: load_catalog and REST
        errors can embed the catalog properties."""
        try:
            yield
        except (ProbeArgumentError, ProbeConnectionError, ProbeInternalError):
            raise
        except pydantic.ValidationError:
            # Already reported as a failure to read what the source sent.
            raise
        except NoSuchNamespaceError as exc:
            # Every call here follows the namespace's resolution against the
            # catalog's own listing, so this is the source's state: dropped
            # since, or unreadable (Glue refuses a database named with a dot).
            raise ProbeConnectionError(
                f"{operation} failed: the catalog lists the namespace but "
                f"could not read it {name_foreign(exc, type(self))}; {effect}"
            ) from None
        except _SOURCE_ERRORS as exc:
            raise ProbeConnectionError(
                f"{operation} failed {name_foreign(exc, type(self))}; {effect}"
            ) from None

    def _top_level_namespaces(self) -> List[Identifier]:
        # No parent argument, as ingestion calls it: every pyiceberg catalog
        # then returns top-level namespaces only, which is all ingestion reads.
        with self._calling("listing the catalog's namespaces", _NAMESPACES_EFFECT):
            return list(self._catalog.list_namespaces())

    def _resolve_namespace(self, namespace: str) -> Identifier:
        """The identifier tuple for a namespace named as ingestion spells it.

        Resolved against the listing rather than split on ".": a REST catalog
        can hold a single-level namespace whose name contains a dot, and
        splitting it would address a namespace that does not exist.
        """
        return resolve_name(
            namespace,
            self._top_level_namespaces(),
            key=_dotted,
            kind="top-level namespace",
            where=(
                "in this catalog; ingestion reads only top-level namespaces, so "
                "a nested one is never ingested"
            ),
            list_command="probe run namespaces",
            stop_at_first=True,
        ).record

    @probe_method(kind=DatasetContainerSubTypes.NAMESPACE, row_limit_param="limit")
    def namespaces(self, limit: int = 500) -> List[str]:
        """Top-level namespaces in the catalog, spelled the way namespace_pattern
        is matched (identifier parts joined with "."). Includes namespaces the
        recipe's namespace_pattern excludes -- a denied namespace is reported,
        not hidden. Nested namespaces are not listed because ingestion never
        reads them. Metadata only."""
        return sorted(_dotted(ns) for ns in self._top_level_namespaces())[:limit]

    def _list_tables(self, resolved: Identifier) -> List[Identifier]:
        with self._calling(
            f"listing the tables of namespace {echoed(_dotted(resolved))}",
            _NAMESPACE_EFFECT,
        ):
            return list(self._catalog.list_tables(resolved))

    def _load_table(self, namespace: str, table: str) -> Optional[Table]:
        """The loaded table, or None (with a warning) for one that exists but
        is not an Iceberg table."""
        resolved = self._resolve_namespace(namespace)
        # The table is resolved against the namespace's listing too, so a
        # table the catalog lists but cannot load is told apart from a typo.
        identifier = resolve_name(
            table,
            self._list_tables(resolved),
            key=lambda listed: str(listed[-1]),
            kind="table",
            where=(
                f"in namespace {echoed(namespace)}; the catalog lists only "
                "Iceberg tables, so a non-Iceberg table is never ingested"
            ),
            list_command="probe run tables",
            stop_at_first=True,
        ).record
        qualified = echoed(_dotted(identifier))
        with self._calling(f"loading table {qualified}", _TABLE_EFFECT):
            try:
                return self._catalog.load_table(identifier)
            # Both are checked before NoSuchTableError, their base class. The
            # table exists, so each is an empty answer with a warning, as
            # ingestion skips it with one.
            except NoSuchIcebergTableError:
                # Glue and Hive list only Iceberg tables, so this is the race
                # where the table stopped being Iceberg between the two calls
                # (or a catalog that lists every table).
                self._warn(
                    f"{qualified} is not an Iceberg table, so ingestion skips it "
                    f"with a warning and nothing is reported for it"
                )
                return None
            except NoSuchPropertyException:
                # Glue and Hive raise this for a table typed Iceberg but
                # registered without metadata_location (or without table_type
                # at all), which passes the listing: no race is needed.
                self._warn(
                    f"{qualified} lacks a catalog property needed to load it "
                    f"(such as metadata_location), so ingestion skips it with "
                    f"a warning and nothing is reported for it"
                )
                return None
            except NoSuchTableError as exc:
                raise ProbeConnectionError(
                    f"loading table {qualified} failed: the catalog lists it but "
                    f"could not load it {name_foreign(exc, type(self))}; "
                    f"{_TABLE_EFFECT}"
                ) from None
            except ValueError as exc:
                message = str(exc)
                if FILE_IO_FAILURE not in message and FILE_IO_BAD_PATH not in message:
                    raise
                # The py-io-impl the catalog's merged properties name (often the
                # server's) cannot be loaded in this environment: retrying
                # cannot help, as with any missing module (exit 1). Both of
                # pyiceberg's messages end with the configured value, which
                # tells the operator what to install or fix.
                io_impl = message.rsplit(": ", 1)[-1].strip()
                raise ProbeInternalError(
                    f"the FileIO implementation {echoed(io_impl)} for "
                    f"{qualified} could not be loaded; install the package that "
                    f"provides it, or set py-io-impl on the catalog to a full "
                    f"module.Class path; {_TABLE_EFFECT}"
                ) from None

    @probe_method()
    def namespace_properties(self, namespace: str) -> Dict[str, str]:
        """Properties of one top-level namespace (location, owner, comment...),
        the ones ingestion attaches to the namespace container. Ingestion skips
        a namespace, and every table in it, if this read fails -- so an error
        here explains a namespace that ingests nothing."""
        resolved = self._resolve_namespace(namespace)
        with self._calling(
            f"reading the properties of namespace {echoed(namespace)}",
            _NAMESPACE_EFFECT,
        ):
            properties = self._catalog.load_namespace_properties(resolved)
        return {str(k): str(v) for k, v in properties.items()}

    @probe_method(
        kind=DatasetSubTypes.TABLE,
        row_limit_param="limit",
        parent_params=("namespace",),
    )
    def tables(self, namespace: str, limit: int = 500) -> List[str]:
        """Tables in one top-level namespace, as bare names. Includes tables the
        recipe's table_pattern excludes -- a denied table is reported, not
        hidden. table_pattern is matched against "<namespace>.<table>";
        `probe filter` builds that from the namespace reported as the parent.
        Metadata only."""
        identifiers = self._list_tables(self._resolve_namespace(namespace))
        return sorted(identifier[-1] for identifier in identifiers)[:limit]

    @probe_method()
    def columns(self, namespace: str, table: str) -> List[Dict[str, object]]:
        """Top-level fields of the table's current schema: field id, name,
        Iceberg type, required, doc. Nested types are shown as their Iceberg
        type string. Reads the table metadata file through the same load_table
        call ingestion makes, so a storage permission problem shows up here as
        it would during ingestion. Never reads data files."""
        loaded = self._load_table(namespace, table)
        if loaded is None:
            return []
        return [
            {
                "id": field.field_id,
                "name": field.name,
                "type": str(field.field_type),
                "required": field.required,
                "doc": field.doc,
            }
            for field in loaded.schema().fields
        ]

    @probe_method()
    def table_metadata(self, namespace: str, table: str) -> Dict[str, str]:
        """The custom properties ingestion records on the table's dataset: its
        table properties plus location, format-version, partition-spec,
        snapshot-id and manifest-list. Never includes FileIO or catalog
        properties, which can hold vended storage credentials."""
        loaded = self._load_table(namespace, table)
        if loaded is None:
            return {}
        partition_spec: Optional[str] = None
        try:
            partition_spec = partition_spec_json(loaded)
        except (ValueError, KeyError, StopIteration) as exc:
            # Ingestion warns and records the partition spec as None.
            # StopIteration: Table.spec() or Table.schema() finds no entry for
            # default-spec-id or current-schema-id, which pyiceberg validates
            # on load for format v2/v3 but not v1.
            self._warn(
                f"the partition spec could not be read "
                f"{name_foreign(exc, type(self))}; ingestion records it as None"
            )
        return table_custom_properties(loaded, partition_spec)
