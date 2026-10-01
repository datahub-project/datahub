"""Probe provider for the AWS Glue Data Catalog: databases, tables and jobs.

Metadata only. Every record is an allowlist projection of the Glue API shape:
Glue keeps free-form parameters on databases, tables and columns, and job
arguments that routinely carry connection passwords, and register_secrets only
masks secrets that came from the recipe.
"""

import itertools
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    TypeVar,
    Union,
)

import yaml
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    NoRegionError,
    ParamValidationError,
    PartialCredentialsError,
)

from datahub.emitter import mce_builder
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import (
    ProbeConnectionError,
    ProbeInternalError,
    ProbeSoftError,
)
from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.aws.glue import (
    GlueSource,
    GlueSourceConfig,
    GlueSourceReport,
    glue_catalog_kwargs,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
    FlowContainerSubTypes,
)

if TYPE_CHECKING:
    from mypy_boto3_glue import GlueClient

_T = TypeVar("_T")

# What get_all_databases_and_tables does when one database's GetTables fails.
_DENIED_TABLES_SUFFIX = (
    "Ingestion skips this database's tables with a warning ('Failed to get "
    "tables from database') and still emits the database itself"
)

# A refusal of one call by IAM or Lake Formation. Glue says
# AccessDeniedException; STS (aws_role) says AccessDenied.
_ACCESS_DENIED_CODES = frozenset({"AccessDeniedException", "AccessDenied"})
# The request was not accepted as coming from valid credentials at all.
_AUTH_CODES = frozenset(
    {
        "UnrecognizedClientException",
        "InvalidClientTokenId",
        "ExpiredTokenException",
        "ExpiredToken",
        "InvalidSignatureException",
        "SignatureDoesNotMatch",
        "IncompleteSignature",
        "MissingAuthenticationTokenException",
    }
)


def _request_id_suffix(exc: ClientError) -> str:
    request_id = exc.response.get("ResponseMetadata", {}).get("RequestId")
    return f" (AWS request id {request_id})" if request_id else ""


def _translated(
    exc: Union[ClientError, BotoCoreError], action: str, soft_on_denied: bool
) -> Exception:
    """The probe's own exception for one failed AWS call.

    Worded here, never from str(exc). A ClientError renders as "An error
    occurred (Code) when calling the Op operation: <message>", and for an
    authorization failure the message names the calling principal's ARN: the
    account id, the role, and for assumed roles and SSO a session name that is
    often a person's email. botocore keeps key material out of its own text
    (it masks proxy userinfo, and the request id lives in ResponseMetadata),
    but an identity is not ours to print. The code, the action and the
    request id are enough to diagnose with.
    """
    if isinstance(exc, NoRegionError):
        return ValueError(
            "no AWS region was resolved for Glue; set aws_region in the recipe"
        )
    if isinstance(exc, ParamValidationError):
        # Refused by botocore before anything was sent, so the input is at
        # fault, not the source. Its text quotes the offending value.
        return ValueError(
            f"{action} was refused before it was sent: a request parameter is "
            f"invalid ({type(exc).__name__}); check the names passed to the "
            f"command and the recipe's catalog_id"
        )
    if isinstance(exc, (NoCredentialsError, PartialCredentialsError)):
        return ProbeConnectionError(
            f"no usable AWS credentials were found for {action} "
            f"({type(exc).__name__}); set aws_access_key_id and "
            f"aws_secret_access_key, aws_profile or aws_role, or run where the "
            f"default AWS credential chain resolves"
        )
    if isinstance(exc, ClientError):
        code = aws_error_code(exc) or "unknown error"
        suffix = _request_id_suffix(exc)
        if code in _ACCESS_DENIED_CODES and exc.operation_name == "AssumeRole":
            # aws_role is assumed while the client is built, before any Glue
            # call; Lake Formation has nothing to do with it, and nothing
            # downstream can degrade around it.
            return ProbeConnectionError(
                f"sts:AssumeRole on the recipe's aws_role was denied while "
                f"{action} ({code}){suffix}; the recipe's AWS principal needs "
                f"sts:AssumeRole on aws_role, and the role's trust policy must "
                f"allow that principal"
            )
        if code in _ACCESS_DENIED_CODES:
            message = (
                f"{action} was denied ({code}){suffix}; the recipe's AWS "
                f"principal needs this IAM permission, and Lake Formation "
                f"permission where Lake Formation governs the catalog"
            )
            if soft_on_denied:
                return ProbeSoftError(message)
            return ProbeConnectionError(message)
        if code in _AUTH_CODES:
            return ProbeConnectionError(
                f"AWS did not accept the recipe's credentials on {action} "
                f"({code}){suffix}; check aws_access_key_id/"
                f"aws_secret_access_key, aws_profile or aws_role"
            )
        if code == "EntityNotFoundException":
            return ValueError(f"{action} found no such object ({code})")
        return ProbeConnectionError(f"{action} failed ({code}){suffix}")
    # Endpoint, proxy, timeout and SSL errors. Named by class only: SSLError's
    # text embeds the transport error verbatim, and a proxy error's the proxy.
    return ProbeConnectionError(f"could not complete {action} ({aws_error_code(exc)})")


@contextmanager
def aws_call(action: str, soft_on_denied: bool = False) -> Iterator[None]:
    """Run AWS SDK calls -- and iterate their paginators, which is where a
    paged call actually fails -- with errors translated. `action` names the
    IAM action and object, e.g. "glue:GetTables on database 'sales'".

    soft_on_denied turns an access denial into a ProbeSoftError; the caller
    must catch it and record the reason (see ProbeSoftError's docstring).
    """
    try:
        yield
    except (ClientError, BotoCoreError) as exc:
        raise _translated(exc, action, soft_on_denied) from exc


def _take(items: Iterable[_T], limit: Optional[int]) -> List[_T]:
    """At most `limit` items, pulling no further page than needed.

    A paginator's search() is lazy, so islice stops the paging itself; on a
    catalog with tens of thousands of tables the discarded pages would be
    real requests.
    """
    return list(items) if limit is None else list(itertools.islice(items, limit))


def _database_record(database: Mapping[str, Any]) -> Dict[str, object]:
    target = database.get("TargetDatabase")
    return {
        "name": database["Name"],
        # "" rather than absent when Glue omits it: ingestion keeps such a
        # database whatever catalog_id says (get_all_databases tests the
        # CatalogId for truthiness), and an absent attribute would make
        # `probe filter --from-run` warn that it cannot tell.
        "catalog_id": database.get("CatalogId") or "",
        # Truthiness, as the JMESPath `[?!TargetDatabase]` in
        # get_all_databases reads it: an empty struct is not a link.
        "resource_link": bool(target),
        "target": (
            f"{target.get('CatalogId', '')}/{target.get('DatabaseName', '')}"
            if target
            else None
        ),
    }


def _table_record(
    table: Mapping[str, Any], database: Mapping[str, Any]
) -> Dict[str, object]:
    table_type = table.get("TableType")
    descriptor = table.get("StorageDescriptor") or {}
    is_view = table_type == GlueSource._VIRTUAL_VIEW_TABLE_TYPE
    return {
        "name": table["Name"],
        # The subtype _gen_table_wu gives it.
        "subtype": str(DatasetSubTypes.VIEW if is_view else DatasetSubTypes.TABLE),
        "table_type": table_type,
        "catalog_id": table.get("CatalogId") or "",
        # Membership, as get_tables_from_database tests it.
        "resource_link": "TargetTable" in table,
        "column_count": len(descriptor.get("Columns") or [])
        + len(table.get("PartitionKeys") or []),
        # The database's facts, carried on each table: `probe filter` judges a
        # --parent container by name only, so a rule decided by them -- a
        # table under an ignored resource-link database -- can only be judged
        # from the table's own record.
        "database_resource_link": bool(database.get("TargetDatabase")),
        "database_catalog_id": database.get("CatalogId") or "",
    }


def _column_record(column: Mapping[str, Any], partition_key: bool) -> Dict[str, object]:
    return {
        "name": column["Name"],
        "type": column.get("Type"),
        "comment": column.get("Comment"),
        "partition_key": partition_key,
    }


def _job_record(job: Mapping[str, Any], config: GlueSourceConfig) -> Dict[str, object]:
    command = job.get("Command") or {}
    created = job.get("CreatedOn")
    modified = job.get("LastModifiedOn")
    return {
        "name": job["Name"],
        # The URN _transform_extraction builds for this job's DataFlow.
        "flow_urn": mce_builder.make_data_flow_urn(
            orchestrator=config.platform, flow_id=job["Name"], cluster=config.env
        ),
        "command": command.get("Name"),
        "script_location": command.get("ScriptLocation"),
        "role": job.get("Role"),
        "glue_version": job.get("GlueVersion"),
        # str(), as get_dataflow_wus renders them into custom properties.
        "created_on": str(created) if created is not None else None,
        "last_modified_on": str(modified) if modified is not None else None,
    }


def _node_record(node: Mapping[str, Any], job_name: str) -> Dict[str, object]:
    node_type = node["NodeType"]
    # Sources and sinks become the input/output datasets of the nodes they
    # feed, not DataJobs of their own (_transform_extraction).
    emitted = node_type not in ("DataSource", "DataSink")
    return {
        "id": node["Id"],
        "node_type": node_type,
        "emitted_as_datajob": emitted,
        # get_datajob_wu's name for it.
        "datajob_name": f"{job_name}:{node_type}-{node['Id']}" if emitted else None,
        "urn": node["urn"],
        "input_datasets": list(node["inputDatasets"]),
        "output_datasets": list(node["outputDatasets"]),
        "input_datajobs": list(node["inputDatajobs"]),
    }


# The start of process_dataflow_node's error for a node it does not recognise
# when ignore_unsupported_connectors is false; the rest is the node's args.
_UNRECOGNIZED_NODE_PREFIX = "Unrecognized Glue data object type"


def _unparseable_node(dag: Mapping[str, Any]) -> str:
    """`<NodeType>-<Id>` of the first node whose args process_dataflow_node
    cannot YAML-load, without keeping or echoing the value itself."""
    for node in dag.get("DagNodes") or []:
        if node.get("NodeType") not in ("DataSource", "DataSink"):
            continue
        for arg in node.get("Args") or []:
            try:
                yaml.safe_load(arg.get("Value", ""))
            except yaml.YAMLError:
                return f"{node.get('NodeType')}-{node.get('Id')}"
    return "node unknown"


class GlueMetadataProbe:
    """Metadata-only probe over the AWS Glue Data Catalog and Glue jobs.

    Reuses the connector's fetch, never its policy: no getter applies
    database_pattern, table_pattern, ignore_resource_links or the catalog_id
    check, so a name ingestion would drop is reported for `probe filter` to
    explain rather than hidden.
    """

    warnings: List[str]

    def __init__(self, config: GlueSourceConfig) -> None:
        self._config = config
        self._client: Optional["GlueClient"] = None
        self._source: Optional[GlueSource] = None
        self.warnings = []

    @classmethod
    def for_config(cls, config: GlueSourceConfig) -> "GlueMetadataProbe":
        # No client here. Building one resolves the session, which with
        # aws_role calls sts:AssumeRole, and the framework reports a failure
        # raised from for_config with its raw text.
        return cls(config)

    def __enter__(self) -> "GlueMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        # The S3 client is left open: get_s3_client memoizes it on the config.
        if self._client is not None:
            self._client.close()

    @property
    def probe_report(self) -> Optional[GlueSourceReport]:
        """The report ingestion's job-DAG helpers write to (`job_nodes`), so
        their warnings and failures are folded into the result."""
        return self._source.report if self._source is not None else None

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

    def _glue(self) -> "GlueClient":
        if self._client is None:
            with aws_call("resolving AWS credentials for Glue"):
                self._client = self._config.get_glue_client()
        return self._client

    def _ingestion_source(self) -> GlueSource:
        if self._source is None:
            glue = self._glue()
            with aws_call("resolving AWS credentials for S3"):
                s3 = self._config.get_s3_client()
            self._source = GlueSource.for_probe(
                self._config, glue_client=glue, s3_client=s3
            )
        return self._source

    def _catalog_label(self) -> str:
        if self._config.catalog_id:
            return f"Glue catalog {self._config.catalog_id}"
        return "the Glue catalog of the recipe's AWS account"

    def _list_databases(self, limit: Optional[int]) -> List[Dict[str, Any]]:
        with aws_call("glue:GetDatabases"):
            pages = (
                self._glue()
                .get_paginator("get_databases")
                .paginate(**glue_catalog_kwargs(self._config.catalog_id))
            )
            return _take(pages.search("DatabaseList"), limit)

    def _database(self, name: str) -> Dict[str, Any]:
        """One database's record, from the listing ingestion reads.

        GetDatabases rather than GetDatabase: ingestion's documented policy
        grants the former only, and the record carries the TargetDatabase and
        CatalogId facts the recipe's non-pattern rules read.
        """
        with aws_call("glue:GetDatabases"):
            pages = (
                self._glue()
                .get_paginator("get_databases")
                .paginate(**glue_catalog_kwargs(self._config.catalog_id))
            )
            found = next(
                (db for db in pages.search("DatabaseList") if db.get("Name") == name),
                None,
            )
        if found is None:
            raise ValueError(
                f"no database named '{name}' in {self._catalog_label()}; "
                f"`probe run databases` lists them"
            )
        return found

    @probe_method(kind=DatasetContainerSubTypes.DATABASE, row_limit_param="limit")
    def databases(self, limit: int = 500) -> List[Dict[str, object]]:
        """Databases in the Glue Data Catalog the recipe reads (the calling
        account's, or catalog_id's when set), in catalog order. Includes
        databases database_pattern, ignore_resource_links or the catalog_id
        check would drop -- a dropped database is reported, not hidden, so
        `probe filter --from-run` can explain it. Each record is name,
        catalog_id (the owning account Glue reports), resource_link (a Lake
        Formation resource link to another catalog's database) and, for a
        link, target as "<account>/<database>". Database parameters and
        descriptions are withheld. Metadata only."""
        return [_database_record(db) for db in self._list_databases(limit)]

    def _list_tables(
        self, database: str, limit: Optional[int], action: str
    ) -> List[Dict[str, Any]]:
        """One database's GetTables listing, paged as get_tables_from_database
        pages it. An access denial raises ProbeSoftError for the caller to
        record: ingestion skips such a database's tables with a warning."""
        with aws_call(action, soft_on_denied=True):
            pages = (
                self._glue()
                .get_paginator("get_tables")
                .paginate(
                    DatabaseName=database,
                    **glue_catalog_kwargs(self._config.catalog_id),
                )
            )
            return _take(pages.search("TableList"), limit)

    def _note_database_rules(self, database: Mapping[str, Any]) -> None:
        name = database["Name"]
        if self._config.ignore_resource_links and database.get("TargetDatabase"):
            self._warn(
                f"database '{name}' is a Lake Formation resource link and "
                f"ignore_resource_links is true, so ingestion never lists it "
                f"or any table in it"
            )
        owner = database.get("CatalogId")
        if self._config.catalog_id and owner and owner != self._config.catalog_id:
            self._warn(
                f"database '{name}' belongs to catalog {owner}, not catalog_id "
                f"{self._config.catalog_id}, so ingestion drops it and every "
                f"table in it"
            )

    @probe_method(
        kind=DatasetSubTypes.TABLE,
        row_limit_param="limit",
        parent_params=("database",),
    )
    def tables(self, database: str, limit: int = 500) -> List[Dict[str, object]]:
        """Tables and views in one Glue database, in catalog order, including
        ones table_pattern or ignore_resource_links would drop -- a dropped
        table is reported, not hidden. table_pattern is matched against
        "<database>.<table>" for tables and views alike; `probe filter` builds
        that from the database this command reports as the parent. subtype is
        the DataHub subtype ingestion gives each (View for VIRTUAL_VIEW,
        otherwise Table). Each record also carries the facts the recipe's
        non-pattern rules read -- whether the table is itself a Lake Formation
        resource link, and whether its database is one or belongs to another
        catalog -- so judge this output with `probe filter --from-run`. Lake
        Formation hides tables this principal may not describe; they are
        absent here exactly as they are absent from ingestion. When the
        listing is denied outright, ingestion skips this database's tables
        with a warning, and so does this command. Table parameters, view SQL,
        storage and SerDe settings and the owner are withheld."""
        record = self._database(database)
        self._note_database_rules(record)
        action = f"glue:GetTables on database '{database}'"
        try:
            tables = self._list_tables(database, limit, action)
        except ProbeSoftError as exc:
            self._warn(f"{exc}. {_DENIED_TABLES_SUFFIX}")
            return []
        return [_table_record(table, record) for table in tables]

    @probe_method()
    def columns(self, database: str, table: str) -> List[Dict[str, object]]:
        """Columns of one Glue table or view as the catalog stores them --
        name, Hive type, comment, and partition_key for partition columns --
        in the order ingestion emits them (storage-descriptor columns, then
        partition keys). Found through the same glue:GetTables listing
        ingestion reads, so it needs no permission ingestion does not.
        Column parameters are withheld. A resource link has no columns of its
        own, and a Delta table may keep its real schema in table parameters;
        both are reported as warnings, since ingestion reads those schemas
        from elsewhere."""
        action = f"glue:GetTables on database '{database}'"
        try:
            listed = self._list_tables(database, None, action)
        except ProbeSoftError as exc:
            self._warn(f"{exc}. {_DENIED_TABLES_SUFFIX}")
            return []
        found = next((t for t in listed if t.get("Name") == table), None)
        if found is None:
            raise ValueError(
                f"no table '{table}' in database '{database}'; `probe run "
                f"tables --database {database}` lists them"
            )
        qualified = f"{database}.{table}"
        if "TargetTable" in found:
            self._warn(
                f"'{qualified}' is a Lake Formation resource link and has no "
                f"columns of its own; with resolve_resource_link_schema on, "
                f"ingestion takes them from the owning table"
            )
        descriptor = found.get("StorageDescriptor") or {}
        if not descriptor:
            if "TargetTable" not in found:
                self._warn(
                    f"'{qualified}' has no storage descriptor, so ingestion "
                    f"emits no schema for it"
                )
            return []
        if self._ingestion_source()._is_delta_schema(found):
            self._warn(
                f"'{qualified}' keeps its Delta schema in table parameters and "
                f"extract_delta_schema_from_parameters is on, so ingestion "
                f"reads its columns from there; these are the catalog's "
                f"placeholder columns"
            )
        columns = [
            _column_record(column, partition_key=False)
            for column in descriptor.get("Columns") or []
        ]
        columns.extend(
            _column_record(key, partition_key=True)
            for key in found.get("PartitionKeys") or []
        )
        return columns

    def _list_jobs(self, limit: Optional[int]) -> List[Dict[str, Any]]:
        # No CatalogId: get_all_jobs pages GetJobs without one, because the
        # job API is not cross-account.
        with aws_call("glue:GetJobs"):
            pages = self._glue().get_paginator("get_jobs").paginate()
            return _take(pages.search("Jobs"), limit)

    def _find_job(self, name: str) -> Dict[str, Any]:
        # GetJobs rather than GetJob: ingestion's policy grants only the former.
        found = next(
            (j for j in self._list_jobs(None) if j.get("Name") == name), None
        )
        if found is None:
            raise ValueError(
                f"no Glue job named '{name}' in this account and region; "
                f"`probe run jobs` lists them"
            )
        return found

    @probe_method(kind=FlowContainerSubTypes.GLUE_JOB, row_limit_param="limit")
    def jobs(self, limit: int = 500) -> List[Dict[str, object]]:
        """Glue jobs in the recipe's AWS account and region, in API order.
        Ingestion emits each as a DataFlow (subtype Job) when
        extract_transforms is on; nothing else filters them -- there is no
        job pattern. flow_urn is the URN ingestion gives it. Glue's job API is
        not cross-account, so with catalog_id set these are still the calling
        account's jobs, exactly as in ingestion. Each record is name,
        flow_urn, command (glueetl, pythonshell, gluestreaming...),
        script_location, role, glue_version, created_on and
        last_modified_on. Job arguments are withheld: Glue jobs routinely pass
        connection passwords and tokens as arguments. `job_nodes` lists the
        DataJobs one job becomes."""
        jobs = self._list_jobs(limit)
        if not self._config.extract_transforms:
            self._warn(
                "extract_transforms is off, so ingestion emits none of these "
                "jobs; they are listed for inspection only"
            )
        if self._config.catalog_id:
            self._warn(
                "catalog_id does not apply to jobs: Glue's job API is not "
                "cross-account, so these are the calling account's jobs, and "
                "ingestion emits them under this recipe"
            )
        return [_job_record(job, self._config) for job in jobs]

    def _job_dag_nodes(
        self, source: GlueSource, job: str, script_location: str, flow_urn: str
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        """The processed DAG nodes of one job, through ingestion's helpers, or
        None when ingestion would emit the job as a single DataJob."""
        # Both helpers record their own expected failures on the report
        # (probe_report); anything else is translated here.
        with aws_call(f"s3:GetObject / glue:GetDataflowGraph for job '{job}'"):
            script = source.get_dataflow_script(script_location, flow_urn)
            dag = (
                source.get_dataflow_graph(script, script_location, flow_urn)
                if script
                else None
            )
        if dag is None:
            return None
        # aws_call outside the try, so only boto errors reach it and the
        # messages written below are not re-translated.
        with aws_call(f"glue:GetConnection for job '{job}'"):
            try:
                nodes = source.process_dataflow_graph(dag, flow_urn)
            except (ClientError, BotoCoreError):
                raise
            # Every rewrite below is `from None`: the originals embed node
            # args, which hold connection options and can hold passwords --
            # ingestion's ValueError formats them in, and PyYAML's error quotes
            # a window of the value it could not parse.
            except yaml.YAMLError:
                raise ValueError(
                    f"job '{job}' has a DAG node whose arguments are not valid "
                    f"YAML ({_unparseable_node(dag)}), so ingestion fails on "
                    f"this job; the job script was likely edited by hand"
                ) from None
            except ValueError as exc:
                raise ValueError(self._dag_value_error(job, exc)) from None
            except Exception as exc:
                raise ProbeInternalError(
                    f"resolving the DAG of job '{job}' failed inside the "
                    f"connector ({type(exc).__name__}); this is a defect, not a "
                    f"problem with the arguments"
                ) from None
        return nodes

    def _dag_value_error(self, job: str, exc: ValueError) -> str:
        unrecognized = str(exc).startswith(_UNRECOGNIZED_NODE_PREFIX)
        if unrecognized and not self._config.ignore_unsupported_connectors:
            return (
                f"job '{job}' has a data source or sink ingestion does not "
                f"recognise, and ignore_unsupported_connectors is false, so "
                f"ingestion fails on this job; set it to true to skip such "
                f"nodes with a warning"
            )
        return (
            f"job '{job}' has a DAG node ingestion cannot resolve "
            f"({type(exc).__name__}), so ingestion fails on this job"
        )

    @probe_method()
    def job_nodes(self, job: str) -> List[Dict[str, object]]:
        """The DataJobs one Glue job becomes, through ingestion's own path:
        the job script is read from S3 (never returned), Glue's
        GetDataflowGraph turns it into a DAG, and each node is resolved as
        ingestion resolves it. Every node is listed; emitted_as_datajob says
        which ones ingestion emits (sources and sinks become the input/output
        datasets of the nodes they feed instead), datajob_name and urn are
        what it emits them as, and input_datasets/output_datasets are the
        lineage URNs. A job whose script is missing, unreadable or not
        parseable by Glue becomes a single DataJob named after the job, with
        the reason as a warning. Node arguments (connection options, S3
        paths, SQL) and the script itself are withheld. Nothing filters
        DataJobs; they follow their job's verdict."""
        record = self._find_job(job)
        name = record["Name"]
        source = self._ingestion_source()
        flow_urn = mce_builder.make_data_flow_urn(
            orchestrator=source.platform, flow_id=name, cluster=source.env
        )
        script_location = (record.get("Command") or {}).get("ScriptLocation")
        nodes: Optional[Dict[str, Dict[str, Any]]] = None
        if script_location is None:
            self._warn(
                f"job '{job}' has no script location, so ingestion cannot read "
                f"its DAG and emits one DataJob named after the job"
            )
        else:
            nodes = self._job_dag_nodes(source, job, script_location, flow_urn)
        if not nodes:
            return [
                {
                    "id": None,
                    "node_type": None,
                    "emitted_as_datajob": True,
                    "datajob_name": name,
                    "urn": mce_builder.make_data_job_urn_with_flow(
                        flow_urn, job_id=name
                    ),
                    "input_datasets": [],
                    "output_datasets": [],
                    "input_datajobs": [],
                }
            ]
        return [_node_record(node, name) for node in nodes.values()]
