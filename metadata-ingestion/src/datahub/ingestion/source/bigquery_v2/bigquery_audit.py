import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar, Dict, List, Optional, Pattern, Tuple, Union

from dateutil import parser

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.utilities.parsing_util import (
    get_first_missing_key,
    get_first_missing_key_any,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn

AuditLogEntry = Any

# BigQueryAuditMetadata is the v2 format in which audit logs are exported to BigQuery
BigQueryAuditMetadata = Any

logger: logging.Logger = logging.getLogger(__name__)

# Regexp for sharded tables.
# A sharded table is a table that has a suffix of the form _yyyymmdd or yyyymmdd, where yyyymmdd is a date.
# The regexp checks for valid dates in the suffix (e.g. 20200101, 20200229, 20201231) and if the date is not valid
# then it is not a sharded table.
_BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX = (
    "((.+\\D)[_$]?)?(\\d\\d\\d\\d(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01]))$"
)


@dataclass(frozen=True, order=True)
class BigqueryTableIdentifier:
    project_id: str
    dataset: str
    table: str

    # Note: this regex may get overwritten by the sharded_table_pattern config.
    # The class-level constant, however, will not be overwritten.
    _BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX: ClassVar[
        str
    ] = _BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX
    _BIGQUERY_WILDCARD_REGEX: ClassVar[str] = "((_(\\d+)?)\\*$)|\\*$"
    _BQ_SHARDED_TABLE_SUFFIX: str = "_yyyymmdd"

    @staticmethod
    def get_table_and_shard(table_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Args:
            table_name:
                table name (in form <table-id> or <table-prefix>_<shard>`, optionally prefixed with <project-id>.<dataset-id>)
                See https://cloud.google.com/bigquery/docs/reference/standard-sql/wildcard-table-reference
                If table_name is fully qualified, i.e. prefixed with <project-id>.<dataset-id> then the special case of
                dataset as a sharded table, i.e. table-id itself as shard can not be detected.
        Returns:
            (table_name_without_shard, shard):
                In case of non-sharded tables, returns (<table-id>, None)
                In case of sharded tables, returns (<table-prefix>, shard)
        """
        new_table_name = table_name
        match = re.match(
            BigqueryTableIdentifier._BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX,
            table_name,
            re.IGNORECASE,
        )
        if match:
            shard: str = match[3]
            if shard:
                if table_name.endswith(shard):
                    new_table_name = table_name[: -len(shard)]

            new_table_name = (
                new_table_name.rstrip("_") if new_table_name else new_table_name
            )
            if new_table_name.endswith("."):
                new_table_name = table_name
            return (new_table_name, shard) if new_table_name else (None, shard)
        return new_table_name, None

    @classmethod
    def from_string_name(cls, table: str) -> "BigqueryTableIdentifier":
        parts = table.split(".", maxsplit=2)
        # If the table name contains dollar sign, it is a reference to a partitioned table and we have to strip it
        table = parts[2].split("$", 1)[0]
        return cls(parts[0], parts[1], table)

    def raw_table_name(self):
        return f"{self.project_id}.{self.dataset}.{self.table}"

    def get_table_display_name(self) -> str:
        """
        Returns table display name to be used for DataHub
            - removes shard suffix (table_yyyymmdd-> table)
            - removes wildcard part (table_yyyy* -> table)
            - remove time decorator (table@1624046611000 -> table)
            - removes partition ids (table$20210101 -> table or table$__UNPARTITIONED__ -> table)
        """
        # if table name ends in _* or * or _yyyy* or _yyyymm* then we strip it as that represents a query on a sharded table
        shortened_table_name = re.sub(self._BIGQUERY_WILDCARD_REGEX, "", self.table)

        matches = BigQueryTableRef.SNAPSHOT_TABLE_REGEX.match(shortened_table_name)
        if matches:
            shortened_table_name = matches.group(1)
            logger.debug(
                f"Found table snapshot. Using {shortened_table_name} as the table name."
            )

        if "$" in shortened_table_name:
            shortened_table_name = shortened_table_name.split("$", maxsplit=1)[0]
            logger.debug(
                f"Found partitioned table. Using {shortened_table_name} as the table name."
            )

        table_name, _ = self.get_table_and_shard(shortened_table_name)
        return table_name or self.dataset

    def get_table_name(self) -> str:
        """
        Returns qualified table name to be used for DataHub
            - removes shard suffix (table_yyyymmdd-> table)
            - removes wildcard part (table_yyyy* -> table)
            - remove time decorator (table@1624046611000 -> table)
        """
        table_name: str = (
            f"{self.project_id}.{self.dataset}.{self.get_table_display_name()}"
        )
        if self.is_sharded_table():
            table_name += BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX
        return table_name

    def is_sharded_table(self) -> bool:
        _, shard = self.get_table_and_shard(self.table)
        if shard:
            return True

        if re.match(
            f".*({BigqueryTableIdentifier._BIGQUERY_WILDCARD_REGEX})",
            self.raw_table_name(),
            re.IGNORECASE,
        ):
            return True

        return False

    def __str__(self) -> str:
        return self.get_table_name()


@dataclass(frozen=True, order=True)
class BigQueryTableRef:
    # Handle table time travel. See https://cloud.google.com/bigquery/docs/time-travel
    # See https://cloud.google.com/bigquery/docs/table-decorators#time_decorators
    SNAPSHOT_TABLE_REGEX: ClassVar[Pattern[str]] = re.compile(
        "^(.+)@(-?\\d{1,13})(-(-?\\d{1,13})?)?$"
    )

    table_identifier: BigqueryTableIdentifier

    @classmethod
    def from_bigquery_table(cls, table: BigqueryTableIdentifier) -> "BigQueryTableRef":
        return cls(
            BigqueryTableIdentifier(table.project_id, table.dataset, table.table)
        )

    @classmethod
    def from_spec_obj(cls, spec: dict) -> "BigQueryTableRef":
        for key in ["projectId", "datasetId", "tableId"]:
            if key not in spec.keys():
                raise ValueError(f"invalid BigQuery table reference dict: {spec}")

        return cls(
            # spec dict always has to have projectId, datasetId, tableId otherwise it is an invalid spec
            BigqueryTableIdentifier(
                spec["projectId"], spec["datasetId"], spec["tableId"]
            )
        )

    @classmethod
    def from_string_name(cls, ref: str) -> "BigQueryTableRef":
        parts = ref.split("/")
        if (
            len(parts) != 6
            or parts[0] != "projects"
            or parts[2] != "datasets"
            or parts[4] != "tables"
        ):
            raise ValueError(f"invalid BigQuery table reference: {ref}")
        return cls(BigqueryTableIdentifier(parts[1], parts[3], parts[5]))

    @classmethod
    def from_urn(cls, urn: str) -> "BigQueryTableRef":
        """Raises: ValueError if urn is not a valid BigQuery table URN."""
        dataset_urn = DatasetUrn.create_from_string(urn)
        split = dataset_urn.name.rsplit(".", 3)
        if len(split) == 3:
            project, dataset, table = split
        else:
            _, project, dataset, table = split
        return cls(BigqueryTableIdentifier(project, dataset, table))

    def is_temporary_table(self, prefixes: List[str]) -> bool:
        for prefix in prefixes:
            if self.table_identifier.dataset.startswith(prefix):
                return True
        # Temporary tables will have a dataset that begins with an underscore.
        return False

    def get_sanitized_table_ref(self) -> "BigQueryTableRef":
        sanitized_table = self.table_identifier.get_table_name()
        # Handle partitioned and sharded tables.
        return BigQueryTableRef(
            BigqueryTableIdentifier.from_string_name(sanitized_table)
        )

    def to_urn(self, env: str) -> str:
        return make_dataset_urn(
            "bigquery",
            f"{self.table_identifier.project_id}.{self.table_identifier.dataset}.{self.table_identifier.table}",
            env,
        )

    def __str__(self) -> str:
        return f"projects/{self.table_identifier.project_id}/datasets/{self.table_identifier.dataset}/tables/{self.table_identifier.table}"


@dataclass
class QueryEvent:
    """
    A container class for a query job completion event.
    See https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/AuditData#JobCompletedEvent.
    """

    timestamp: datetime
    actor_email: str
    query: str
    statementType: str
    project_id: str

    job_name: Optional[str] = None
    destinationTable: Optional[BigQueryTableRef] = None
    referencedTables: List[BigQueryTableRef] = field(default_factory=list)
    referencedViews: List[BigQueryTableRef] = field(default_factory=list)
    payload: Optional[Dict] = None
    stats: Optional[Dict] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    billed_bytes: Optional[int] = None
    default_dataset: Optional[str] = None
    numAffectedRows: Optional[int] = None

    query_on_view: bool = False

    @staticmethod
    def get_missing_key_entry(entry: AuditLogEntry) -> Optional[str]:
        return get_first_missing_key(
            inp_dict=entry.payload, keys=["serviceData", "jobCompletedEvent", "job"]
        )

    @staticmethod
    def get_missing_key_entry_v2(entry: AuditLogEntry) -> Optional[str]:
        return get_first_missing_key(
            inp_dict=entry.payload, keys=["metadata", "jobChange", "job"]
        )

    @staticmethod
    def _job_name_ref(project: str, jobId: str) -> Optional[str]:
        if project and jobId:
            return f"projects/{project}/jobs/{jobId}"
        return None

    @staticmethod
    def _get_project_id_from_job_name(job_name: str) -> str:
        project_id_pattern = r"projects\/(.*)\/jobs\/.*"
        matches = re.match(project_id_pattern, job_name, re.MULTILINE)
        if matches:
            return matches.group(1)
        else:
            raise ValueError(f"Unable to get project_id from jobname: {job_name}")

    @classmethod
    def from_entry(
        cls, entry: AuditLogEntry, debug_include_full_payloads: bool = False
    ) -> "QueryEvent":
        job: Dict = entry.payload["serviceData"]["jobCompletedEvent"]["job"]
        job_query_conf: Dict = job["jobConfiguration"]["query"]
        # basic query_event
        query_event = QueryEvent(
            timestamp=entry.timestamp,
            actor_email=entry.payload["authenticationInfo"]["principalEmail"],
            query=job_query_conf["query"],
            job_name=QueryEvent._job_name_ref(
                job.get("jobName", {}).get("projectId"),
                job.get("jobName", {}).get("jobId"),
            ),
            project_id=job.get("jobName", {}).get("projectId"),
            default_dataset=job_query_conf["defaultDataset"]
            if job_query_conf["defaultDataset"]
            else None,
            start_time=parser.parse(job["jobStatistics"]["startTime"])
            if job["jobStatistics"]["startTime"]
            else None,
            end_time=parser.parse(job["jobStatistics"]["endTime"])
            if job["jobStatistics"]["endTime"]
            else None,
            numAffectedRows=int(job["jobStatistics"]["queryOutputRowCount"])
            if "queryOutputRowCount" in job["jobStatistics"]
            and job["jobStatistics"]["queryOutputRowCount"]
            else None,
            statementType=job_query_conf.get("statementType", "UNKNOWN"),
        )
        # destinationTable
        raw_dest_table = job_query_conf.get("destinationTable")
        if raw_dest_table:
            query_event.destinationTable = BigQueryTableRef.from_spec_obj(
                raw_dest_table
            ).get_sanitized_table_ref()
        # statementType
        # referencedTables
        job_stats: Dict = job["jobStatistics"]
        if job_stats.get("totalBilledBytes"):
            query_event.billed_bytes = job_stats["totalBilledBytes"]

        raw_ref_tables = job_stats.get("referencedTables")
        if raw_ref_tables:
            query_event.referencedTables = [
                BigQueryTableRef.from_spec_obj(spec).get_sanitized_table_ref()
                for spec in raw_ref_tables
            ]
        # referencedViews
        raw_ref_views = job_stats.get("referencedViews")
        if raw_ref_views:
            query_event.referencedViews = [
                BigQueryTableRef.from_spec_obj(spec).get_sanitized_table_ref()
                for spec in raw_ref_views
            ]
            query_event.query_on_view = True

        # payload
        query_event.payload = entry.payload if debug_include_full_payloads else None
        if not query_event.job_name:
            logger.debug(
                "jobName from query events is absent. "
                "Auditlog entry - {logEntry}".format(logEntry=entry)
            )

        return query_event

    @staticmethod
    def get_missing_key_exported_bigquery_audit_metadata(
        row: BigQueryAuditMetadata,
    ) -> Optional[str]:
        missing_key = get_first_missing_key_any(
            row._xxx_field_to_index, ["timestamp", "protoPayload", "metadata"]
        )
        if not missing_key:
            missing_key = get_first_missing_key_any(
                json.loads(row["metadata"]), ["jobChange"]
            )
        return missing_key

    @classmethod
    def from_exported_bigquery_audit_metadata(
        cls, row: BigQueryAuditMetadata, debug_include_full_payloads: bool = False
    ) -> "QueryEvent":
        payload: Dict = row["protoPayload"]
        metadata: Dict = json.loads(row["metadata"])
        job: Dict = metadata["jobChange"]["job"]
        query_config: Dict = job["jobConfig"]["queryConfig"]
        query_stats: Dict = job["jobStats"]["queryStats"]

        # basic query_event
        query_event = QueryEvent(
            timestamp=row["timestamp"],
            actor_email=payload["authenticationInfo"]["principalEmail"],
            query=query_config["query"],
            job_name=job["jobName"],
            project_id=QueryEvent._get_project_id_from_job_name(job["jobName"]),
            default_dataset=query_config["defaultDataset"]
            if query_config.get("defaultDataset")
            else None,
            start_time=parser.parse(job["jobStats"]["startTime"])
            if job["jobStats"]["startTime"]
            else None,
            end_time=parser.parse(job["jobStats"]["endTime"])
            if job["jobStats"]["endTime"]
            else None,
            numAffectedRows=int(query_stats["outputRowCount"])
            if query_stats.get("outputRowCount")
            else None,
            statementType=query_config.get("statementType", "UNKNOWN"),
        )
        # jobName
        query_event.job_name = job.get("jobName")
        # destinationTable
        raw_dest_table = query_config.get("destinationTable")
        if raw_dest_table:
            query_event.destinationTable = BigQueryTableRef.from_string_name(
                raw_dest_table
            ).get_sanitized_table_ref()
        # referencedTables
        raw_ref_tables = query_stats.get("referencedTables")
        if raw_ref_tables:
            query_event.referencedTables = [
                BigQueryTableRef.from_string_name(spec).get_sanitized_table_ref()
                for spec in raw_ref_tables
            ]
        # referencedViews
        raw_ref_views = query_stats.get("referencedViews")
        if raw_ref_views:
            query_event.referencedViews = [
                BigQueryTableRef.from_string_name(spec).get_sanitized_table_ref()
                for spec in raw_ref_views
            ]
            query_event.query_on_view = True

        # payload
        query_event.payload = payload if debug_include_full_payloads else None

        if not query_event.job_name:
            logger.debug(
                "jobName from query events is absent. "
                "BigQueryAuditMetadata entry - {logEntry}".format(logEntry=row)
            )

        if query_stats.get("totalBilledBytes"):
            query_event.billed_bytes = int(query_stats["totalBilledBytes"])

        return query_event

    @classmethod
    def from_entry_v2(
        cls, row: BigQueryAuditMetadata, debug_include_full_payloads: bool = False
    ) -> "QueryEvent":
        payload: Dict = row.payload
        metadata: Dict = payload["metadata"]
        job: Dict = metadata["jobChange"]["job"]
        query_config: Dict = job["jobConfig"]["queryConfig"]
        query_stats: Dict = job["jobStats"]["queryStats"]

        # basic query_event
        query_event = QueryEvent(
            job_name=job["jobName"],
            project_id=QueryEvent._get_project_id_from_job_name(job["jobName"]),
            timestamp=row.timestamp,
            actor_email=payload["authenticationInfo"]["principalEmail"],
            query=query_config["query"],
            default_dataset=query_config["defaultDataset"]
            if "defaultDataset" in query_config and query_config["defaultDataset"]
            else None,
            start_time=parser.parse(job["jobStats"]["startTime"])
            if job["jobStats"]["startTime"]
            else None,
            end_time=parser.parse(job["jobStats"]["endTime"])
            if job["jobStats"]["endTime"]
            else None,
            numAffectedRows=int(query_stats["outputRowCount"])
            if "outputRowCount" in query_stats and query_stats["outputRowCount"]
            else None,
            statementType=query_config.get("statementType", "UNKNOWN"),
        )
        query_event.job_name = job.get("jobName")
        # destinationTable
        raw_dest_table = query_config.get("destinationTable")
        if raw_dest_table:
            query_event.destinationTable = BigQueryTableRef.from_string_name(
                raw_dest_table
            ).get_sanitized_table_ref()
        # statementType
        # referencedTables
        raw_ref_tables = query_stats.get("referencedTables")
        if raw_ref_tables:
            query_event.referencedTables = [
                BigQueryTableRef.from_string_name(spec).get_sanitized_table_ref()
                for spec in raw_ref_tables
            ]
        # referencedViews
        raw_ref_views = query_stats.get("referencedViews")
        if raw_ref_views:
            query_event.referencedViews = [
                BigQueryTableRef.from_string_name(spec).get_sanitized_table_ref()
                for spec in raw_ref_views
            ]
            query_event.query_on_view = True

        # payload
        query_event.payload = payload if debug_include_full_payloads else None

        if not query_event.job_name:
            logger.debug(
                "jobName from query events is absent. "
                "BigQueryAuditMetadata entry - {logEntry}".format(logEntry=row)
            )

        if query_stats.get("totalBilledBytes"):
            query_event.billed_bytes = int(query_stats["totalBilledBytes"])

        return query_event


@dataclass
class ReadEvent:
    """
    A container class for data from a TableDataRead event.
    See https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#BigQueryAuditMetadata.TableDataRead.
    """

    timestamp: datetime
    actor_email: str

    resource: BigQueryTableRef
    fieldsRead: List[str]
    readReason: Optional[str]
    jobName: Optional[str]

    payload: Any

    from_query: bool = False

    # We really should use composition here since the query isn't actually
    # part of the read event, but this solution is just simpler.
    # query: Optional["QueryEvent"] = None  # populated via join

    @classmethod
    def get_missing_key_entry(cls, entry: AuditLogEntry) -> Optional[str]:
        return (
            get_first_missing_key(
                inp_dict=entry.payload, keys=["metadata", "tableDataRead"]
            )
            or get_first_missing_key(
                inp_dict=entry.payload, keys=["authenticationInfo", "principalEmail"]
            )
            or get_first_missing_key(inp_dict=entry.payload, keys=["resourceName"])
        )

    @staticmethod
    def get_missing_key_exported_bigquery_audit_metadata(
        row: BigQueryAuditMetadata,
    ) -> Optional[str]:
        missing_key = get_first_missing_key_any(dict(row), ["metadata"])
        if not missing_key:
            metadata = json.loads(row["metadata"])
            missing_key = get_first_missing_key_any(metadata, ["tableDataRead"])

        return missing_key

    @classmethod
    def from_entry(
        cls, entry: AuditLogEntry, debug_include_full_payloads: bool = False
    ) -> "ReadEvent":
        user = entry.payload["authenticationInfo"]["principalEmail"]
        resourceName = entry.payload["resourceName"]
        readInfo = entry.payload["metadata"]["tableDataRead"]

        fields = readInfo.get("fields", [])

        # https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.TableDataRead.Reason
        readReason = readInfo.get("reason")
        jobName = None
        if readReason == "JOB":
            jobName = readInfo.get("jobName")

        resource = BigQueryTableRef.from_string_name(
            resourceName
        ).get_sanitized_table_ref()

        readEvent = ReadEvent(
            actor_email=user,
            timestamp=entry.timestamp,
            resource=resource,
            fieldsRead=fields,
            readReason=readReason,
            jobName=jobName,
            payload=entry.payload if debug_include_full_payloads else None,
        )
        if readReason == "JOB" and not jobName:
            logger.debug(
                "jobName from read events is absent when readReason is JOB. "
                "Auditlog entry - {logEntry}".format(logEntry=entry)
            )
        return readEvent

    @classmethod
    def from_query_event(
        cls,
        read_resource: BigQueryTableRef,
        query_event: QueryEvent,
        debug_include_full_payloads: bool = False,
    ) -> "ReadEvent":
        return ReadEvent(
            actor_email=query_event.actor_email,
            timestamp=query_event.timestamp,
            resource=read_resource,
            fieldsRead=[],
            readReason="JOB",
            jobName=query_event.job_name,
            payload=query_event.payload if debug_include_full_payloads else None,
            from_query=True,
        )

    @classmethod
    def from_exported_bigquery_audit_metadata(
        cls, row: BigQueryAuditMetadata, debug_include_full_payloads: bool = False
    ) -> "ReadEvent":
        payload = row["protoPayload"]
        user = payload["authenticationInfo"]["principalEmail"]
        resourceName = payload["resourceName"]
        metadata = json.loads(row["metadata"])
        readInfo = metadata["tableDataRead"]

        fields = readInfo.get("fields", [])

        # https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.TableDataRead.Reason
        readReason = readInfo.get("reason")
        jobName = None
        if readReason == "JOB":
            jobName = readInfo.get("jobName")

        resource = BigQueryTableRef.from_string_name(
            resourceName
        ).get_sanitized_table_ref()

        readEvent = ReadEvent(
            actor_email=user,
            timestamp=row["timestamp"],
            resource=resource,
            fieldsRead=fields,
            readReason=readReason,
            jobName=jobName,
            payload=payload if debug_include_full_payloads else None,
        )
        if readReason == "JOB" and not jobName:
            logger.debug(
                "jobName from read events is absent when readReason is JOB. "
                "Auditlog entry - {logEntry}".format(logEntry=row)
            )
        return readEvent


@dataclass()
class AuditEvent:
    read_event: Optional[ReadEvent] = None
    query_event: Optional[QueryEvent] = None

    @classmethod
    def create(cls, event: Union[ReadEvent, QueryEvent]) -> "AuditEvent":
        if isinstance(event, QueryEvent):
            return AuditEvent(query_event=event)
        elif isinstance(event, ReadEvent):
            return AuditEvent(read_event=event)
        else:
            raise TypeError(f"Cannot create AuditEvent: {event}")
