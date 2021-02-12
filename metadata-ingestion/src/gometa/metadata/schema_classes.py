import json
import os.path
import decimal
import datetime
import six
from avrogen.dict_wrapper import DictWrapper
from avrogen import avrojson
from avro.schema import RecordSchema, SchemaFromJSONData as make_avsc_object
from avro import schema as avro_schema
from typing import List, Dict, Union, Optional, overload


def __read_file(file_name):
    with open(file_name, "r") as f:
        return f.read()
        

def __get_names_and_schema(json_str):
    names = avro_schema.Names()
    schema = make_avsc_object(json.loads(json_str), names)
    return names, schema


SCHEMA_JSON_STR = __read_file(os.path.join(os.path.dirname(__file__), "schema.avsc"))


__NAMES, SCHEMA = __get_names_and_schema(SCHEMA_JSON_STR)
__SCHEMAS: Dict[str, RecordSchema] = {}


def get_schema_type(fullname):
    return __SCHEMAS.get(fullname)
    
    
__SCHEMAS = dict((n.fullname.lstrip("."), n) for n in six.itervalues(__NAMES.names))

class KafkaAuditHeaderClass(DictWrapper):
    """This header records information about the context of an event as it is emitted into kafka and is intended to be used by the kafka audit application.  For more information see go/kafkaauditheader"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.events.KafkaAuditHeader")
    
    @overload
    def __init__(self,
        time: Optional[int]=None,
        server: Optional[str]=None,
        instance: Optional[Union[None, str]]=None,
        appName: Optional[str]=None,
        messageId: Optional[bytes]=None,
        auditVersion: Optional[Union[None, int]]=None,
        fabricUrn: Optional[Union[None, str]]=None,
        clusterConnectionString: Optional[Union[None, str]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(KafkaAuditHeaderClass, self).__init__({})
        self.time = int()
        self.server = str()
        self.instance = self.RECORD_SCHEMA.field_map["instance"].default
        self.appName = str()
        self.messageId = str()
        self.auditVersion = self.RECORD_SCHEMA.field_map["auditVersion"].default
        self.fabricUrn = self.RECORD_SCHEMA.field_map["fabricUrn"].default
        self.clusterConnectionString = self.RECORD_SCHEMA.field_map["clusterConnectionString"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def time(self) -> int:
        """Getter: The time at which the event was emitted into kafka."""
        return self._inner_dict.get('time')  # type: ignore
    
    
    @time.setter
    def time(self, value: int):
        """Setter: The time at which the event was emitted into kafka."""
        self._inner_dict['time'] = value
    
    
    @property
    def server(self) -> str:
        """Getter: The fully qualified name of the host from which the event is being emitted."""
        return self._inner_dict.get('server')  # type: ignore
    
    
    @server.setter
    def server(self, value: str):
        """Setter: The fully qualified name of the host from which the event is being emitted."""
        self._inner_dict['server'] = value
    
    
    @property
    def instance(self) -> Union[None, str]:
        """Getter: The instance on the server from which the event is being emitted. e.g. i001"""
        return self._inner_dict.get('instance')  # type: ignore
    
    
    @instance.setter
    def instance(self, value: Union[None, str]):
        """Setter: The instance on the server from which the event is being emitted. e.g. i001"""
        self._inner_dict['instance'] = value
    
    
    @property
    def appName(self) -> str:
        """Getter: The name of the application from which the event is being emitted. see go/appname"""
        return self._inner_dict.get('appName')  # type: ignore
    
    
    @appName.setter
    def appName(self, value: str):
        """Setter: The name of the application from which the event is being emitted. see go/appname"""
        self._inner_dict['appName'] = value
    
    
    @property
    def messageId(self) -> bytes:
        """Getter: A unique identifier for the message"""
        return self._inner_dict.get('messageId')  # type: ignore
    
    
    @messageId.setter
    def messageId(self, value: bytes):
        """Setter: A unique identifier for the message"""
        self._inner_dict['messageId'] = value
    
    
    @property
    def auditVersion(self) -> Union[None, int]:
        """Getter: The version that is being used for auditing. In version 0, the audit trail buckets events into 10 minute audit windows based on the EventHeader timestamp. In version 1, the audit trail buckets events as follows: if the schema has an outer KafkaAuditHeader, use the outer audit header timestamp for bucketing; else if the EventHeader has an inner KafkaAuditHeader use that inner audit header's timestamp for bucketing"""
        return self._inner_dict.get('auditVersion')  # type: ignore
    
    
    @auditVersion.setter
    def auditVersion(self, value: Union[None, int]):
        """Setter: The version that is being used for auditing. In version 0, the audit trail buckets events into 10 minute audit windows based on the EventHeader timestamp. In version 1, the audit trail buckets events as follows: if the schema has an outer KafkaAuditHeader, use the outer audit header timestamp for bucketing; else if the EventHeader has an inner KafkaAuditHeader use that inner audit header's timestamp for bucketing"""
        self._inner_dict['auditVersion'] = value
    
    
    @property
    def fabricUrn(self) -> Union[None, str]:
        """Getter: The fabricUrn of the host from which the event is being emitted. Fabric Urn in the format of urn:li:fabric:{fabric_name}. See go/fabric."""
        return self._inner_dict.get('fabricUrn')  # type: ignore
    
    
    @fabricUrn.setter
    def fabricUrn(self, value: Union[None, str]):
        """Setter: The fabricUrn of the host from which the event is being emitted. Fabric Urn in the format of urn:li:fabric:{fabric_name}. See go/fabric."""
        self._inner_dict['fabricUrn'] = value
    
    
    @property
    def clusterConnectionString(self) -> Union[None, str]:
        """Getter: This is a String that the client uses to establish some kind of connection with the Kafka cluster. The exact format of it depends on specific versions of clients and brokers. This information could potentially identify the fabric and cluster with which the client is producing to or consuming from."""
        return self._inner_dict.get('clusterConnectionString')  # type: ignore
    
    
    @clusterConnectionString.setter
    def clusterConnectionString(self, value: Union[None, str]):
        """Setter: This is a String that the client uses to establish some kind of connection with the Kafka cluster. The exact format of it depends on specific versions of clients and brokers. This information could potentially identify the fabric and cluster with which the client is producing to or consuming from."""
        self._inner_dict['clusterConnectionString'] = value
    
    
class ChartInfoClass(DictWrapper):
    """Information about a chart"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.chart.ChartInfo")
    
    @overload
    def __init__(self,
        title: Optional[str]=None,
        description: Optional[str]=None,
        lastModified: Optional["ChangeAuditStampsClass"]=None,
        chartUrl: Optional[Union[None, str]]=None,
        inputs: Optional[Union[None, List[str]]]=None,
        type: Optional[Union[None, "ChartTypeClass"]]=None,
        access: Optional[Union[None, "AccessLevelClass"]]=None,
        lastRefreshed: Optional[Union[None, int]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(ChartInfoClass, self).__init__({})
        self.title = str()
        self.description = str()
        self.lastModified = ChangeAuditStampsClass()
        self.chartUrl = self.RECORD_SCHEMA.field_map["chartUrl"].default
        self.inputs = self.RECORD_SCHEMA.field_map["inputs"].default
        self.type = self.RECORD_SCHEMA.field_map["type"].default
        self.access = self.RECORD_SCHEMA.field_map["access"].default
        self.lastRefreshed = self.RECORD_SCHEMA.field_map["lastRefreshed"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def title(self) -> str:
        """Getter: Title of the chart"""
        return self._inner_dict.get('title')  # type: ignore
    
    
    @title.setter
    def title(self, value: str):
        """Setter: Title of the chart"""
        self._inner_dict['title'] = value
    
    
    @property
    def description(self) -> str:
        """Getter: Detailed description about the chart"""
        return self._inner_dict.get('description')  # type: ignore
    
    
    @description.setter
    def description(self, value: str):
        """Setter: Detailed description about the chart"""
        self._inner_dict['description'] = value
    
    
    @property
    def lastModified(self) -> "ChangeAuditStampsClass":
        """Getter: Captures information about who created/last modified/deleted this chart and when"""
        return self._inner_dict.get('lastModified')  # type: ignore
    
    
    @lastModified.setter
    def lastModified(self, value: "ChangeAuditStampsClass"):
        """Setter: Captures information about who created/last modified/deleted this chart and when"""
        self._inner_dict['lastModified'] = value
    
    
    @property
    def chartUrl(self) -> Union[None, str]:
        """Getter: URL for the chart. This could be used as an external link on DataHub to allow users access/view the chart"""
        return self._inner_dict.get('chartUrl')  # type: ignore
    
    
    @chartUrl.setter
    def chartUrl(self, value: Union[None, str]):
        """Setter: URL for the chart. This could be used as an external link on DataHub to allow users access/view the chart"""
        self._inner_dict['chartUrl'] = value
    
    
    @property
    def inputs(self) -> Union[None, List[str]]:
        """Getter: Data sources for the chart"""
        return self._inner_dict.get('inputs')  # type: ignore
    
    
    @inputs.setter
    def inputs(self, value: Union[None, List[str]]):
        """Setter: Data sources for the chart"""
        self._inner_dict['inputs'] = value
    
    
    @property
    def type(self) -> Union[None, "ChartTypeClass"]:
        """Getter: Type of the chart"""
        return self._inner_dict.get('type')  # type: ignore
    
    
    @type.setter
    def type(self, value: Union[None, "ChartTypeClass"]):
        """Setter: Type of the chart"""
        self._inner_dict['type'] = value
    
    
    @property
    def access(self) -> Union[None, "AccessLevelClass"]:
        """Getter: Access level for the chart"""
        return self._inner_dict.get('access')  # type: ignore
    
    
    @access.setter
    def access(self, value: Union[None, "AccessLevelClass"]):
        """Setter: Access level for the chart"""
        self._inner_dict['access'] = value
    
    
    @property
    def lastRefreshed(self) -> Union[None, int]:
        """Getter: The time when this chart last refreshed"""
        return self._inner_dict.get('lastRefreshed')  # type: ignore
    
    
    @lastRefreshed.setter
    def lastRefreshed(self, value: Union[None, int]):
        """Setter: The time when this chart last refreshed"""
        self._inner_dict['lastRefreshed'] = value
    
    
class ChartQueryClass(DictWrapper):
    """Information for chart query which is used for getting data of the chart"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.chart.ChartQuery")
    
    @overload
    def __init__(self,
        rawQuery: Optional[str]=None,
        type: Optional["ChartQueryTypeClass"]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(ChartQueryClass, self).__init__({})
        self.rawQuery = str()
        self.type = ChartQueryTypeClass.LOOKML
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def rawQuery(self) -> str:
        """Getter: Raw query to build a chart from input datasets"""
        return self._inner_dict.get('rawQuery')  # type: ignore
    
    
    @rawQuery.setter
    def rawQuery(self, value: str):
        """Setter: Raw query to build a chart from input datasets"""
        self._inner_dict['rawQuery'] = value
    
    
    @property
    def type(self) -> "ChartQueryTypeClass":
        """Getter: Chart query type"""
        return self._inner_dict.get('type')  # type: ignore
    
    
    @type.setter
    def type(self, value: "ChartQueryTypeClass"):
        """Setter: Chart query type"""
        self._inner_dict['type'] = value
    
    
class ChartQueryTypeClass(object):
    # No docs available.
    
    LOOKML = "LOOKML"
    SQL = "SQL"
    
    
class ChartTypeClass(object):
    """The various types of charts"""
    
    BAR = "BAR"
    PIE = "PIE"
    SCATTER = "SCATTER"
    TABLE = "TABLE"
    TEXT = "TEXT"
    
    
class AccessLevelClass(object):
    """The various access levels"""
    
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"
    
    
class AuditStampClass(DictWrapper):
    """Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.AuditStamp")
    
    @overload
    def __init__(self,
        time: Optional[int]=None,
        actor: Optional[str]=None,
        impersonator: Optional[Union[None, str]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(AuditStampClass, self).__init__({})
        self.time = int()
        self.actor = str()
        self.impersonator = self.RECORD_SCHEMA.field_map["impersonator"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def time(self) -> int:
        """Getter: When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."""
        return self._inner_dict.get('time')  # type: ignore
    
    
    @time.setter
    def time(self, value: int):
        """Setter: When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."""
        self._inner_dict['time'] = value
    
    
    @property
    def actor(self) -> str:
        """Getter: The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."""
        return self._inner_dict.get('actor')  # type: ignore
    
    
    @actor.setter
    def actor(self, value: str):
        """Setter: The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."""
        self._inner_dict['actor'] = value
    
    
    @property
    def impersonator(self) -> Union[None, str]:
        """Getter: The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."""
        return self._inner_dict.get('impersonator')  # type: ignore
    
    
    @impersonator.setter
    def impersonator(self, value: Union[None, str]):
        """Setter: The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."""
        self._inner_dict['impersonator'] = value
    
    
class ChangeAuditStampsClass(DictWrapper):
    """Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into various lifecycle stages, and who acted to move it into those lifecycle stages. The recommended best practice is to include this record in your record schema, and annotate its fields as @readOnly in your resource. See https://github.com/linkedin/rest.li/wiki/Validation-in-Rest.li#restli-validation-annotations"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.ChangeAuditStamps")
    
    @overload
    def __init__(self,
        created: Optional["AuditStampClass"]=None,
        lastModified: Optional["AuditStampClass"]=None,
        deleted: Optional[Union[None, "AuditStampClass"]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(ChangeAuditStampsClass, self).__init__({})
        self.created = AuditStampClass()
        self.lastModified = AuditStampClass()
        self.deleted = self.RECORD_SCHEMA.field_map["deleted"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def created(self) -> "AuditStampClass":
        """Getter: An AuditStamp corresponding to the creation of this resource/association/sub-resource"""
        return self._inner_dict.get('created')  # type: ignore
    
    
    @created.setter
    def created(self, value: "AuditStampClass"):
        """Setter: An AuditStamp corresponding to the creation of this resource/association/sub-resource"""
        self._inner_dict['created'] = value
    
    
    @property
    def lastModified(self) -> "AuditStampClass":
        """Getter: An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created"""
        return self._inner_dict.get('lastModified')  # type: ignore
    
    
    @lastModified.setter
    def lastModified(self, value: "AuditStampClass"):
        """Setter: An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created"""
        self._inner_dict['lastModified'] = value
    
    
    @property
    def deleted(self) -> Union[None, "AuditStampClass"]:
        """Getter: An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."""
        return self._inner_dict.get('deleted')  # type: ignore
    
    
    @deleted.setter
    def deleted(self, value: Union[None, "AuditStampClass"]):
        """Setter: An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."""
        self._inner_dict['deleted'] = value
    
    
class CostClass(DictWrapper):
    # No docs available.
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.Cost")
    
    @overload
    def __init__(self,
        costType: Optional["CostTypeClass"]=None,
        cost: Optional["CostCostClass"]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(CostClass, self).__init__({})
        self.costType = CostTypeClass.ORG_COST_TYPE
        self.cost = CostCostClass()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def costType(self) -> "CostTypeClass":
        # No docs available.
        return self._inner_dict.get('costType')  # type: ignore
    
    
    @costType.setter
    def costType(self, value: "CostTypeClass"):
        # No docs available.
        self._inner_dict['costType'] = value
    
    
    @property
    def cost(self) -> "CostCostClass":
        # No docs available.
        return self._inner_dict.get('cost')  # type: ignore
    
    
    @cost.setter
    def cost(self, value: "CostCostClass"):
        # No docs available.
        self._inner_dict['cost'] = value
    
    
class CostCostClass(DictWrapper):
    # No docs available.
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.CostCost")
    
    @overload
    def __init__(self,
        costId: Optional[Union[None, float]]=None,
        costCode: Optional[Union[None, str]]=None,
        fieldDiscriminator: Optional["CostCostDiscriminatorClass"]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(CostCostClass, self).__init__({})
        self.costId = self.RECORD_SCHEMA.field_map["costId"].default
        self.costCode = self.RECORD_SCHEMA.field_map["costCode"].default
        self.fieldDiscriminator = CostCostDiscriminatorClass.costId
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def costId(self) -> Union[None, float]:
        # No docs available.
        return self._inner_dict.get('costId')  # type: ignore
    
    
    @costId.setter
    def costId(self, value: Union[None, float]):
        # No docs available.
        self._inner_dict['costId'] = value
    
    
    @property
    def costCode(self) -> Union[None, str]:
        # No docs available.
        return self._inner_dict.get('costCode')  # type: ignore
    
    
    @costCode.setter
    def costCode(self, value: Union[None, str]):
        # No docs available.
        self._inner_dict['costCode'] = value
    
    
    @property
    def fieldDiscriminator(self) -> "CostCostDiscriminatorClass":
        """Getter: Contains the name of the field that has its value set."""
        return self._inner_dict.get('fieldDiscriminator')  # type: ignore
    
    
    @fieldDiscriminator.setter
    def fieldDiscriminator(self, value: "CostCostDiscriminatorClass"):
        """Setter: Contains the name of the field that has its value set."""
        self._inner_dict['fieldDiscriminator'] = value
    
    
class CostCostDiscriminatorClass(object):
    # No docs available.
    
    costId = "costId"
    costCode = "costCode"
    
    
class CostTypeClass(object):
    """Type of Cost Code"""
    
    ORG_COST_TYPE = "ORG_COST_TYPE"
    
    
class DeprecationClass(DictWrapper):
    """Deprecation status of an entity"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.Deprecation")
    
    @overload
    def __init__(self,
        deprecated: Optional[bool]=None,
        decommissionTime: Optional[Union[None, int]]=None,
        note: Optional[str]=None,
        actor: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(DeprecationClass, self).__init__({})
        self.deprecated = bool()
        self.decommissionTime = self.RECORD_SCHEMA.field_map["decommissionTime"].default
        self.note = str()
        self.actor = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def deprecated(self) -> bool:
        """Getter: Whether the entity is deprecated."""
        return self._inner_dict.get('deprecated')  # type: ignore
    
    
    @deprecated.setter
    def deprecated(self, value: bool):
        """Setter: Whether the entity is deprecated."""
        self._inner_dict['deprecated'] = value
    
    
    @property
    def decommissionTime(self) -> Union[None, int]:
        """Getter: The time user plan to decommission this entity."""
        return self._inner_dict.get('decommissionTime')  # type: ignore
    
    
    @decommissionTime.setter
    def decommissionTime(self, value: Union[None, int]):
        """Setter: The time user plan to decommission this entity."""
        self._inner_dict['decommissionTime'] = value
    
    
    @property
    def note(self) -> str:
        """Getter: Additional information about the entity deprecation plan, such as the wiki, doc, RB."""
        return self._inner_dict.get('note')  # type: ignore
    
    
    @note.setter
    def note(self, value: str):
        """Setter: Additional information about the entity deprecation plan, such as the wiki, doc, RB."""
        self._inner_dict['note'] = value
    
    
    @property
    def actor(self) -> str:
        """Getter: The corpuser URN which will be credited for modifying this deprecation content."""
        return self._inner_dict.get('actor')  # type: ignore
    
    
    @actor.setter
    def actor(self, value: str):
        """Setter: The corpuser URN which will be credited for modifying this deprecation content."""
        self._inner_dict['actor'] = value
    
    
class InstitutionalMemoryClass(DictWrapper):
    """Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.InstitutionalMemory")
    
    @overload
    def __init__(self,
        elements: Optional[List["InstitutionalMemoryMetadataClass"]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(InstitutionalMemoryClass, self).__init__({})
        self.elements = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def elements(self) -> List["InstitutionalMemoryMetadataClass"]:
        """Getter: List of records that represent institutional memory of an entity. Each record consists of a link, description, creator and timestamps associated with that record."""
        return self._inner_dict.get('elements')  # type: ignore
    
    
    @elements.setter
    def elements(self, value: List["InstitutionalMemoryMetadataClass"]):
        """Setter: List of records that represent institutional memory of an entity. Each record consists of a link, description, creator and timestamps associated with that record."""
        self._inner_dict['elements'] = value
    
    
class InstitutionalMemoryMetadataClass(DictWrapper):
    """Metadata corresponding to a record of institutional memory."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.InstitutionalMemoryMetadata")
    
    @overload
    def __init__(self,
        url: Optional[str]=None,
        description: Optional[str]=None,
        createStamp: Optional["AuditStampClass"]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(InstitutionalMemoryMetadataClass, self).__init__({})
        self.url = str()
        self.description = str()
        self.createStamp = AuditStampClass()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def url(self) -> str:
        """Getter: Link to an engineering design document or a wiki page."""
        return self._inner_dict.get('url')  # type: ignore
    
    
    @url.setter
    def url(self, value: str):
        """Setter: Link to an engineering design document or a wiki page."""
        self._inner_dict['url'] = value
    
    
    @property
    def description(self) -> str:
        """Getter: Description of the link."""
        return self._inner_dict.get('description')  # type: ignore
    
    
    @description.setter
    def description(self, value: str):
        """Setter: Description of the link."""
        self._inner_dict['description'] = value
    
    
    @property
    def createStamp(self) -> "AuditStampClass":
        """Getter: Audit stamp associated with creation of this record"""
        return self._inner_dict.get('createStamp')  # type: ignore
    
    
    @createStamp.setter
    def createStamp(self, value: "AuditStampClass"):
        """Setter: Audit stamp associated with creation of this record"""
        self._inner_dict['createStamp'] = value
    
    
class MLFeatureDataTypeClass(object):
    """MLFeature Data Type"""
    
    USELESS = "USELESS"
    NOMINAL = "NOMINAL"
    ORDINAL = "ORDINAL"
    BINARY = "BINARY"
    COUNT = "COUNT"
    TIME = "TIME"
    INTERVAL = "INTERVAL"
    IMAGE = "IMAGE"
    VIDEO = "VIDEO"
    AUDIO = "AUDIO"
    TEXT = "TEXT"
    MAP = "MAP"
    SEQUENCE = "SEQUENCE"
    SET = "SET"
    
    
class OwnerClass(DictWrapper):
    """Ownership information"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.Owner")
    
    @overload
    def __init__(self,
        owner: Optional[str]=None,
        type: Optional["OwnershipTypeClass"]=None,
        source: Optional[Union[None, "OwnershipSourceClass"]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(OwnerClass, self).__init__({})
        self.owner = str()
        self.type = OwnershipTypeClass.DEVELOPER
        self.source = self.RECORD_SCHEMA.field_map["source"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def owner(self) -> str:
        """Getter: Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name"""
        return self._inner_dict.get('owner')  # type: ignore
    
    
    @owner.setter
    def owner(self, value: str):
        """Setter: Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name"""
        self._inner_dict['owner'] = value
    
    
    @property
    def type(self) -> "OwnershipTypeClass":
        """Getter: The type of the ownership"""
        return self._inner_dict.get('type')  # type: ignore
    
    
    @type.setter
    def type(self, value: "OwnershipTypeClass"):
        """Setter: The type of the ownership"""
        self._inner_dict['type'] = value
    
    
    @property
    def source(self) -> Union[None, "OwnershipSourceClass"]:
        """Getter: Source information for the ownership"""
        return self._inner_dict.get('source')  # type: ignore
    
    
    @source.setter
    def source(self, value: Union[None, "OwnershipSourceClass"]):
        """Setter: Source information for the ownership"""
        self._inner_dict['source'] = value
    
    
class OwnershipClass(DictWrapper):
    """Ownership information of an entity."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.Ownership")
    
    @overload
    def __init__(self,
        owners: Optional[List["OwnerClass"]]=None,
        lastModified: Optional["AuditStampClass"]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(OwnershipClass, self).__init__({})
        self.owners = list()
        self.lastModified = AuditStampClass()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def owners(self) -> List["OwnerClass"]:
        """Getter: List of owners of the entity."""
        return self._inner_dict.get('owners')  # type: ignore
    
    
    @owners.setter
    def owners(self, value: List["OwnerClass"]):
        """Setter: List of owners of the entity."""
        self._inner_dict['owners'] = value
    
    
    @property
    def lastModified(self) -> "AuditStampClass":
        """Getter: Audit stamp containing who last modified the record and when."""
        return self._inner_dict.get('lastModified')  # type: ignore
    
    
    @lastModified.setter
    def lastModified(self, value: "AuditStampClass"):
        """Setter: Audit stamp containing who last modified the record and when."""
        self._inner_dict['lastModified'] = value
    
    
class OwnershipSourceClass(DictWrapper):
    """Source/provider of the ownership information"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.OwnershipSource")
    
    @overload
    def __init__(self,
        type: Optional["OwnershipSourceTypeClass"]=None,
        url: Optional[Union[None, str]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(OwnershipSourceClass, self).__init__({})
        self.type = OwnershipSourceTypeClass.AUDIT
        self.url = self.RECORD_SCHEMA.field_map["url"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def type(self) -> "OwnershipSourceTypeClass":
        """Getter: The type of the source"""
        return self._inner_dict.get('type')  # type: ignore
    
    
    @type.setter
    def type(self, value: "OwnershipSourceTypeClass"):
        """Setter: The type of the source"""
        self._inner_dict['type'] = value
    
    
    @property
    def url(self) -> Union[None, str]:
        """Getter: A reference URL for the source"""
        return self._inner_dict.get('url')  # type: ignore
    
    
    @url.setter
    def url(self, value: Union[None, str]):
        """Setter: A reference URL for the source"""
        self._inner_dict['url'] = value
    
    
class OwnershipSourceTypeClass(object):
    # No docs available.
    
    AUDIT = "AUDIT"
    DATABASE = "DATABASE"
    FILE_SYSTEM = "FILE_SYSTEM"
    ISSUE_TRACKING_SYSTEM = "ISSUE_TRACKING_SYSTEM"
    MANUAL = "MANUAL"
    SERVICE = "SERVICE"
    SOURCE_CONTROL = "SOURCE_CONTROL"
    OTHER = "OTHER"
    
    
class OwnershipTypeClass(object):
    """Owner category or owner role"""
    
    DEVELOPER = "DEVELOPER"
    DATAOWNER = "DATAOWNER"
    DELEGATE = "DELEGATE"
    PRODUCER = "PRODUCER"
    CONSUMER = "CONSUMER"
    STAKEHOLDER = "STAKEHOLDER"
    
    
class StatusClass(DictWrapper):
    """The status metadata of an entity, e.g. dataset, metric, feature, etc."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.Status")
    
    @overload
    def __init__(self,
        removed: Optional[bool]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(StatusClass, self).__init__({})
        self.removed = self.RECORD_SCHEMA.field_map["removed"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def removed(self) -> bool:
        """Getter: whether the entity is removed or not"""
        return self._inner_dict.get('removed')  # type: ignore
    
    
    @removed.setter
    def removed(self, value: bool):
        """Setter: whether the entity is removed or not"""
        self._inner_dict['removed'] = value
    
    
class VersionTagClass(DictWrapper):
    """A resource-defined string representing the resource state for the purpose of concurrency control"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.VersionTag")
    
    @overload
    def __init__(self,
        versionTag: Optional[Union[None, str]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(VersionTagClass, self).__init__({})
        self.versionTag = self.RECORD_SCHEMA.field_map["versionTag"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def versionTag(self) -> Union[None, str]:
        # No docs available.
        return self._inner_dict.get('versionTag')  # type: ignore
    
    
    @versionTag.setter
    def versionTag(self, value: Union[None, str]):
        # No docs available.
        self._inner_dict['versionTag'] = value
    
    
class TransformationTypeClass(object):
    """Type of the transformation involved in generating destination fields from source fields."""
    
    BLACKBOX = "BLACKBOX"
    IDENTITY = "IDENTITY"
    
    
class UDFTransformerClass(DictWrapper):
    """Field transformation expressed in UDF"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.common.fieldtransformer.UDFTransformer")
    
    @overload
    def __init__(self,
        udf: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(UDFTransformerClass, self).__init__({})
        self.udf = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def udf(self) -> str:
        """Getter: A UDF mentioning how the source fields got transformed to destination field. This is the FQCN(Fully Qualified Class Name) of the udf."""
        return self._inner_dict.get('udf')  # type: ignore
    
    
    @udf.setter
    def udf(self, value: str):
        """Setter: A UDF mentioning how the source fields got transformed to destination field. This is the FQCN(Fully Qualified Class Name) of the udf."""
        self._inner_dict['udf'] = value
    
    
class DashboardInfoClass(DictWrapper):
    """Information about a dashboard"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.dashboard.DashboardInfo")
    
    @overload
    def __init__(self,
        title: Optional[str]=None,
        description: Optional[str]=None,
        charts: Optional[List[str]]=None,
        lastModified: Optional["ChangeAuditStampsClass"]=None,
        dashboardUrl: Optional[Union[None, str]]=None,
        access: Optional[Union[None, "AccessLevelClass"]]=None,
        lastRefreshed: Optional[Union[None, int]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(DashboardInfoClass, self).__init__({})
        self.title = str()
        self.description = str()
        self.charts = list()
        self.lastModified = ChangeAuditStampsClass()
        self.dashboardUrl = self.RECORD_SCHEMA.field_map["dashboardUrl"].default
        self.access = self.RECORD_SCHEMA.field_map["access"].default
        self.lastRefreshed = self.RECORD_SCHEMA.field_map["lastRefreshed"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def title(self) -> str:
        """Getter: Title of the dashboard"""
        return self._inner_dict.get('title')  # type: ignore
    
    
    @title.setter
    def title(self, value: str):
        """Setter: Title of the dashboard"""
        self._inner_dict['title'] = value
    
    
    @property
    def description(self) -> str:
        """Getter: Detailed description about the dashboard"""
        return self._inner_dict.get('description')  # type: ignore
    
    
    @description.setter
    def description(self, value: str):
        """Setter: Detailed description about the dashboard"""
        self._inner_dict['description'] = value
    
    
    @property
    def charts(self) -> List[str]:
        """Getter: Charts in a dashboard"""
        return self._inner_dict.get('charts')  # type: ignore
    
    
    @charts.setter
    def charts(self, value: List[str]):
        """Setter: Charts in a dashboard"""
        self._inner_dict['charts'] = value
    
    
    @property
    def lastModified(self) -> "ChangeAuditStampsClass":
        """Getter: Captures information about who created/last modified/deleted this dashboard and when"""
        return self._inner_dict.get('lastModified')  # type: ignore
    
    
    @lastModified.setter
    def lastModified(self, value: "ChangeAuditStampsClass"):
        """Setter: Captures information about who created/last modified/deleted this dashboard and when"""
        self._inner_dict['lastModified'] = value
    
    
    @property
    def dashboardUrl(self) -> Union[None, str]:
        """Getter: URL for the dashboard. This could be used as an external link on DataHub to allow users access/view the dashboard"""
        return self._inner_dict.get('dashboardUrl')  # type: ignore
    
    
    @dashboardUrl.setter
    def dashboardUrl(self, value: Union[None, str]):
        """Setter: URL for the dashboard. This could be used as an external link on DataHub to allow users access/view the dashboard"""
        self._inner_dict['dashboardUrl'] = value
    
    
    @property
    def access(self) -> Union[None, "AccessLevelClass"]:
        """Getter: Access level for the dashboard"""
        return self._inner_dict.get('access')  # type: ignore
    
    
    @access.setter
    def access(self, value: Union[None, "AccessLevelClass"]):
        """Setter: Access level for the dashboard"""
        self._inner_dict['access'] = value
    
    
    @property
    def lastRefreshed(self) -> Union[None, int]:
        """Getter: The time when this dashboard last refreshed"""
        return self._inner_dict.get('lastRefreshed')  # type: ignore
    
    
    @lastRefreshed.setter
    def lastRefreshed(self, value: Union[None, int]):
        """Setter: The time when this dashboard last refreshed"""
        self._inner_dict['lastRefreshed'] = value
    
    
class DataProcessInfoClass(DictWrapper):
    """The inputs and outputs of this data process"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.dataprocess.DataProcessInfo")
    
    @overload
    def __init__(self,
        inputs: Optional[Union[None, List[str]]]=None,
        outputs: Optional[Union[None, List[str]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(DataProcessInfoClass, self).__init__({})
        self.inputs = self.RECORD_SCHEMA.field_map["inputs"].default
        self.outputs = self.RECORD_SCHEMA.field_map["outputs"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def inputs(self) -> Union[None, List[str]]:
        """Getter: the inputs of the data process"""
        return self._inner_dict.get('inputs')  # type: ignore
    
    
    @inputs.setter
    def inputs(self, value: Union[None, List[str]]):
        """Setter: the inputs of the data process"""
        self._inner_dict['inputs'] = value
    
    
    @property
    def outputs(self) -> Union[None, List[str]]:
        """Getter: the outputs of the data process"""
        return self._inner_dict.get('outputs')  # type: ignore
    
    
    @outputs.setter
    def outputs(self, value: Union[None, List[str]]):
        """Setter: the outputs of the data process"""
        self._inner_dict['outputs'] = value
    
    
class DatasetDeprecationClass(DictWrapper):
    """Dataset deprecation status"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.dataset.DatasetDeprecation")
    
    @overload
    def __init__(self,
        deprecated: Optional[bool]=None,
        decommissionTime: Optional[Union[None, int]]=None,
        note: Optional[str]=None,
        actor: Optional[Union[None, str]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(DatasetDeprecationClass, self).__init__({})
        self.deprecated = bool()
        self.decommissionTime = self.RECORD_SCHEMA.field_map["decommissionTime"].default
        self.note = str()
        self.actor = self.RECORD_SCHEMA.field_map["actor"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def deprecated(self) -> bool:
        """Getter: Whether the dataset is deprecated by owner."""
        return self._inner_dict.get('deprecated')  # type: ignore
    
    
    @deprecated.setter
    def deprecated(self, value: bool):
        """Setter: Whether the dataset is deprecated by owner."""
        self._inner_dict['deprecated'] = value
    
    
    @property
    def decommissionTime(self) -> Union[None, int]:
        """Getter: The time user plan to decommission this dataset."""
        return self._inner_dict.get('decommissionTime')  # type: ignore
    
    
    @decommissionTime.setter
    def decommissionTime(self, value: Union[None, int]):
        """Setter: The time user plan to decommission this dataset."""
        self._inner_dict['decommissionTime'] = value
    
    
    @property
    def note(self) -> str:
        """Getter: Additional information about the dataset deprecation plan, such as the wiki, doc, RB."""
        return self._inner_dict.get('note')  # type: ignore
    
    
    @note.setter
    def note(self, value: str):
        """Setter: Additional information about the dataset deprecation plan, such as the wiki, doc, RB."""
        self._inner_dict['note'] = value
    
    
    @property
    def actor(self) -> Union[None, str]:
        """Getter: The corpuser URN which will be credited for modifying this deprecation content."""
        return self._inner_dict.get('actor')  # type: ignore
    
    
    @actor.setter
    def actor(self, value: Union[None, str]):
        """Setter: The corpuser URN which will be credited for modifying this deprecation content."""
        self._inner_dict['actor'] = value
    
    
class DatasetFieldMappingClass(DictWrapper):
    """Representation of mapping between fields in source dataset to the field in destination dataset"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.dataset.DatasetFieldMapping")
    
    @overload
    def __init__(self,
        created: Optional["AuditStampClass"]=None,
        transformation: Optional[Union["TransformationTypeClass", "UDFTransformerClass"]]=None,
        sourceFields: Optional[List[str]]=None,
        destinationField: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(DatasetFieldMappingClass, self).__init__({})
        self.created = AuditStampClass()
        self.transformation = TransformationTypeClass.BLACKBOX
        self.sourceFields = list()
        self.destinationField = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def created(self) -> "AuditStampClass":
        """Getter: Audit stamp containing who reported the field mapping and when"""
        return self._inner_dict.get('created')  # type: ignore
    
    
    @created.setter
    def created(self, value: "AuditStampClass"):
        """Setter: Audit stamp containing who reported the field mapping and when"""
        self._inner_dict['created'] = value
    
    
    @property
    def transformation(self) -> Union["TransformationTypeClass", "UDFTransformerClass"]:
        """Getter: Transfomration function between the fields involved"""
        return self._inner_dict.get('transformation')  # type: ignore
    
    
    @transformation.setter
    def transformation(self, value: Union["TransformationTypeClass", "UDFTransformerClass"]):
        """Setter: Transfomration function between the fields involved"""
        self._inner_dict['transformation'] = value
    
    
    @property
    def sourceFields(self) -> List[str]:
        """Getter: Source fields from which the fine grained lineage is derived"""
        return self._inner_dict.get('sourceFields')  # type: ignore
    
    
    @sourceFields.setter
    def sourceFields(self, value: List[str]):
        """Setter: Source fields from which the fine grained lineage is derived"""
        self._inner_dict['sourceFields'] = value
    
    
    @property
    def destinationField(self) -> str:
        """Getter: Destination field which is derived from source fields"""
        return self._inner_dict.get('destinationField')  # type: ignore
    
    
    @destinationField.setter
    def destinationField(self, value: str):
        """Setter: Destination field which is derived from source fields"""
        self._inner_dict['destinationField'] = value
    
    
class DatasetLineageTypeClass(object):
    """The various types of supported dataset lineage"""
    
    COPY = "COPY"
    TRANSFORMED = "TRANSFORMED"
    VIEW = "VIEW"
    
    
class DatasetPropertiesClass(DictWrapper):
    """Properties associated with a Dataset"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.dataset.DatasetProperties")
    
    @overload
    def __init__(self,
        description: Optional[Union[None, str]]=None,
        uri: Optional[Union[None, str]]=None,
        tags: Optional[List[str]]=None,
        customProperties: Optional[Dict[str, str]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(DatasetPropertiesClass, self).__init__({})
        self.description = self.RECORD_SCHEMA.field_map["description"].default
        self.uri = self.RECORD_SCHEMA.field_map["uri"].default
        self.tags = list()
        self.customProperties = dict()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def description(self) -> Union[None, str]:
        """Getter: Documentation of the dataset"""
        return self._inner_dict.get('description')  # type: ignore
    
    
    @description.setter
    def description(self, value: Union[None, str]):
        """Setter: Documentation of the dataset"""
        self._inner_dict['description'] = value
    
    
    @property
    def uri(self) -> Union[None, str]:
        """Getter: The abstracted URI such as hdfs:///data/tracking/PageViewEvent, file:///dir/file_name. Uri should not include any environment specific properties. Some datasets might not have a standardized uri, which makes this field optional (i.e. kafka topic)."""
        return self._inner_dict.get('uri')  # type: ignore
    
    
    @uri.setter
    def uri(self, value: Union[None, str]):
        """Setter: The abstracted URI such as hdfs:///data/tracking/PageViewEvent, file:///dir/file_name. Uri should not include any environment specific properties. Some datasets might not have a standardized uri, which makes this field optional (i.e. kafka topic)."""
        self._inner_dict['uri'] = value
    
    
    @property
    def tags(self) -> List[str]:
        """Getter: tags for the dataset"""
        return self._inner_dict.get('tags')  # type: ignore
    
    
    @tags.setter
    def tags(self, value: List[str]):
        """Setter: tags for the dataset"""
        self._inner_dict['tags'] = value
    
    
    @property
    def customProperties(self) -> Dict[str, str]:
        """Getter: A key-value map to capture any other non-standardized properties for the dataset"""
        return self._inner_dict.get('customProperties')  # type: ignore
    
    
    @customProperties.setter
    def customProperties(self, value: Dict[str, str]):
        """Setter: A key-value map to capture any other non-standardized properties for the dataset"""
        self._inner_dict['customProperties'] = value
    
    
class DatasetUpstreamLineageClass(DictWrapper):
    """Fine Grained upstream lineage for fields in a dataset"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.dataset.DatasetUpstreamLineage")
    
    @overload
    def __init__(self,
        fieldMappings: Optional[List["DatasetFieldMappingClass"]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(DatasetUpstreamLineageClass, self).__init__({})
        self.fieldMappings = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def fieldMappings(self) -> List["DatasetFieldMappingClass"]:
        """Getter: Upstream to downstream field level lineage mappings"""
        return self._inner_dict.get('fieldMappings')  # type: ignore
    
    
    @fieldMappings.setter
    def fieldMappings(self, value: List["DatasetFieldMappingClass"]):
        """Setter: Upstream to downstream field level lineage mappings"""
        self._inner_dict['fieldMappings'] = value
    
    
class UpstreamClass(DictWrapper):
    """Upstream lineage information about a dataset including the source reporting the lineage"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.dataset.Upstream")
    
    @overload
    def __init__(self,
        auditStamp: Optional["AuditStampClass"]=None,
        dataset: Optional[str]=None,
        type: Optional["DatasetLineageTypeClass"]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(UpstreamClass, self).__init__({})
        self.auditStamp = AuditStampClass()
        self.dataset = str()
        self.type = DatasetLineageTypeClass.COPY
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def auditStamp(self) -> "AuditStampClass":
        """Getter: Audit stamp containing who reported the lineage and when"""
        return self._inner_dict.get('auditStamp')  # type: ignore
    
    
    @auditStamp.setter
    def auditStamp(self, value: "AuditStampClass"):
        """Setter: Audit stamp containing who reported the lineage and when"""
        self._inner_dict['auditStamp'] = value
    
    
    @property
    def dataset(self) -> str:
        """Getter: The upstream dataset the lineage points to"""
        return self._inner_dict.get('dataset')  # type: ignore
    
    
    @dataset.setter
    def dataset(self, value: str):
        """Setter: The upstream dataset the lineage points to"""
        self._inner_dict['dataset'] = value
    
    
    @property
    def type(self) -> "DatasetLineageTypeClass":
        """Getter: The type of the lineage"""
        return self._inner_dict.get('type')  # type: ignore
    
    
    @type.setter
    def type(self, value: "DatasetLineageTypeClass"):
        """Setter: The type of the lineage"""
        self._inner_dict['type'] = value
    
    
class UpstreamLineageClass(DictWrapper):
    """Upstream lineage of a dataset"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.dataset.UpstreamLineage")
    
    @overload
    def __init__(self,
        upstreams: Optional[List["UpstreamClass"]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(UpstreamLineageClass, self).__init__({})
        self.upstreams = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def upstreams(self) -> List["UpstreamClass"]:
        """Getter: List of upstream dataset lineage information"""
        return self._inner_dict.get('upstreams')  # type: ignore
    
    
    @upstreams.setter
    def upstreams(self, value: List["UpstreamClass"]):
        """Setter: List of upstream dataset lineage information"""
        self._inner_dict['upstreams'] = value
    
    
class CorpGroupInfoClass(DictWrapper):
    """group of corpUser, it may contains nested group"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.identity.CorpGroupInfo")
    
    @overload
    def __init__(self,
        email: Optional[str]=None,
        admins: Optional[List[str]]=None,
        members: Optional[List[str]]=None,
        groups: Optional[List[str]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(CorpGroupInfoClass, self).__init__({})
        self.email = str()
        self.admins = list()
        self.members = list()
        self.groups = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def email(self) -> str:
        """Getter: email of this group"""
        return self._inner_dict.get('email')  # type: ignore
    
    
    @email.setter
    def email(self, value: str):
        """Setter: email of this group"""
        self._inner_dict['email'] = value
    
    
    @property
    def admins(self) -> List[str]:
        """Getter: owners of this group"""
        return self._inner_dict.get('admins')  # type: ignore
    
    
    @admins.setter
    def admins(self, value: List[str]):
        """Setter: owners of this group"""
        self._inner_dict['admins'] = value
    
    
    @property
    def members(self) -> List[str]:
        """Getter: List of ldap urn in this group."""
        return self._inner_dict.get('members')  # type: ignore
    
    
    @members.setter
    def members(self, value: List[str]):
        """Setter: List of ldap urn in this group."""
        self._inner_dict['members'] = value
    
    
    @property
    def groups(self) -> List[str]:
        """Getter: List of groups in this group."""
        return self._inner_dict.get('groups')  # type: ignore
    
    
    @groups.setter
    def groups(self, value: List[str]):
        """Setter: List of groups in this group."""
        self._inner_dict['groups'] = value
    
    
class CorpUserEditableInfoClass(DictWrapper):
    """Linkedin corp user information that can be edited from UI"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.identity.CorpUserEditableInfo")
    
    @overload
    def __init__(self,
        aboutMe: Optional[Union[None, str]]=None,
        teams: Optional[List[str]]=None,
        skills: Optional[List[str]]=None,
        pictureLink: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(CorpUserEditableInfoClass, self).__init__({})
        self.aboutMe = self.RECORD_SCHEMA.field_map["aboutMe"].default
        self.teams = list()
        self.skills = list()
        self.pictureLink = self.RECORD_SCHEMA.field_map["pictureLink"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def aboutMe(self) -> Union[None, str]:
        """Getter: About me section of the user"""
        return self._inner_dict.get('aboutMe')  # type: ignore
    
    
    @aboutMe.setter
    def aboutMe(self, value: Union[None, str]):
        """Setter: About me section of the user"""
        self._inner_dict['aboutMe'] = value
    
    
    @property
    def teams(self) -> List[str]:
        """Getter: Teams that the user belongs to e.g. Metadata"""
        return self._inner_dict.get('teams')  # type: ignore
    
    
    @teams.setter
    def teams(self, value: List[str]):
        """Setter: Teams that the user belongs to e.g. Metadata"""
        self._inner_dict['teams'] = value
    
    
    @property
    def skills(self) -> List[str]:
        """Getter: Skills that the user possesses e.g. Machine Learning"""
        return self._inner_dict.get('skills')  # type: ignore
    
    
    @skills.setter
    def skills(self, value: List[str]):
        """Setter: Skills that the user possesses e.g. Machine Learning"""
        self._inner_dict['skills'] = value
    
    
    @property
    def pictureLink(self) -> str:
        """Getter: A URL which points to a picture which user wants to set as a profile photo"""
        return self._inner_dict.get('pictureLink')  # type: ignore
    
    
    @pictureLink.setter
    def pictureLink(self, value: str):
        """Setter: A URL which points to a picture which user wants to set as a profile photo"""
        self._inner_dict['pictureLink'] = value
    
    
class CorpUserInfoClass(DictWrapper):
    """Linkedin corp user information"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.identity.CorpUserInfo")
    
    @overload
    def __init__(self,
        active: Optional[bool]=None,
        displayName: Optional[Union[None, str]]=None,
        email: Optional[str]=None,
        title: Optional[Union[None, str]]=None,
        managerUrn: Optional[Union[None, str]]=None,
        departmentId: Optional[Union[None, int]]=None,
        departmentName: Optional[Union[None, str]]=None,
        firstName: Optional[Union[None, str]]=None,
        lastName: Optional[Union[None, str]]=None,
        fullName: Optional[Union[None, str]]=None,
        countryCode: Optional[Union[None, str]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(CorpUserInfoClass, self).__init__({})
        self.active = bool()
        self.displayName = self.RECORD_SCHEMA.field_map["displayName"].default
        self.email = str()
        self.title = self.RECORD_SCHEMA.field_map["title"].default
        self.managerUrn = self.RECORD_SCHEMA.field_map["managerUrn"].default
        self.departmentId = self.RECORD_SCHEMA.field_map["departmentId"].default
        self.departmentName = self.RECORD_SCHEMA.field_map["departmentName"].default
        self.firstName = self.RECORD_SCHEMA.field_map["firstName"].default
        self.lastName = self.RECORD_SCHEMA.field_map["lastName"].default
        self.fullName = self.RECORD_SCHEMA.field_map["fullName"].default
        self.countryCode = self.RECORD_SCHEMA.field_map["countryCode"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def active(self) -> bool:
        """Getter: Whether the corpUser is active, ref: https://iwww.corp.linkedin.com/wiki/cf/display/GTSD/Accessing+Active+Directory+via+LDAP+tools"""
        return self._inner_dict.get('active')  # type: ignore
    
    
    @active.setter
    def active(self, value: bool):
        """Setter: Whether the corpUser is active, ref: https://iwww.corp.linkedin.com/wiki/cf/display/GTSD/Accessing+Active+Directory+via+LDAP+tools"""
        self._inner_dict['active'] = value
    
    
    @property
    def displayName(self) -> Union[None, str]:
        """Getter: displayName of this user ,  e.g.  Hang Zhang(DataHQ)"""
        return self._inner_dict.get('displayName')  # type: ignore
    
    
    @displayName.setter
    def displayName(self, value: Union[None, str]):
        """Setter: displayName of this user ,  e.g.  Hang Zhang(DataHQ)"""
        self._inner_dict['displayName'] = value
    
    
    @property
    def email(self) -> str:
        """Getter: email address of this user"""
        return self._inner_dict.get('email')  # type: ignore
    
    
    @email.setter
    def email(self, value: str):
        """Setter: email address of this user"""
        self._inner_dict['email'] = value
    
    
    @property
    def title(self) -> Union[None, str]:
        """Getter: title of this user"""
        return self._inner_dict.get('title')  # type: ignore
    
    
    @title.setter
    def title(self, value: Union[None, str]):
        """Setter: title of this user"""
        self._inner_dict['title'] = value
    
    
    @property
    def managerUrn(self) -> Union[None, str]:
        """Getter: direct manager of this user"""
        return self._inner_dict.get('managerUrn')  # type: ignore
    
    
    @managerUrn.setter
    def managerUrn(self, value: Union[None, str]):
        """Setter: direct manager of this user"""
        self._inner_dict['managerUrn'] = value
    
    
    @property
    def departmentId(self) -> Union[None, int]:
        """Getter: department id this user belong to"""
        return self._inner_dict.get('departmentId')  # type: ignore
    
    
    @departmentId.setter
    def departmentId(self, value: Union[None, int]):
        """Setter: department id this user belong to"""
        self._inner_dict['departmentId'] = value
    
    
    @property
    def departmentName(self) -> Union[None, str]:
        """Getter: department name this user belong to"""
        return self._inner_dict.get('departmentName')  # type: ignore
    
    
    @departmentName.setter
    def departmentName(self, value: Union[None, str]):
        """Setter: department name this user belong to"""
        self._inner_dict['departmentName'] = value
    
    
    @property
    def firstName(self) -> Union[None, str]:
        """Getter: first name of this user"""
        return self._inner_dict.get('firstName')  # type: ignore
    
    
    @firstName.setter
    def firstName(self, value: Union[None, str]):
        """Setter: first name of this user"""
        self._inner_dict['firstName'] = value
    
    
    @property
    def lastName(self) -> Union[None, str]:
        """Getter: last name of this user"""
        return self._inner_dict.get('lastName')  # type: ignore
    
    
    @lastName.setter
    def lastName(self, value: Union[None, str]):
        """Setter: last name of this user"""
        self._inner_dict['lastName'] = value
    
    
    @property
    def fullName(self) -> Union[None, str]:
        """Getter: Common name of this user, format is firstName + lastName (split by a whitespace)"""
        return self._inner_dict.get('fullName')  # type: ignore
    
    
    @fullName.setter
    def fullName(self, value: Union[None, str]):
        """Setter: Common name of this user, format is firstName + lastName (split by a whitespace)"""
        self._inner_dict['fullName'] = value
    
    
    @property
    def countryCode(self) -> Union[None, str]:
        """Getter: two uppercase letters country code. e.g.  US"""
        return self._inner_dict.get('countryCode')  # type: ignore
    
    
    @countryCode.setter
    def countryCode(self, value: Union[None, str]):
        """Setter: two uppercase letters country code. e.g.  US"""
        self._inner_dict['countryCode'] = value
    
    
class ChartSnapshotClass(DictWrapper):
    """A metadata snapshot for a specific Chart entity."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.metadata.snapshot.ChartSnapshot")
    
    @overload
    def __init__(self,
        urn: Optional[str]=None,
        aspects: Optional[List[Union["ChartInfoClass", "ChartQueryClass", "OwnershipClass", "StatusClass"]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(ChartSnapshotClass, self).__init__({})
        self.urn = str()
        self.aspects = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def urn(self) -> str:
        """Getter: URN for the entity the metadata snapshot is associated with."""
        return self._inner_dict.get('urn')  # type: ignore
    
    
    @urn.setter
    def urn(self, value: str):
        """Setter: URN for the entity the metadata snapshot is associated with."""
        self._inner_dict['urn'] = value
    
    
    @property
    def aspects(self) -> List[Union["ChartInfoClass", "ChartQueryClass", "OwnershipClass", "StatusClass"]]:
        """Getter: The list of metadata aspects associated with the chart. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        return self._inner_dict.get('aspects')  # type: ignore
    
    
    @aspects.setter
    def aspects(self, value: List[Union["ChartInfoClass", "ChartQueryClass", "OwnershipClass", "StatusClass"]]):
        """Setter: The list of metadata aspects associated with the chart. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        self._inner_dict['aspects'] = value
    
    
class CorpGroupSnapshotClass(DictWrapper):
    """A metadata snapshot for a specific CorpGroup entity."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.metadata.snapshot.CorpGroupSnapshot")
    
    @overload
    def __init__(self,
        urn: Optional[str]=None,
        aspects: Optional[List["CorpGroupInfoClass"]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(CorpGroupSnapshotClass, self).__init__({})
        self.urn = str()
        self.aspects = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def urn(self) -> str:
        """Getter: URN for the entity the metadata snapshot is associated with."""
        return self._inner_dict.get('urn')  # type: ignore
    
    
    @urn.setter
    def urn(self, value: str):
        """Setter: URN for the entity the metadata snapshot is associated with."""
        self._inner_dict['urn'] = value
    
    
    @property
    def aspects(self) -> List["CorpGroupInfoClass"]:
        """Getter: The list of metadata aspects associated with the LdapUser. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        return self._inner_dict.get('aspects')  # type: ignore
    
    
    @aspects.setter
    def aspects(self, value: List["CorpGroupInfoClass"]):
        """Setter: The list of metadata aspects associated with the LdapUser. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        self._inner_dict['aspects'] = value
    
    
class CorpUserSnapshotClass(DictWrapper):
    """A metadata snapshot for a specific CorpUser entity."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot")
    
    @overload
    def __init__(self,
        urn: Optional[str]=None,
        aspects: Optional[List[Union["CorpUserInfoClass", "CorpUserEditableInfoClass"]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(CorpUserSnapshotClass, self).__init__({})
        self.urn = str()
        self.aspects = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def urn(self) -> str:
        """Getter: URN for the entity the metadata snapshot is associated with."""
        return self._inner_dict.get('urn')  # type: ignore
    
    
    @urn.setter
    def urn(self, value: str):
        """Setter: URN for the entity the metadata snapshot is associated with."""
        self._inner_dict['urn'] = value
    
    
    @property
    def aspects(self) -> List[Union["CorpUserInfoClass", "CorpUserEditableInfoClass"]]:
        """Getter: The list of metadata aspects associated with the CorpUser. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        return self._inner_dict.get('aspects')  # type: ignore
    
    
    @aspects.setter
    def aspects(self, value: List[Union["CorpUserInfoClass", "CorpUserEditableInfoClass"]]):
        """Setter: The list of metadata aspects associated with the CorpUser. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        self._inner_dict['aspects'] = value
    
    
class DashboardSnapshotClass(DictWrapper):
    """A metadata snapshot for a specific Dashboard entity."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.metadata.snapshot.DashboardSnapshot")
    
    @overload
    def __init__(self,
        urn: Optional[str]=None,
        aspects: Optional[List[Union["DashboardInfoClass", "OwnershipClass", "StatusClass"]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(DashboardSnapshotClass, self).__init__({})
        self.urn = str()
        self.aspects = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def urn(self) -> str:
        """Getter: URN for the entity the metadata snapshot is associated with."""
        return self._inner_dict.get('urn')  # type: ignore
    
    
    @urn.setter
    def urn(self, value: str):
        """Setter: URN for the entity the metadata snapshot is associated with."""
        self._inner_dict['urn'] = value
    
    
    @property
    def aspects(self) -> List[Union["DashboardInfoClass", "OwnershipClass", "StatusClass"]]:
        """Getter: The list of metadata aspects associated with the dashboard. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        return self._inner_dict.get('aspects')  # type: ignore
    
    
    @aspects.setter
    def aspects(self, value: List[Union["DashboardInfoClass", "OwnershipClass", "StatusClass"]]):
        """Setter: The list of metadata aspects associated with the dashboard. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        self._inner_dict['aspects'] = value
    
    
class DataProcessSnapshotClass(DictWrapper):
    """A metadata snapshot for a specific Data process entity."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.metadata.snapshot.DataProcessSnapshot")
    
    @overload
    def __init__(self,
        urn: Optional[str]=None,
        aspects: Optional[List[Union["OwnershipClass", "DataProcessInfoClass"]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(DataProcessSnapshotClass, self).__init__({})
        self.urn = str()
        self.aspects = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def urn(self) -> str:
        """Getter: URN for the entity the metadata snapshot is associated with."""
        return self._inner_dict.get('urn')  # type: ignore
    
    
    @urn.setter
    def urn(self, value: str):
        """Setter: URN for the entity the metadata snapshot is associated with."""
        self._inner_dict['urn'] = value
    
    
    @property
    def aspects(self) -> List[Union["OwnershipClass", "DataProcessInfoClass"]]:
        """Getter: The list of metadata aspects associated with the data process. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        return self._inner_dict.get('aspects')  # type: ignore
    
    
    @aspects.setter
    def aspects(self, value: List[Union["OwnershipClass", "DataProcessInfoClass"]]):
        """Setter: The list of metadata aspects associated with the data process. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        self._inner_dict['aspects'] = value
    
    
class DatasetSnapshotClass(DictWrapper):
    """A metadata snapshot for a specific dataset entity."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot")
    
    @overload
    def __init__(self,
        urn: Optional[str]=None,
        aspects: Optional[List[Union["DatasetPropertiesClass", "DatasetDeprecationClass", "DatasetUpstreamLineageClass", "UpstreamLineageClass", "InstitutionalMemoryClass", "OwnershipClass", "StatusClass", "SchemaMetadataClass"]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(DatasetSnapshotClass, self).__init__({})
        self.urn = str()
        self.aspects = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def urn(self) -> str:
        """Getter: URN for the entity the metadata snapshot is associated with."""
        return self._inner_dict.get('urn')  # type: ignore
    
    
    @urn.setter
    def urn(self, value: str):
        """Setter: URN for the entity the metadata snapshot is associated with."""
        self._inner_dict['urn'] = value
    
    
    @property
    def aspects(self) -> List[Union["DatasetPropertiesClass", "DatasetDeprecationClass", "DatasetUpstreamLineageClass", "UpstreamLineageClass", "InstitutionalMemoryClass", "OwnershipClass", "StatusClass", "SchemaMetadataClass"]]:
        """Getter: The list of metadata aspects associated with the dataset. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        return self._inner_dict.get('aspects')  # type: ignore
    
    
    @aspects.setter
    def aspects(self, value: List[Union["DatasetPropertiesClass", "DatasetDeprecationClass", "DatasetUpstreamLineageClass", "UpstreamLineageClass", "InstitutionalMemoryClass", "OwnershipClass", "StatusClass", "SchemaMetadataClass"]]):
        """Setter: The list of metadata aspects associated with the dataset. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        self._inner_dict['aspects'] = value
    
    
class MLFeatureSnapshotClass(DictWrapper):
    # No docs available.
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.metadata.snapshot.MLFeatureSnapshot")
    
    @overload
    def __init__(self,
        urn: Optional[str]=None,
        aspects: Optional[List[Union["OwnershipClass", "MLFeaturePropertiesClass", "InstitutionalMemoryClass", "StatusClass", "DeprecationClass"]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(MLFeatureSnapshotClass, self).__init__({})
        self.urn = str()
        self.aspects = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def urn(self) -> str:
        """Getter: URN for the entity the metadata snapshot is associated with."""
        return self._inner_dict.get('urn')  # type: ignore
    
    
    @urn.setter
    def urn(self, value: str):
        """Setter: URN for the entity the metadata snapshot is associated with."""
        self._inner_dict['urn'] = value
    
    
    @property
    def aspects(self) -> List[Union["OwnershipClass", "MLFeaturePropertiesClass", "InstitutionalMemoryClass", "StatusClass", "DeprecationClass"]]:
        """Getter: The list of metadata aspects associated with the MLModel. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        return self._inner_dict.get('aspects')  # type: ignore
    
    
    @aspects.setter
    def aspects(self, value: List[Union["OwnershipClass", "MLFeaturePropertiesClass", "InstitutionalMemoryClass", "StatusClass", "DeprecationClass"]]):
        """Setter: The list of metadata aspects associated with the MLModel. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        self._inner_dict['aspects'] = value
    
    
class MLModelSnapshotClass(DictWrapper):
    """MLModel Snapshot entity details."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.metadata.snapshot.MLModelSnapshot")
    
    @overload
    def __init__(self,
        urn: Optional[str]=None,
        aspects: Optional[List[Union["OwnershipClass", "MLModelPropertiesClass", "IntendedUseClass", "MLModelFactorPromptsClass", "MetricsClass", "EvaluationDataClass", "TrainingDataClass", "QuantitativeAnalysesClass", "EthicalConsiderationsClass", "CaveatsAndRecommendationsClass", "InstitutionalMemoryClass", "SourceCodeClass", "StatusClass", "CostClass", "DeprecationClass"]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(MLModelSnapshotClass, self).__init__({})
        self.urn = str()
        self.aspects = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def urn(self) -> str:
        """Getter: URN for the entity the metadata snapshot is associated with."""
        return self._inner_dict.get('urn')  # type: ignore
    
    
    @urn.setter
    def urn(self, value: str):
        """Setter: URN for the entity the metadata snapshot is associated with."""
        self._inner_dict['urn'] = value
    
    
    @property
    def aspects(self) -> List[Union["OwnershipClass", "MLModelPropertiesClass", "IntendedUseClass", "MLModelFactorPromptsClass", "MetricsClass", "EvaluationDataClass", "TrainingDataClass", "QuantitativeAnalysesClass", "EthicalConsiderationsClass", "CaveatsAndRecommendationsClass", "InstitutionalMemoryClass", "SourceCodeClass", "StatusClass", "CostClass", "DeprecationClass"]]:
        """Getter: The list of metadata aspects associated with the MLModel. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        return self._inner_dict.get('aspects')  # type: ignore
    
    
    @aspects.setter
    def aspects(self, value: List[Union["OwnershipClass", "MLModelPropertiesClass", "IntendedUseClass", "MLModelFactorPromptsClass", "MetricsClass", "EvaluationDataClass", "TrainingDataClass", "QuantitativeAnalysesClass", "EthicalConsiderationsClass", "CaveatsAndRecommendationsClass", "InstitutionalMemoryClass", "SourceCodeClass", "StatusClass", "CostClass", "DeprecationClass"]]):
        """Setter: The list of metadata aspects associated with the MLModel. Depending on the use case, this can either be all, or a selection, of supported aspects."""
        self._inner_dict['aspects'] = value
    
    
class BaseDataClass(DictWrapper):
    """BaseData record"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.BaseData")
    
    @overload
    def __init__(self,
        dataset: Optional[str]=None,
        motivation: Optional[Union[None, str]]=None,
        preProcessing: Optional[Union[None, List[str]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(BaseDataClass, self).__init__({})
        self.dataset = str()
        self.motivation = self.RECORD_SCHEMA.field_map["motivation"].default
        self.preProcessing = self.RECORD_SCHEMA.field_map["preProcessing"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def dataset(self) -> str:
        """Getter: What dataset were used in the MLModel?"""
        return self._inner_dict.get('dataset')  # type: ignore
    
    
    @dataset.setter
    def dataset(self, value: str):
        """Setter: What dataset were used in the MLModel?"""
        self._inner_dict['dataset'] = value
    
    
    @property
    def motivation(self) -> Union[None, str]:
        """Getter: Why was this dataset chosen?"""
        return self._inner_dict.get('motivation')  # type: ignore
    
    
    @motivation.setter
    def motivation(self, value: Union[None, str]):
        """Setter: Why was this dataset chosen?"""
        self._inner_dict['motivation'] = value
    
    
    @property
    def preProcessing(self) -> Union[None, List[str]]:
        """Getter: How was the data preprocessed (e.g., tokenization of sentences, cropping of images, any filtering such as dropping images without faces)?"""
        return self._inner_dict.get('preProcessing')  # type: ignore
    
    
    @preProcessing.setter
    def preProcessing(self, value: Union[None, List[str]]):
        """Setter: How was the data preprocessed (e.g., tokenization of sentences, cropping of images, any filtering such as dropping images without faces)?"""
        self._inner_dict['preProcessing'] = value
    
    
class CaveatDetailsClass(DictWrapper):
    """This section should list additional concerns that were not covered in the previous sections. For example, did the results suggest any further testing? Were there any relevant groups that were not represented in the evaluation dataset? Are there additional recommendations for model use?"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.CaveatDetails")
    
    @overload
    def __init__(self,
        needsFurtherTesting: Optional[Union[None, bool]]=None,
        caveatDescription: Optional[Union[None, str]]=None,
        groupsNotRepresented: Optional[Union[None, List[str]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(CaveatDetailsClass, self).__init__({})
        self.needsFurtherTesting = self.RECORD_SCHEMA.field_map["needsFurtherTesting"].default
        self.caveatDescription = self.RECORD_SCHEMA.field_map["caveatDescription"].default
        self.groupsNotRepresented = self.RECORD_SCHEMA.field_map["groupsNotRepresented"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def needsFurtherTesting(self) -> Union[None, bool]:
        """Getter: Did the results suggest any further testing?"""
        return self._inner_dict.get('needsFurtherTesting')  # type: ignore
    
    
    @needsFurtherTesting.setter
    def needsFurtherTesting(self, value: Union[None, bool]):
        """Setter: Did the results suggest any further testing?"""
        self._inner_dict['needsFurtherTesting'] = value
    
    
    @property
    def caveatDescription(self) -> Union[None, str]:
        """Getter: Caveat Description
    For ex: Given gender classes are binary (male/not male), which we include as male/female. Further work needed to evaluate across a spectrum of genders."""
        return self._inner_dict.get('caveatDescription')  # type: ignore
    
    
    @caveatDescription.setter
    def caveatDescription(self, value: Union[None, str]):
        """Setter: Caveat Description
    For ex: Given gender classes are binary (male/not male), which we include as male/female. Further work needed to evaluate across a spectrum of genders."""
        self._inner_dict['caveatDescription'] = value
    
    
    @property
    def groupsNotRepresented(self) -> Union[None, List[str]]:
        """Getter: Relevant groups that were not represented in the evaluation dataset?"""
        return self._inner_dict.get('groupsNotRepresented')  # type: ignore
    
    
    @groupsNotRepresented.setter
    def groupsNotRepresented(self, value: Union[None, List[str]]):
        """Setter: Relevant groups that were not represented in the evaluation dataset?"""
        self._inner_dict['groupsNotRepresented'] = value
    
    
class CaveatsAndRecommendationsClass(DictWrapper):
    """This section should list additional concerns that were not covered in the previous sections. For example, did the results suggest any further testing? Were there any relevant groups that were not represented in the evaluation dataset? Are there additional recommendations for model use?"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.CaveatsAndRecommendations")
    
    @overload
    def __init__(self,
        caveats: Optional[Union[None, "CaveatDetailsClass"]]=None,
        recommendations: Optional[Union[None, str]]=None,
        idealDatasetCharacteristics: Optional[Union[None, List[str]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(CaveatsAndRecommendationsClass, self).__init__({})
        self.caveats = self.RECORD_SCHEMA.field_map["caveats"].default
        self.recommendations = self.RECORD_SCHEMA.field_map["recommendations"].default
        self.idealDatasetCharacteristics = self.RECORD_SCHEMA.field_map["idealDatasetCharacteristics"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def caveats(self) -> Union[None, "CaveatDetailsClass"]:
        """Getter: This section should list additional concerns that were not covered in the previous sections. For example, did the results suggest any further testing? Were there any relevant groups that were not represented in the evaluation dataset?"""
        return self._inner_dict.get('caveats')  # type: ignore
    
    
    @caveats.setter
    def caveats(self, value: Union[None, "CaveatDetailsClass"]):
        """Setter: This section should list additional concerns that were not covered in the previous sections. For example, did the results suggest any further testing? Were there any relevant groups that were not represented in the evaluation dataset?"""
        self._inner_dict['caveats'] = value
    
    
    @property
    def recommendations(self) -> Union[None, str]:
        """Getter: Recommendations on where this MLModel should be used."""
        return self._inner_dict.get('recommendations')  # type: ignore
    
    
    @recommendations.setter
    def recommendations(self, value: Union[None, str]):
        """Setter: Recommendations on where this MLModel should be used."""
        self._inner_dict['recommendations'] = value
    
    
    @property
    def idealDatasetCharacteristics(self) -> Union[None, List[str]]:
        """Getter: Ideal characteristics of an evaluation dataset for this MLModel"""
        return self._inner_dict.get('idealDatasetCharacteristics')  # type: ignore
    
    
    @idealDatasetCharacteristics.setter
    def idealDatasetCharacteristics(self, value: Union[None, List[str]]):
        """Setter: Ideal characteristics of an evaluation dataset for this MLModel"""
        self._inner_dict['idealDatasetCharacteristics'] = value
    
    
class EthicalConsiderationsClass(DictWrapper):
    """This section is intended to demonstrate the ethical considerations that went into MLModel development, surfacing ethical challenges and solutions to stakeholders."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.EthicalConsiderations")
    
    @overload
    def __init__(self,
        data: Optional[Union[None, List[str]]]=None,
        humanLife: Optional[Union[None, List[str]]]=None,
        mitigations: Optional[Union[None, List[str]]]=None,
        risksAndHarms: Optional[Union[None, List[str]]]=None,
        useCases: Optional[Union[None, List[str]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(EthicalConsiderationsClass, self).__init__({})
        self.data = self.RECORD_SCHEMA.field_map["data"].default
        self.humanLife = self.RECORD_SCHEMA.field_map["humanLife"].default
        self.mitigations = self.RECORD_SCHEMA.field_map["mitigations"].default
        self.risksAndHarms = self.RECORD_SCHEMA.field_map["risksAndHarms"].default
        self.useCases = self.RECORD_SCHEMA.field_map["useCases"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def data(self) -> Union[None, List[str]]:
        """Getter: Does the MLModel use any sensitive data (e.g., protected classes)?"""
        return self._inner_dict.get('data')  # type: ignore
    
    
    @data.setter
    def data(self, value: Union[None, List[str]]):
        """Setter: Does the MLModel use any sensitive data (e.g., protected classes)?"""
        self._inner_dict['data'] = value
    
    
    @property
    def humanLife(self) -> Union[None, List[str]]:
        """Getter:  Is the MLModel intended to inform decisions about matters central to human life or flourishing – e.g., health or safety? Or could it be used in such a way?"""
        return self._inner_dict.get('humanLife')  # type: ignore
    
    
    @humanLife.setter
    def humanLife(self, value: Union[None, List[str]]):
        """Setter:  Is the MLModel intended to inform decisions about matters central to human life or flourishing – e.g., health or safety? Or could it be used in such a way?"""
        self._inner_dict['humanLife'] = value
    
    
    @property
    def mitigations(self) -> Union[None, List[str]]:
        """Getter: What risk mitigation strategies were used during MLModel development?"""
        return self._inner_dict.get('mitigations')  # type: ignore
    
    
    @mitigations.setter
    def mitigations(self, value: Union[None, List[str]]):
        """Setter: What risk mitigation strategies were used during MLModel development?"""
        self._inner_dict['mitigations'] = value
    
    
    @property
    def risksAndHarms(self) -> Union[None, List[str]]:
        """Getter: What risks may be present in MLModel usage? Try to identify the potential recipients, likelihood, and magnitude of harms. If these cannot be determined, note that they were considered but remain unknown."""
        return self._inner_dict.get('risksAndHarms')  # type: ignore
    
    
    @risksAndHarms.setter
    def risksAndHarms(self, value: Union[None, List[str]]):
        """Setter: What risks may be present in MLModel usage? Try to identify the potential recipients, likelihood, and magnitude of harms. If these cannot be determined, note that they were considered but remain unknown."""
        self._inner_dict['risksAndHarms'] = value
    
    
    @property
    def useCases(self) -> Union[None, List[str]]:
        """Getter: Are there any known MLModel use cases that are especially fraught? This may connect directly to the intended use section"""
        return self._inner_dict.get('useCases')  # type: ignore
    
    
    @useCases.setter
    def useCases(self, value: Union[None, List[str]]):
        """Setter: Are there any known MLModel use cases that are especially fraught? This may connect directly to the intended use section"""
        self._inner_dict['useCases'] = value
    
    
class EvaluationDataClass(DictWrapper):
    """All referenced datasets would ideally point to any set of documents that provide visibility into the source and composition of the dataset."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.EvaluationData")
    
    @overload
    def __init__(self,
        evaluationData: Optional[List["BaseDataClass"]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(EvaluationDataClass, self).__init__({})
        self.evaluationData = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def evaluationData(self) -> List["BaseDataClass"]:
        """Getter: Details on the dataset(s) used for the quantitative analyses in the MLModel"""
        return self._inner_dict.get('evaluationData')  # type: ignore
    
    
    @evaluationData.setter
    def evaluationData(self, value: List["BaseDataClass"]):
        """Setter: Details on the dataset(s) used for the quantitative analyses in the MLModel"""
        self._inner_dict['evaluationData'] = value
    
    
class IntendedUseClass(DictWrapper):
    """Intended Use for the ML Model"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.IntendedUse")
    
    @overload
    def __init__(self,
        primaryUses: Optional[Union[None, List[str]]]=None,
        primaryUsers: Optional[Union[None, List["IntendedUserTypeClass"]]]=None,
        outOfScopeUses: Optional[Union[None, List[str]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(IntendedUseClass, self).__init__({})
        self.primaryUses = self.RECORD_SCHEMA.field_map["primaryUses"].default
        self.primaryUsers = self.RECORD_SCHEMA.field_map["primaryUsers"].default
        self.outOfScopeUses = self.RECORD_SCHEMA.field_map["outOfScopeUses"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def primaryUses(self) -> Union[None, List[str]]:
        """Getter: Primary Use cases for the MLModel."""
        return self._inner_dict.get('primaryUses')  # type: ignore
    
    
    @primaryUses.setter
    def primaryUses(self, value: Union[None, List[str]]):
        """Setter: Primary Use cases for the MLModel."""
        self._inner_dict['primaryUses'] = value
    
    
    @property
    def primaryUsers(self) -> Union[None, List["IntendedUserTypeClass"]]:
        """Getter: Primary Intended Users - For example, was the MLModel developed for entertainment purposes, for hobbyists, or enterprise solutions?"""
        return self._inner_dict.get('primaryUsers')  # type: ignore
    
    
    @primaryUsers.setter
    def primaryUsers(self, value: Union[None, List["IntendedUserTypeClass"]]):
        """Setter: Primary Intended Users - For example, was the MLModel developed for entertainment purposes, for hobbyists, or enterprise solutions?"""
        self._inner_dict['primaryUsers'] = value
    
    
    @property
    def outOfScopeUses(self) -> Union[None, List[str]]:
        """Getter: Highlight technology that the MLModel might easily be confused with, or related contexts that users could try to apply the MLModel to."""
        return self._inner_dict.get('outOfScopeUses')  # type: ignore
    
    
    @outOfScopeUses.setter
    def outOfScopeUses(self, value: Union[None, List[str]]):
        """Setter: Highlight technology that the MLModel might easily be confused with, or related contexts that users could try to apply the MLModel to."""
        self._inner_dict['outOfScopeUses'] = value
    
    
class IntendedUserTypeClass(object):
    # No docs available.
    
    ENTERPRISE = "ENTERPRISE"
    HOBBY = "HOBBY"
    ENTERTAINMENT = "ENTERTAINMENT"
    
    
class MLFeaturePropertiesClass(DictWrapper):
    """Properties associated with a MLFeature"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.MLFeatureProperties")
    
    @overload
    def __init__(self,
        description: Optional[Union[None, str]]=None,
        dataType: Optional[Union[None, "MLFeatureDataTypeClass"]]=None,
        version: Optional[Union[None, "VersionTagClass"]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(MLFeaturePropertiesClass, self).__init__({})
        self.description = self.RECORD_SCHEMA.field_map["description"].default
        self.dataType = self.RECORD_SCHEMA.field_map["dataType"].default
        self.version = self.RECORD_SCHEMA.field_map["version"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def description(self) -> Union[None, str]:
        """Getter: Documentation of the MLFeature"""
        return self._inner_dict.get('description')  # type: ignore
    
    
    @description.setter
    def description(self, value: Union[None, str]):
        """Setter: Documentation of the MLFeature"""
        self._inner_dict['description'] = value
    
    
    @property
    def dataType(self) -> Union[None, "MLFeatureDataTypeClass"]:
        """Getter: Data Type of the MLFeature"""
        return self._inner_dict.get('dataType')  # type: ignore
    
    
    @dataType.setter
    def dataType(self, value: Union[None, "MLFeatureDataTypeClass"]):
        """Setter: Data Type of the MLFeature"""
        self._inner_dict['dataType'] = value
    
    
    @property
    def version(self) -> Union[None, "VersionTagClass"]:
        """Getter: Version of the MLFeature"""
        return self._inner_dict.get('version')  # type: ignore
    
    
    @version.setter
    def version(self, value: Union[None, "VersionTagClass"]):
        """Setter: Version of the MLFeature"""
        self._inner_dict['version'] = value
    
    
class MLModelFactorPromptsClass(DictWrapper):
    """Prompts which affect the performance of the MLModel"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.MLModelFactorPrompts")
    
    @overload
    def __init__(self,
        relevantFactors: Optional[Union[None, List["MLModelFactorsClass"]]]=None,
        evaluationFactors: Optional[Union[None, List["MLModelFactorsClass"]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(MLModelFactorPromptsClass, self).__init__({})
        self.relevantFactors = self.RECORD_SCHEMA.field_map["relevantFactors"].default
        self.evaluationFactors = self.RECORD_SCHEMA.field_map["evaluationFactors"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def relevantFactors(self) -> Union[None, List["MLModelFactorsClass"]]:
        """Getter: What are foreseeable salient factors for which MLModel performance may vary, and how were these determined?"""
        return self._inner_dict.get('relevantFactors')  # type: ignore
    
    
    @relevantFactors.setter
    def relevantFactors(self, value: Union[None, List["MLModelFactorsClass"]]):
        """Setter: What are foreseeable salient factors for which MLModel performance may vary, and how were these determined?"""
        self._inner_dict['relevantFactors'] = value
    
    
    @property
    def evaluationFactors(self) -> Union[None, List["MLModelFactorsClass"]]:
        """Getter: Which factors are being reported, and why were these chosen?"""
        return self._inner_dict.get('evaluationFactors')  # type: ignore
    
    
    @evaluationFactors.setter
    def evaluationFactors(self, value: Union[None, List["MLModelFactorsClass"]]):
        """Setter: Which factors are being reported, and why were these chosen?"""
        self._inner_dict['evaluationFactors'] = value
    
    
class MLModelFactorsClass(DictWrapper):
    """Factors affecting the performance of the MLModel."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.MLModelFactors")
    
    @overload
    def __init__(self,
        groups: Optional[Union[None, List[str]]]=None,
        instrumentation: Optional[Union[None, List[str]]]=None,
        environment: Optional[Union[None, List[str]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(MLModelFactorsClass, self).__init__({})
        self.groups = self.RECORD_SCHEMA.field_map["groups"].default
        self.instrumentation = self.RECORD_SCHEMA.field_map["instrumentation"].default
        self.environment = self.RECORD_SCHEMA.field_map["environment"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def groups(self) -> Union[None, List[str]]:
        """Getter: Groups refers to distinct categories with similar characteristics that are present in the evaluation data instances.
    For human-centric machine learning MLModels, groups are people who share one or multiple characteristics."""
        return self._inner_dict.get('groups')  # type: ignore
    
    
    @groups.setter
    def groups(self, value: Union[None, List[str]]):
        """Setter: Groups refers to distinct categories with similar characteristics that are present in the evaluation data instances.
    For human-centric machine learning MLModels, groups are people who share one or multiple characteristics."""
        self._inner_dict['groups'] = value
    
    
    @property
    def instrumentation(self) -> Union[None, List[str]]:
        """Getter: The performance of a MLModel can vary depending on what instruments were used to capture the input to the MLModel.
    For example, a face detection model may perform differently depending on the camera’s hardware and software,
    including lens, image stabilization, high dynamic range techniques, and background blurring for portrait mode."""
        return self._inner_dict.get('instrumentation')  # type: ignore
    
    
    @instrumentation.setter
    def instrumentation(self, value: Union[None, List[str]]):
        """Setter: The performance of a MLModel can vary depending on what instruments were used to capture the input to the MLModel.
    For example, a face detection model may perform differently depending on the camera’s hardware and software,
    including lens, image stabilization, high dynamic range techniques, and background blurring for portrait mode."""
        self._inner_dict['instrumentation'] = value
    
    
    @property
    def environment(self) -> Union[None, List[str]]:
        """Getter: A further factor affecting MLModel performance is the environment in which it is deployed."""
        return self._inner_dict.get('environment')  # type: ignore
    
    
    @environment.setter
    def environment(self, value: Union[None, List[str]]):
        """Setter: A further factor affecting MLModel performance is the environment in which it is deployed."""
        self._inner_dict['environment'] = value
    
    
class MLModelPropertiesClass(DictWrapper):
    """Properties associated with a ML Model"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.MLModelProperties")
    
    @overload
    def __init__(self,
        description: Optional[Union[None, str]]=None,
        date: Optional[Union[None, int]]=None,
        version: Optional[Union[None, "VersionTagClass"]]=None,
        type: Optional[Union[None, str]]=None,
        hyperParameters: Optional[Union[None, Dict[str, Union[str, int, float, float, bool]]]]=None,
        mlFeatures: Optional[Union[None, List[str]]]=None,
        tags: Optional[List[str]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(MLModelPropertiesClass, self).__init__({})
        self.description = self.RECORD_SCHEMA.field_map["description"].default
        self.date = self.RECORD_SCHEMA.field_map["date"].default
        self.version = self.RECORD_SCHEMA.field_map["version"].default
        self.type = self.RECORD_SCHEMA.field_map["type"].default
        self.hyperParameters = self.RECORD_SCHEMA.field_map["hyperParameters"].default
        self.mlFeatures = self.RECORD_SCHEMA.field_map["mlFeatures"].default
        self.tags = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def description(self) -> Union[None, str]:
        """Getter: Documentation of the MLModel"""
        return self._inner_dict.get('description')  # type: ignore
    
    
    @description.setter
    def description(self, value: Union[None, str]):
        """Setter: Documentation of the MLModel"""
        self._inner_dict['description'] = value
    
    
    @property
    def date(self) -> Union[None, int]:
        """Getter: Date when the MLModel was developed"""
        return self._inner_dict.get('date')  # type: ignore
    
    
    @date.setter
    def date(self, value: Union[None, int]):
        """Setter: Date when the MLModel was developed"""
        self._inner_dict['date'] = value
    
    
    @property
    def version(self) -> Union[None, "VersionTagClass"]:
        """Getter: Version of the MLModel"""
        return self._inner_dict.get('version')  # type: ignore
    
    
    @version.setter
    def version(self, value: Union[None, "VersionTagClass"]):
        """Setter: Version of the MLModel"""
        self._inner_dict['version'] = value
    
    
    @property
    def type(self) -> Union[None, str]:
        """Getter: Type of Algorithm or MLModel such as whether it is a Naive Bayes classifier, Convolutional Neural Network, etc"""
        return self._inner_dict.get('type')  # type: ignore
    
    
    @type.setter
    def type(self, value: Union[None, str]):
        """Setter: Type of Algorithm or MLModel such as whether it is a Naive Bayes classifier, Convolutional Neural Network, etc"""
        self._inner_dict['type'] = value
    
    
    @property
    def hyperParameters(self) -> Union[None, Dict[str, Union[str, int, float, float, bool]]]:
        """Getter: Hyper Parameters of the MLModel"""
        return self._inner_dict.get('hyperParameters')  # type: ignore
    
    
    @hyperParameters.setter
    def hyperParameters(self, value: Union[None, Dict[str, Union[str, int, float, float, bool]]]):
        """Setter: Hyper Parameters of the MLModel"""
        self._inner_dict['hyperParameters'] = value
    
    
    @property
    def mlFeatures(self) -> Union[None, List[str]]:
        """Getter: List of features used for MLModel training"""
        return self._inner_dict.get('mlFeatures')  # type: ignore
    
    
    @mlFeatures.setter
    def mlFeatures(self, value: Union[None, List[str]]):
        """Setter: List of features used for MLModel training"""
        self._inner_dict['mlFeatures'] = value
    
    
    @property
    def tags(self) -> List[str]:
        """Getter: Tags for the MLModel"""
        return self._inner_dict.get('tags')  # type: ignore
    
    
    @tags.setter
    def tags(self, value: List[str]):
        """Setter: Tags for the MLModel"""
        self._inner_dict['tags'] = value
    
    
class MetricsClass(DictWrapper):
    """Metrics to be featured for the MLModel."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.Metrics")
    
    @overload
    def __init__(self,
        performanceMeasures: Optional[Union[None, List[str]]]=None,
        decisionThreshold: Optional[Union[None, List[str]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(MetricsClass, self).__init__({})
        self.performanceMeasures = self.RECORD_SCHEMA.field_map["performanceMeasures"].default
        self.decisionThreshold = self.RECORD_SCHEMA.field_map["decisionThreshold"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def performanceMeasures(self) -> Union[None, List[str]]:
        """Getter: Measures of MLModel performance"""
        return self._inner_dict.get('performanceMeasures')  # type: ignore
    
    
    @performanceMeasures.setter
    def performanceMeasures(self, value: Union[None, List[str]]):
        """Setter: Measures of MLModel performance"""
        self._inner_dict['performanceMeasures'] = value
    
    
    @property
    def decisionThreshold(self) -> Union[None, List[str]]:
        """Getter: Decision Thresholds used (if any)?"""
        return self._inner_dict.get('decisionThreshold')  # type: ignore
    
    
    @decisionThreshold.setter
    def decisionThreshold(self, value: Union[None, List[str]]):
        """Setter: Decision Thresholds used (if any)?"""
        self._inner_dict['decisionThreshold'] = value
    
    
class QuantitativeAnalysesClass(DictWrapper):
    """Quantitative analyses should be disaggregated, that is, broken down by the chosen factors. Quantitative analyses should provide the results of evaluating the MLModel according to the chosen metrics, providing confidence interval values when possible."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.QuantitativeAnalyses")
    
    @overload
    def __init__(self,
        unitaryResults: Optional[Union[None, str]]=None,
        intersectionalResults: Optional[Union[None, str]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(QuantitativeAnalysesClass, self).__init__({})
        self.unitaryResults = self.RECORD_SCHEMA.field_map["unitaryResults"].default
        self.intersectionalResults = self.RECORD_SCHEMA.field_map["intersectionalResults"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def unitaryResults(self) -> Union[None, str]:
        """Getter: Link to a dashboard with results showing how the MLModel performed with respect to each factor"""
        return self._inner_dict.get('unitaryResults')  # type: ignore
    
    
    @unitaryResults.setter
    def unitaryResults(self, value: Union[None, str]):
        """Setter: Link to a dashboard with results showing how the MLModel performed with respect to each factor"""
        self._inner_dict['unitaryResults'] = value
    
    
    @property
    def intersectionalResults(self) -> Union[None, str]:
        """Getter: Link to a dashboard with results showing how the MLModel performed with respect to the intersection of evaluated factors?"""
        return self._inner_dict.get('intersectionalResults')  # type: ignore
    
    
    @intersectionalResults.setter
    def intersectionalResults(self, value: Union[None, str]):
        """Setter: Link to a dashboard with results showing how the MLModel performed with respect to the intersection of evaluated factors?"""
        self._inner_dict['intersectionalResults'] = value
    
    
class SourceCodeClass(DictWrapper):
    """Source Code"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.SourceCode")
    
    @overload
    def __init__(self,
        sourceCode: Optional[List["SourceCodeUrlClass"]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(SourceCodeClass, self).__init__({})
        self.sourceCode = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def sourceCode(self) -> List["SourceCodeUrlClass"]:
        """Getter: Source Code along with types"""
        return self._inner_dict.get('sourceCode')  # type: ignore
    
    
    @sourceCode.setter
    def sourceCode(self, value: List["SourceCodeUrlClass"]):
        """Setter: Source Code along with types"""
        self._inner_dict['sourceCode'] = value
    
    
class SourceCodeUrlClass(DictWrapper):
    """Source Code Url Entity"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.SourceCodeUrl")
    
    @overload
    def __init__(self,
        type: Optional["SourceCodeUrlTypeClass"]=None,
        sourceCodeUrl: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(SourceCodeUrlClass, self).__init__({})
        self.type = SourceCodeUrlTypeClass.ML_MODEL_SOURCE_CODE
        self.sourceCodeUrl = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def type(self) -> "SourceCodeUrlTypeClass":
        """Getter: Source Code Url Types"""
        return self._inner_dict.get('type')  # type: ignore
    
    
    @type.setter
    def type(self, value: "SourceCodeUrlTypeClass"):
        """Setter: Source Code Url Types"""
        self._inner_dict['type'] = value
    
    
    @property
    def sourceCodeUrl(self) -> str:
        """Getter: Source Code Url"""
        return self._inner_dict.get('sourceCodeUrl')  # type: ignore
    
    
    @sourceCodeUrl.setter
    def sourceCodeUrl(self, value: str):
        """Setter: Source Code Url"""
        self._inner_dict['sourceCodeUrl'] = value
    
    
class SourceCodeUrlTypeClass(object):
    # No docs available.
    
    ML_MODEL_SOURCE_CODE = "ML_MODEL_SOURCE_CODE"
    TRAINING_PIPELINE_SOURCE_CODE = "TRAINING_PIPELINE_SOURCE_CODE"
    EVALUATION_PIPELINE_SOURCE_CODE = "EVALUATION_PIPELINE_SOURCE_CODE"
    
    
class TrainingDataClass(DictWrapper):
    """Ideally, the MLModel card would contain as much information about the training data as the evaluation data. However, there might be cases where it is not feasible to provide this level of detailed information about the training data. For example, the data may be proprietary, or require a non-disclosure agreement. In these cases, we advocate for basic details about the distributions over groups in the data, as well as any other details that could inform stakeholders on the kinds of biases the model may have encoded."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.ml.metadata.TrainingData")
    
    @overload
    def __init__(self,
        trainingData: Optional[List["BaseDataClass"]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(TrainingDataClass, self).__init__({})
        self.trainingData = list()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def trainingData(self) -> List["BaseDataClass"]:
        """Getter: Details on the dataset(s) used for training the MLModel"""
        return self._inner_dict.get('trainingData')  # type: ignore
    
    
    @trainingData.setter
    def trainingData(self, value: List["BaseDataClass"]):
        """Setter: Details on the dataset(s) used for training the MLModel"""
        self._inner_dict['trainingData'] = value
    
    
class MetadataChangeEventClass(DictWrapper):
    """Kafka event for proposing a metadata change for an entity. A corresponding MetadataAuditEvent is emitted when the change is accepted and committed, otherwise a FailedMetadataChangeEvent will be emitted instead."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.mxe.MetadataChangeEvent")
    
    @overload
    def __init__(self,
        auditHeader: Optional[Union[None, "KafkaAuditHeaderClass"]]=None,
        proposedSnapshot: Optional[Union["ChartSnapshotClass", "CorpGroupSnapshotClass", "CorpUserSnapshotClass", "DashboardSnapshotClass", "DatasetSnapshotClass", "DataProcessSnapshotClass", "MLModelSnapshotClass", "MLFeatureSnapshotClass"]]=None,
        proposedDelta: Optional[None]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(MetadataChangeEventClass, self).__init__({})
        self.auditHeader = self.RECORD_SCHEMA.field_map["auditHeader"].default
        self.proposedSnapshot = ChartSnapshotClass()
        self.proposedDelta = self.RECORD_SCHEMA.field_map["proposedDelta"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def auditHeader(self) -> Union[None, "KafkaAuditHeaderClass"]:
        """Getter: Kafka audit header. See go/kafkaauditheader for more info."""
        return self._inner_dict.get('auditHeader')  # type: ignore
    
    
    @auditHeader.setter
    def auditHeader(self, value: Union[None, "KafkaAuditHeaderClass"]):
        """Setter: Kafka audit header. See go/kafkaauditheader for more info."""
        self._inner_dict['auditHeader'] = value
    
    
    @property
    def proposedSnapshot(self) -> Union["ChartSnapshotClass", "CorpGroupSnapshotClass", "CorpUserSnapshotClass", "DashboardSnapshotClass", "DatasetSnapshotClass", "DataProcessSnapshotClass", "MLModelSnapshotClass", "MLFeatureSnapshotClass"]:
        """Getter: Snapshot of the proposed metadata change. Include only the aspects affected by the change in the snapshot."""
        return self._inner_dict.get('proposedSnapshot')  # type: ignore
    
    
    @proposedSnapshot.setter
    def proposedSnapshot(self, value: Union["ChartSnapshotClass", "CorpGroupSnapshotClass", "CorpUserSnapshotClass", "DashboardSnapshotClass", "DatasetSnapshotClass", "DataProcessSnapshotClass", "MLModelSnapshotClass", "MLFeatureSnapshotClass"]):
        """Setter: Snapshot of the proposed metadata change. Include only the aspects affected by the change in the snapshot."""
        self._inner_dict['proposedSnapshot'] = value
    
    
    @property
    def proposedDelta(self) -> None:
        """Getter: Delta of the proposed metadata partial update."""
        return self._inner_dict.get('proposedDelta')  # type: ignore
    
    
    @proposedDelta.setter
    def proposedDelta(self, value: None):
        """Setter: Delta of the proposed metadata partial update."""
        self._inner_dict['proposedDelta'] = value
    
    
class ArrayTypeClass(DictWrapper):
    """Array field type."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.ArrayType")
    
    @overload
    def __init__(self,
        nestedType: Optional[Union[None, List[str]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(ArrayTypeClass, self).__init__({})
        self.nestedType = self.RECORD_SCHEMA.field_map["nestedType"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def nestedType(self) -> Union[None, List[str]]:
        """Getter: List of types this array holds."""
        return self._inner_dict.get('nestedType')  # type: ignore
    
    
    @nestedType.setter
    def nestedType(self, value: Union[None, List[str]]):
        """Setter: List of types this array holds."""
        self._inner_dict['nestedType'] = value
    
    
class BinaryJsonSchemaClass(DictWrapper):
    """Schema text of binary JSON schema."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.BinaryJsonSchema")
    
    @overload
    def __init__(self,
        schema: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(BinaryJsonSchemaClass, self).__init__({})
        self.schema = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def schema(self) -> str:
        """Getter: The native schema text for binary JSON file format."""
        return self._inner_dict.get('schema')  # type: ignore
    
    
    @schema.setter
    def schema(self, value: str):
        """Setter: The native schema text for binary JSON file format."""
        self._inner_dict['schema'] = value
    
    
class BooleanTypeClass(DictWrapper):
    """Boolean field type."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.BooleanType")
    
    @overload
    def __init__(self,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(BooleanTypeClass, self).__init__({})
        pass
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
class BytesTypeClass(DictWrapper):
    """Bytes field type."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.BytesType")
    
    @overload
    def __init__(self,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(BytesTypeClass, self).__init__({})
        pass
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
class DatasetFieldForeignKeyClass(DictWrapper):
    """For non-urn based foregin keys."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.DatasetFieldForeignKey")
    
    @overload
    def __init__(self,
        parentDataset: Optional[str]=None,
        currentFieldPaths: Optional[List[str]]=None,
        parentField: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(DatasetFieldForeignKeyClass, self).__init__({})
        self.parentDataset = str()
        self.currentFieldPaths = list()
        self.parentField = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def parentDataset(self) -> str:
        """Getter: dataset that stores the resource."""
        return self._inner_dict.get('parentDataset')  # type: ignore
    
    
    @parentDataset.setter
    def parentDataset(self, value: str):
        """Setter: dataset that stores the resource."""
        self._inner_dict['parentDataset'] = value
    
    
    @property
    def currentFieldPaths(self) -> List[str]:
        """Getter: List of fields in hosting(current) SchemaMetadata that conform a foreign key. List can contain a single entry or multiple entries if several entries in hosting schema conform a foreign key in a single parent dataset."""
        return self._inner_dict.get('currentFieldPaths')  # type: ignore
    
    
    @currentFieldPaths.setter
    def currentFieldPaths(self, value: List[str]):
        """Setter: List of fields in hosting(current) SchemaMetadata that conform a foreign key. List can contain a single entry or multiple entries if several entries in hosting schema conform a foreign key in a single parent dataset."""
        self._inner_dict['currentFieldPaths'] = value
    
    
    @property
    def parentField(self) -> str:
        """Getter: SchemaField@fieldPath that uniquely identify field in parent dataset that this field references."""
        return self._inner_dict.get('parentField')  # type: ignore
    
    
    @parentField.setter
    def parentField(self, value: str):
        """Setter: SchemaField@fieldPath that uniquely identify field in parent dataset that this field references."""
        self._inner_dict['parentField'] = value
    
    
class EnumTypeClass(DictWrapper):
    """Enum field type."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.EnumType")
    
    @overload
    def __init__(self,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(EnumTypeClass, self).__init__({})
        pass
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
class EspressoSchemaClass(DictWrapper):
    """Schema text of an espresso table schema."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.EspressoSchema")
    
    @overload
    def __init__(self,
        documentSchema: Optional[str]=None,
        tableSchema: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(EspressoSchemaClass, self).__init__({})
        self.documentSchema = str()
        self.tableSchema = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def documentSchema(self) -> str:
        """Getter: The native espresso document schema."""
        return self._inner_dict.get('documentSchema')  # type: ignore
    
    
    @documentSchema.setter
    def documentSchema(self, value: str):
        """Setter: The native espresso document schema."""
        self._inner_dict['documentSchema'] = value
    
    
    @property
    def tableSchema(self) -> str:
        """Getter: The espresso table schema definition."""
        return self._inner_dict.get('tableSchema')  # type: ignore
    
    
    @tableSchema.setter
    def tableSchema(self, value: str):
        """Setter: The espresso table schema definition."""
        self._inner_dict['tableSchema'] = value
    
    
class FixedTypeClass(DictWrapper):
    """Fixed field type."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.FixedType")
    
    @overload
    def __init__(self,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(FixedTypeClass, self).__init__({})
        pass
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
class ForeignKeySpecClass(DictWrapper):
    """Description of a foreign key in a schema."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.ForeignKeySpec")
    
    @overload
    def __init__(self,
        foreignKey: Optional[Union["DatasetFieldForeignKeyClass", "UrnForeignKeyClass"]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(ForeignKeySpecClass, self).__init__({})
        self.foreignKey = DatasetFieldForeignKeyClass()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def foreignKey(self) -> Union["DatasetFieldForeignKeyClass", "UrnForeignKeyClass"]:
        """Getter: Foreign key definition in metadata schema."""
        return self._inner_dict.get('foreignKey')  # type: ignore
    
    
    @foreignKey.setter
    def foreignKey(self, value: Union["DatasetFieldForeignKeyClass", "UrnForeignKeyClass"]):
        """Setter: Foreign key definition in metadata schema."""
        self._inner_dict['foreignKey'] = value
    
    
class KafkaSchemaClass(DictWrapper):
    """Schema holder for kafka schema."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.KafkaSchema")
    
    @overload
    def __init__(self,
        documentSchema: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(KafkaSchemaClass, self).__init__({})
        self.documentSchema = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def documentSchema(self) -> str:
        """Getter: The native kafka document schema. This is a human readable avro document schema."""
        return self._inner_dict.get('documentSchema')  # type: ignore
    
    
    @documentSchema.setter
    def documentSchema(self, value: str):
        """Setter: The native kafka document schema. This is a human readable avro document schema."""
        self._inner_dict['documentSchema'] = value
    
    
class KeyValueSchemaClass(DictWrapper):
    """Schema text of a key-value store schema."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.KeyValueSchema")
    
    @overload
    def __init__(self,
        keySchema: Optional[str]=None,
        valueSchema: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(KeyValueSchemaClass, self).__init__({})
        self.keySchema = str()
        self.valueSchema = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def keySchema(self) -> str:
        """Getter: The raw schema for the key in the key-value store."""
        return self._inner_dict.get('keySchema')  # type: ignore
    
    
    @keySchema.setter
    def keySchema(self, value: str):
        """Setter: The raw schema for the key in the key-value store."""
        self._inner_dict['keySchema'] = value
    
    
    @property
    def valueSchema(self) -> str:
        """Getter: The raw schema for the value in the key-value store."""
        return self._inner_dict.get('valueSchema')  # type: ignore
    
    
    @valueSchema.setter
    def valueSchema(self, value: str):
        """Setter: The raw schema for the value in the key-value store."""
        self._inner_dict['valueSchema'] = value
    
    
class MapTypeClass(DictWrapper):
    """Map field type."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.MapType")
    
    @overload
    def __init__(self,
        keyType: Optional[Union[None, str]]=None,
        valueType: Optional[Union[None, str]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(MapTypeClass, self).__init__({})
        self.keyType = self.RECORD_SCHEMA.field_map["keyType"].default
        self.valueType = self.RECORD_SCHEMA.field_map["valueType"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def keyType(self) -> Union[None, str]:
        """Getter: Key type in a map"""
        return self._inner_dict.get('keyType')  # type: ignore
    
    
    @keyType.setter
    def keyType(self, value: Union[None, str]):
        """Setter: Key type in a map"""
        self._inner_dict['keyType'] = value
    
    
    @property
    def valueType(self) -> Union[None, str]:
        """Getter: Type of the value in a map"""
        return self._inner_dict.get('valueType')  # type: ignore
    
    
    @valueType.setter
    def valueType(self, value: Union[None, str]):
        """Setter: Type of the value in a map"""
        self._inner_dict['valueType'] = value
    
    
class MySqlDDLClass(DictWrapper):
    """Schema holder for MySql data definition language that describes an MySql table."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.MySqlDDL")
    
    @overload
    def __init__(self,
        tableSchema: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(MySqlDDLClass, self).__init__({})
        self.tableSchema = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def tableSchema(self) -> str:
        """Getter: The native schema in the dataset's platform. This is a human readable (json blob) table schema."""
        return self._inner_dict.get('tableSchema')  # type: ignore
    
    
    @tableSchema.setter
    def tableSchema(self, value: str):
        """Setter: The native schema in the dataset's platform. This is a human readable (json blob) table schema."""
        self._inner_dict['tableSchema'] = value
    
    
class NullTypeClass(DictWrapper):
    """Null field type."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.NullType")
    
    @overload
    def __init__(self,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(NullTypeClass, self).__init__({})
        pass
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
class NumberTypeClass(DictWrapper):
    """Number data type: long, integer, short, etc.."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.NumberType")
    
    @overload
    def __init__(self,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(NumberTypeClass, self).__init__({})
        pass
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
class OracleDDLClass(DictWrapper):
    """Schema holder for oracle data definition language that describes an oracle table."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.OracleDDL")
    
    @overload
    def __init__(self,
        tableSchema: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(OracleDDLClass, self).__init__({})
        self.tableSchema = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def tableSchema(self) -> str:
        """Getter: The native schema in the dataset's platform. This is a human readable (json blob) table schema."""
        return self._inner_dict.get('tableSchema')  # type: ignore
    
    
    @tableSchema.setter
    def tableSchema(self, value: str):
        """Setter: The native schema in the dataset's platform. This is a human readable (json blob) table schema."""
        self._inner_dict['tableSchema'] = value
    
    
class OrcSchemaClass(DictWrapper):
    """Schema text of an ORC schema."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.OrcSchema")
    
    @overload
    def __init__(self,
        schema: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(OrcSchemaClass, self).__init__({})
        self.schema = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def schema(self) -> str:
        """Getter: The native schema for ORC file format."""
        return self._inner_dict.get('schema')  # type: ignore
    
    
    @schema.setter
    def schema(self, value: str):
        """Setter: The native schema for ORC file format."""
        self._inner_dict['schema'] = value
    
    
class OtherSchemaClass(DictWrapper):
    """Schema holder for undefined schema types."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.OtherSchema")
    
    @overload
    def __init__(self,
        rawSchema: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(OtherSchemaClass, self).__init__({})
        self.rawSchema = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def rawSchema(self) -> str:
        """Getter: The native schema in the dataset's platform."""
        return self._inner_dict.get('rawSchema')  # type: ignore
    
    
    @rawSchema.setter
    def rawSchema(self, value: str):
        """Setter: The native schema in the dataset's platform."""
        self._inner_dict['rawSchema'] = value
    
    
class PrestoDDLClass(DictWrapper):
    """Schema holder for presto data definition language that describes a presto view."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.PrestoDDL")
    
    @overload
    def __init__(self,
        rawSchema: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(PrestoDDLClass, self).__init__({})
        self.rawSchema = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def rawSchema(self) -> str:
        """Getter: The raw schema in the dataset's platform. This includes the DDL and the columns extracted from DDL."""
        return self._inner_dict.get('rawSchema')  # type: ignore
    
    
    @rawSchema.setter
    def rawSchema(self, value: str):
        """Setter: The raw schema in the dataset's platform. This includes the DDL and the columns extracted from DDL."""
        self._inner_dict['rawSchema'] = value
    
    
class RecordTypeClass(DictWrapper):
    """Record field type."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.RecordType")
    
    @overload
    def __init__(self,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(RecordTypeClass, self).__init__({})
        pass
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
class SchemaFieldClass(DictWrapper):
    """SchemaField to describe metadata related to dataset schema. Schema normalization rules: http://go/tms-schema"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.SchemaField")
    
    @overload
    def __init__(self,
        fieldPath: Optional[str]=None,
        jsonPath: Optional[Union[None, str]]=None,
        nullable: Optional[bool]=None,
        description: Optional[Union[None, str]]=None,
        type: Optional["SchemaFieldDataTypeClass"]=None,
        nativeDataType: Optional[str]=None,
        recursive: Optional[bool]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(SchemaFieldClass, self).__init__({})
        self.fieldPath = str()
        self.jsonPath = self.RECORD_SCHEMA.field_map["jsonPath"].default
        self.nullable = self.RECORD_SCHEMA.field_map["nullable"].default
        self.description = self.RECORD_SCHEMA.field_map["description"].default
        self.type = SchemaFieldDataTypeClass()
        self.nativeDataType = str()
        self.recursive = self.RECORD_SCHEMA.field_map["recursive"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def fieldPath(self) -> str:
        """Getter: Flattened name of the field. Field is computed from jsonPath field. For data translation rules refer to wiki page above."""
        return self._inner_dict.get('fieldPath')  # type: ignore
    
    
    @fieldPath.setter
    def fieldPath(self, value: str):
        """Setter: Flattened name of the field. Field is computed from jsonPath field. For data translation rules refer to wiki page above."""
        self._inner_dict['fieldPath'] = value
    
    
    @property
    def jsonPath(self) -> Union[None, str]:
        """Getter: Flattened name of a field in JSON Path notation."""
        return self._inner_dict.get('jsonPath')  # type: ignore
    
    
    @jsonPath.setter
    def jsonPath(self, value: Union[None, str]):
        """Setter: Flattened name of a field in JSON Path notation."""
        self._inner_dict['jsonPath'] = value
    
    
    @property
    def nullable(self) -> bool:
        """Getter: Indicates if this field is optional or nullable"""
        return self._inner_dict.get('nullable')  # type: ignore
    
    
    @nullable.setter
    def nullable(self, value: bool):
        """Setter: Indicates if this field is optional or nullable"""
        self._inner_dict['nullable'] = value
    
    
    @property
    def description(self) -> Union[None, str]:
        """Getter: Description"""
        return self._inner_dict.get('description')  # type: ignore
    
    
    @description.setter
    def description(self, value: Union[None, str]):
        """Setter: Description"""
        self._inner_dict['description'] = value
    
    
    @property
    def type(self) -> "SchemaFieldDataTypeClass":
        """Getter: Platform independent field type of the field."""
        return self._inner_dict.get('type')  # type: ignore
    
    
    @type.setter
    def type(self, value: "SchemaFieldDataTypeClass"):
        """Setter: Platform independent field type of the field."""
        self._inner_dict['type'] = value
    
    
    @property
    def nativeDataType(self) -> str:
        """Getter: The native type of the field in the dataset's platform as declared by platform schema."""
        return self._inner_dict.get('nativeDataType')  # type: ignore
    
    
    @nativeDataType.setter
    def nativeDataType(self, value: str):
        """Setter: The native type of the field in the dataset's platform as declared by platform schema."""
        self._inner_dict['nativeDataType'] = value
    
    
    @property
    def recursive(self) -> bool:
        """Getter: There are use cases when a field in type B references type A. A field in A references field of type B. In such cases, we will mark the first field as recursive."""
        return self._inner_dict.get('recursive')  # type: ignore
    
    
    @recursive.setter
    def recursive(self, value: bool):
        """Setter: There are use cases when a field in type B references type A. A field in A references field of type B. In such cases, we will mark the first field as recursive."""
        self._inner_dict['recursive'] = value
    
    
class SchemaFieldDataTypeClass(DictWrapper):
    """Schema field data types"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.SchemaFieldDataType")
    
    @overload
    def __init__(self,
        type: Optional[Union["BooleanTypeClass", "FixedTypeClass", "StringTypeClass", "BytesTypeClass", "NumberTypeClass", "EnumTypeClass", "NullTypeClass", "MapTypeClass", "ArrayTypeClass", "UnionTypeClass", "RecordTypeClass"]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(SchemaFieldDataTypeClass, self).__init__({})
        self.type = BooleanTypeClass()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def type(self) -> Union["BooleanTypeClass", "FixedTypeClass", "StringTypeClass", "BytesTypeClass", "NumberTypeClass", "EnumTypeClass", "NullTypeClass", "MapTypeClass", "ArrayTypeClass", "UnionTypeClass", "RecordTypeClass"]:
        """Getter: Data platform specific types"""
        return self._inner_dict.get('type')  # type: ignore
    
    
    @type.setter
    def type(self, value: Union["BooleanTypeClass", "FixedTypeClass", "StringTypeClass", "BytesTypeClass", "NumberTypeClass", "EnumTypeClass", "NullTypeClass", "MapTypeClass", "ArrayTypeClass", "UnionTypeClass", "RecordTypeClass"]):
        """Setter: Data platform specific types"""
        self._inner_dict['type'] = value
    
    
class SchemaMetadataClass(DictWrapper):
    """SchemaMetadata to describe metadata related to store schema"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.SchemaMetadata")
    
    @overload
    def __init__(self,
        schemaName: Optional[str]=None,
        platform: Optional[str]=None,
        version: Optional[int]=None,
        created: Optional["AuditStampClass"]=None,
        lastModified: Optional["AuditStampClass"]=None,
        deleted: Optional[Union[None, "AuditStampClass"]]=None,
        dataset: Optional[Union[None, str]]=None,
        cluster: Optional[Union[None, str]]=None,
        hash: Optional[str]=None,
        platformSchema: Optional[Union["EspressoSchemaClass", "OracleDDLClass", "MySqlDDLClass", "PrestoDDLClass", "KafkaSchemaClass", "BinaryJsonSchemaClass", "OrcSchemaClass", "SchemalessClass", "KeyValueSchemaClass", "OtherSchemaClass"]]=None,
        fields: Optional[List["SchemaFieldClass"]]=None,
        primaryKeys: Optional[Union[None, List[str]]]=None,
        foreignKeysSpecs: Optional[Union[None, Dict[str, "ForeignKeySpecClass"]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(SchemaMetadataClass, self).__init__({})
        self.schemaName = str()
        self.platform = str()
        self.version = int()
        self.created = AuditStampClass()
        self.lastModified = AuditStampClass()
        self.deleted = self.RECORD_SCHEMA.field_map["deleted"].default
        self.dataset = self.RECORD_SCHEMA.field_map["dataset"].default
        self.cluster = self.RECORD_SCHEMA.field_map["cluster"].default
        self.hash = str()
        self.platformSchema = EspressoSchemaClass()
        self.fields = list()
        self.primaryKeys = self.RECORD_SCHEMA.field_map["primaryKeys"].default
        self.foreignKeysSpecs = self.RECORD_SCHEMA.field_map["foreignKeysSpecs"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def schemaName(self) -> str:
        """Getter: Schema name e.g. PageViewEvent, identity.Profile, ams.account_management_tracking"""
        return self._inner_dict.get('schemaName')  # type: ignore
    
    
    @schemaName.setter
    def schemaName(self, value: str):
        """Setter: Schema name e.g. PageViewEvent, identity.Profile, ams.account_management_tracking"""
        self._inner_dict['schemaName'] = value
    
    
    @property
    def platform(self) -> str:
        """Getter: Standardized platform urn where schema is defined. The data platform Urn (urn:li:platform:{platform_name})"""
        return self._inner_dict.get('platform')  # type: ignore
    
    
    @platform.setter
    def platform(self, value: str):
        """Setter: Standardized platform urn where schema is defined. The data platform Urn (urn:li:platform:{platform_name})"""
        self._inner_dict['platform'] = value
    
    
    @property
    def version(self) -> int:
        """Getter: Every change to SchemaMetadata in the resource results in a new version. Version is server assigned. This version is differ from platform native schema version."""
        return self._inner_dict.get('version')  # type: ignore
    
    
    @version.setter
    def version(self, value: int):
        """Setter: Every change to SchemaMetadata in the resource results in a new version. Version is server assigned. This version is differ from platform native schema version."""
        self._inner_dict['version'] = value
    
    
    @property
    def created(self) -> "AuditStampClass":
        """Getter: An AuditStamp corresponding to the creation of this resource/association/sub-resource"""
        return self._inner_dict.get('created')  # type: ignore
    
    
    @created.setter
    def created(self, value: "AuditStampClass"):
        """Setter: An AuditStamp corresponding to the creation of this resource/association/sub-resource"""
        self._inner_dict['created'] = value
    
    
    @property
    def lastModified(self) -> "AuditStampClass":
        """Getter: An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created"""
        return self._inner_dict.get('lastModified')  # type: ignore
    
    
    @lastModified.setter
    def lastModified(self, value: "AuditStampClass"):
        """Setter: An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created"""
        self._inner_dict['lastModified'] = value
    
    
    @property
    def deleted(self) -> Union[None, "AuditStampClass"]:
        """Getter: An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."""
        return self._inner_dict.get('deleted')  # type: ignore
    
    
    @deleted.setter
    def deleted(self, value: Union[None, "AuditStampClass"]):
        """Setter: An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."""
        self._inner_dict['deleted'] = value
    
    
    @property
    def dataset(self) -> Union[None, str]:
        """Getter: Dataset this schema metadata is associated with."""
        return self._inner_dict.get('dataset')  # type: ignore
    
    
    @dataset.setter
    def dataset(self, value: Union[None, str]):
        """Setter: Dataset this schema metadata is associated with."""
        self._inner_dict['dataset'] = value
    
    
    @property
    def cluster(self) -> Union[None, str]:
        """Getter: The cluster this schema metadata resides from"""
        return self._inner_dict.get('cluster')  # type: ignore
    
    
    @cluster.setter
    def cluster(self, value: Union[None, str]):
        """Setter: The cluster this schema metadata resides from"""
        self._inner_dict['cluster'] = value
    
    
    @property
    def hash(self) -> str:
        """Getter: the SHA1 hash of the schema content"""
        return self._inner_dict.get('hash')  # type: ignore
    
    
    @hash.setter
    def hash(self, value: str):
        """Setter: the SHA1 hash of the schema content"""
        self._inner_dict['hash'] = value
    
    
    @property
    def platformSchema(self) -> Union["EspressoSchemaClass", "OracleDDLClass", "MySqlDDLClass", "PrestoDDLClass", "KafkaSchemaClass", "BinaryJsonSchemaClass", "OrcSchemaClass", "SchemalessClass", "KeyValueSchemaClass", "OtherSchemaClass"]:
        """Getter: The native schema in the dataset's platform."""
        return self._inner_dict.get('platformSchema')  # type: ignore
    
    
    @platformSchema.setter
    def platformSchema(self, value: Union["EspressoSchemaClass", "OracleDDLClass", "MySqlDDLClass", "PrestoDDLClass", "KafkaSchemaClass", "BinaryJsonSchemaClass", "OrcSchemaClass", "SchemalessClass", "KeyValueSchemaClass", "OtherSchemaClass"]):
        """Setter: The native schema in the dataset's platform."""
        self._inner_dict['platformSchema'] = value
    
    
    @property
    def fields(self) -> List["SchemaFieldClass"]:
        """Getter: Client provided a list of fields from document schema."""
        return self._inner_dict.get('fields')  # type: ignore
    
    
    @fields.setter
    def fields(self, value: List["SchemaFieldClass"]):
        """Setter: Client provided a list of fields from document schema."""
        self._inner_dict['fields'] = value
    
    
    @property
    def primaryKeys(self) -> Union[None, List[str]]:
        """Getter: Client provided list of fields that define primary keys to access record. Field order defines hierarchical espresso keys. Empty lists indicates absence of primary key access patter. Value is a SchemaField@fieldPath."""
        return self._inner_dict.get('primaryKeys')  # type: ignore
    
    
    @primaryKeys.setter
    def primaryKeys(self, value: Union[None, List[str]]):
        """Setter: Client provided list of fields that define primary keys to access record. Field order defines hierarchical espresso keys. Empty lists indicates absence of primary key access patter. Value is a SchemaField@fieldPath."""
        self._inner_dict['primaryKeys'] = value
    
    
    @property
    def foreignKeysSpecs(self) -> Union[None, Dict[str, "ForeignKeySpecClass"]]:
        """Getter: Map captures all the references schema makes to external datasets. Map key is ForeignKeySpecName typeref."""
        return self._inner_dict.get('foreignKeysSpecs')  # type: ignore
    
    
    @foreignKeysSpecs.setter
    def foreignKeysSpecs(self, value: Union[None, Dict[str, "ForeignKeySpecClass"]]):
        """Setter: Map captures all the references schema makes to external datasets. Map key is ForeignKeySpecName typeref."""
        self._inner_dict['foreignKeysSpecs'] = value
    
    
class SchemalessClass(DictWrapper):
    """The dataset has no specific schema associated with it"""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.Schemaless")
    
    @overload
    def __init__(self,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(SchemalessClass, self).__init__({})
        pass
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
class StringTypeClass(DictWrapper):
    """String field type."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.StringType")
    
    @overload
    def __init__(self,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(StringTypeClass, self).__init__({})
        pass
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
class UnionTypeClass(DictWrapper):
    """Union field type."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.UnionType")
    
    @overload
    def __init__(self,
        nestedTypes: Optional[Union[None, List[str]]]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(UnionTypeClass, self).__init__({})
        self.nestedTypes = self.RECORD_SCHEMA.field_map["nestedTypes"].default
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def nestedTypes(self) -> Union[None, List[str]]:
        """Getter: List of types in union type."""
        return self._inner_dict.get('nestedTypes')  # type: ignore
    
    
    @nestedTypes.setter
    def nestedTypes(self, value: Union[None, List[str]]):
        """Setter: List of types in union type."""
        self._inner_dict['nestedTypes'] = value
    
    
class UrnForeignKeyClass(DictWrapper):
    """If SchemaMetadata fields make any external references and references are of type com.linkeidn.common.Urn or any children, this models can be used to mark it."""
    
    RECORD_SCHEMA = get_schema_type("com.linkedin.pegasus2avro.schema.UrnForeignKey")
    
    @overload
    def __init__(self,
        currentFieldPath: Optional[str]=None,
    ):
        # Note that the defaults here are not necessarily None.
        ...
    @overload
    def __init__(self, _inner_dict: Optional[dict]=None):
        ...
    
    def __init__(self, _inner_dict=None, **kwargs):
        super(UrnForeignKeyClass, self).__init__({})
        self.currentFieldPath = str()
        if _inner_dict is not None:
            for key, value in _inner_dict.items():
                getattr(self, key)
                setattr(self, key, value)
        for key, value in kwargs.items():
            if value is not None:
                getattr(self, key)
                setattr(self, key, value)
    
    
    @property
    def currentFieldPath(self) -> str:
        """Getter: Field in hosting(current) SchemaMetadata."""
        return self._inner_dict.get('currentFieldPath')  # type: ignore
    
    
    @currentFieldPath.setter
    def currentFieldPath(self, value: str):
        """Setter: Field in hosting(current) SchemaMetadata."""
        self._inner_dict['currentFieldPath'] = value
    
    
__SCHEMA_TYPES = {
    'com.linkedin.events.KafkaAuditHeader': KafkaAuditHeaderClass,
    'com.linkedin.pegasus2avro.chart.ChartInfo': ChartInfoClass,
    'com.linkedin.pegasus2avro.chart.ChartQuery': ChartQueryClass,
    'com.linkedin.pegasus2avro.chart.ChartQueryType': ChartQueryTypeClass,
    'com.linkedin.pegasus2avro.chart.ChartType': ChartTypeClass,
    'com.linkedin.pegasus2avro.common.AccessLevel': AccessLevelClass,
    'com.linkedin.pegasus2avro.common.AuditStamp': AuditStampClass,
    'com.linkedin.pegasus2avro.common.ChangeAuditStamps': ChangeAuditStampsClass,
    'com.linkedin.pegasus2avro.common.Cost': CostClass,
    'com.linkedin.pegasus2avro.common.CostCost': CostCostClass,
    'com.linkedin.pegasus2avro.common.CostCostDiscriminator': CostCostDiscriminatorClass,
    'com.linkedin.pegasus2avro.common.CostType': CostTypeClass,
    'com.linkedin.pegasus2avro.common.Deprecation': DeprecationClass,
    'com.linkedin.pegasus2avro.common.InstitutionalMemory': InstitutionalMemoryClass,
    'com.linkedin.pegasus2avro.common.InstitutionalMemoryMetadata': InstitutionalMemoryMetadataClass,
    'com.linkedin.pegasus2avro.common.MLFeatureDataType': MLFeatureDataTypeClass,
    'com.linkedin.pegasus2avro.common.Owner': OwnerClass,
    'com.linkedin.pegasus2avro.common.Ownership': OwnershipClass,
    'com.linkedin.pegasus2avro.common.OwnershipSource': OwnershipSourceClass,
    'com.linkedin.pegasus2avro.common.OwnershipSourceType': OwnershipSourceTypeClass,
    'com.linkedin.pegasus2avro.common.OwnershipType': OwnershipTypeClass,
    'com.linkedin.pegasus2avro.common.Status': StatusClass,
    'com.linkedin.pegasus2avro.common.VersionTag': VersionTagClass,
    'com.linkedin.pegasus2avro.common.fieldtransformer.TransformationType': TransformationTypeClass,
    'com.linkedin.pegasus2avro.common.fieldtransformer.UDFTransformer': UDFTransformerClass,
    'com.linkedin.pegasus2avro.dashboard.DashboardInfo': DashboardInfoClass,
    'com.linkedin.pegasus2avro.dataprocess.DataProcessInfo': DataProcessInfoClass,
    'com.linkedin.pegasus2avro.dataset.DatasetDeprecation': DatasetDeprecationClass,
    'com.linkedin.pegasus2avro.dataset.DatasetFieldMapping': DatasetFieldMappingClass,
    'com.linkedin.pegasus2avro.dataset.DatasetLineageType': DatasetLineageTypeClass,
    'com.linkedin.pegasus2avro.dataset.DatasetProperties': DatasetPropertiesClass,
    'com.linkedin.pegasus2avro.dataset.DatasetUpstreamLineage': DatasetUpstreamLineageClass,
    'com.linkedin.pegasus2avro.dataset.Upstream': UpstreamClass,
    'com.linkedin.pegasus2avro.dataset.UpstreamLineage': UpstreamLineageClass,
    'com.linkedin.pegasus2avro.identity.CorpGroupInfo': CorpGroupInfoClass,
    'com.linkedin.pegasus2avro.identity.CorpUserEditableInfo': CorpUserEditableInfoClass,
    'com.linkedin.pegasus2avro.identity.CorpUserInfo': CorpUserInfoClass,
    'com.linkedin.pegasus2avro.metadata.snapshot.ChartSnapshot': ChartSnapshotClass,
    'com.linkedin.pegasus2avro.metadata.snapshot.CorpGroupSnapshot': CorpGroupSnapshotClass,
    'com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot': CorpUserSnapshotClass,
    'com.linkedin.pegasus2avro.metadata.snapshot.DashboardSnapshot': DashboardSnapshotClass,
    'com.linkedin.pegasus2avro.metadata.snapshot.DataProcessSnapshot': DataProcessSnapshotClass,
    'com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot': DatasetSnapshotClass,
    'com.linkedin.pegasus2avro.metadata.snapshot.MLFeatureSnapshot': MLFeatureSnapshotClass,
    'com.linkedin.pegasus2avro.metadata.snapshot.MLModelSnapshot': MLModelSnapshotClass,
    'com.linkedin.pegasus2avro.ml.metadata.BaseData': BaseDataClass,
    'com.linkedin.pegasus2avro.ml.metadata.CaveatDetails': CaveatDetailsClass,
    'com.linkedin.pegasus2avro.ml.metadata.CaveatsAndRecommendations': CaveatsAndRecommendationsClass,
    'com.linkedin.pegasus2avro.ml.metadata.EthicalConsiderations': EthicalConsiderationsClass,
    'com.linkedin.pegasus2avro.ml.metadata.EvaluationData': EvaluationDataClass,
    'com.linkedin.pegasus2avro.ml.metadata.IntendedUse': IntendedUseClass,
    'com.linkedin.pegasus2avro.ml.metadata.IntendedUserType': IntendedUserTypeClass,
    'com.linkedin.pegasus2avro.ml.metadata.MLFeatureProperties': MLFeaturePropertiesClass,
    'com.linkedin.pegasus2avro.ml.metadata.MLModelFactorPrompts': MLModelFactorPromptsClass,
    'com.linkedin.pegasus2avro.ml.metadata.MLModelFactors': MLModelFactorsClass,
    'com.linkedin.pegasus2avro.ml.metadata.MLModelProperties': MLModelPropertiesClass,
    'com.linkedin.pegasus2avro.ml.metadata.Metrics': MetricsClass,
    'com.linkedin.pegasus2avro.ml.metadata.QuantitativeAnalyses': QuantitativeAnalysesClass,
    'com.linkedin.pegasus2avro.ml.metadata.SourceCode': SourceCodeClass,
    'com.linkedin.pegasus2avro.ml.metadata.SourceCodeUrl': SourceCodeUrlClass,
    'com.linkedin.pegasus2avro.ml.metadata.SourceCodeUrlType': SourceCodeUrlTypeClass,
    'com.linkedin.pegasus2avro.ml.metadata.TrainingData': TrainingDataClass,
    'com.linkedin.pegasus2avro.mxe.MetadataChangeEvent': MetadataChangeEventClass,
    'com.linkedin.pegasus2avro.schema.ArrayType': ArrayTypeClass,
    'com.linkedin.pegasus2avro.schema.BinaryJsonSchema': BinaryJsonSchemaClass,
    'com.linkedin.pegasus2avro.schema.BooleanType': BooleanTypeClass,
    'com.linkedin.pegasus2avro.schema.BytesType': BytesTypeClass,
    'com.linkedin.pegasus2avro.schema.DatasetFieldForeignKey': DatasetFieldForeignKeyClass,
    'com.linkedin.pegasus2avro.schema.EnumType': EnumTypeClass,
    'com.linkedin.pegasus2avro.schema.EspressoSchema': EspressoSchemaClass,
    'com.linkedin.pegasus2avro.schema.FixedType': FixedTypeClass,
    'com.linkedin.pegasus2avro.schema.ForeignKeySpec': ForeignKeySpecClass,
    'com.linkedin.pegasus2avro.schema.KafkaSchema': KafkaSchemaClass,
    'com.linkedin.pegasus2avro.schema.KeyValueSchema': KeyValueSchemaClass,
    'com.linkedin.pegasus2avro.schema.MapType': MapTypeClass,
    'com.linkedin.pegasus2avro.schema.MySqlDDL': MySqlDDLClass,
    'com.linkedin.pegasus2avro.schema.NullType': NullTypeClass,
    'com.linkedin.pegasus2avro.schema.NumberType': NumberTypeClass,
    'com.linkedin.pegasus2avro.schema.OracleDDL': OracleDDLClass,
    'com.linkedin.pegasus2avro.schema.OrcSchema': OrcSchemaClass,
    'com.linkedin.pegasus2avro.schema.OtherSchema': OtherSchemaClass,
    'com.linkedin.pegasus2avro.schema.PrestoDDL': PrestoDDLClass,
    'com.linkedin.pegasus2avro.schema.RecordType': RecordTypeClass,
    'com.linkedin.pegasus2avro.schema.SchemaField': SchemaFieldClass,
    'com.linkedin.pegasus2avro.schema.SchemaFieldDataType': SchemaFieldDataTypeClass,
    'com.linkedin.pegasus2avro.schema.SchemaMetadata': SchemaMetadataClass,
    'com.linkedin.pegasus2avro.schema.Schemaless': SchemalessClass,
    'com.linkedin.pegasus2avro.schema.StringType': StringTypeClass,
    'com.linkedin.pegasus2avro.schema.UnionType': UnionTypeClass,
    'com.linkedin.pegasus2avro.schema.UrnForeignKey': UrnForeignKeyClass,
    'KafkaAuditHeader': KafkaAuditHeaderClass,
    'ChartInfo': ChartInfoClass,
    'ChartQuery': ChartQueryClass,
    'ChartQueryType': ChartQueryTypeClass,
    'ChartType': ChartTypeClass,
    'AccessLevel': AccessLevelClass,
    'AuditStamp': AuditStampClass,
    'ChangeAuditStamps': ChangeAuditStampsClass,
    'Cost': CostClass,
    'CostCost': CostCostClass,
    'CostCostDiscriminator': CostCostDiscriminatorClass,
    'CostType': CostTypeClass,
    'Deprecation': DeprecationClass,
    'InstitutionalMemory': InstitutionalMemoryClass,
    'InstitutionalMemoryMetadata': InstitutionalMemoryMetadataClass,
    'MLFeatureDataType': MLFeatureDataTypeClass,
    'Owner': OwnerClass,
    'Ownership': OwnershipClass,
    'OwnershipSource': OwnershipSourceClass,
    'OwnershipSourceType': OwnershipSourceTypeClass,
    'OwnershipType': OwnershipTypeClass,
    'Status': StatusClass,
    'VersionTag': VersionTagClass,
    'TransformationType': TransformationTypeClass,
    'UDFTransformer': UDFTransformerClass,
    'DashboardInfo': DashboardInfoClass,
    'DataProcessInfo': DataProcessInfoClass,
    'DatasetDeprecation': DatasetDeprecationClass,
    'DatasetFieldMapping': DatasetFieldMappingClass,
    'DatasetLineageType': DatasetLineageTypeClass,
    'DatasetProperties': DatasetPropertiesClass,
    'DatasetUpstreamLineage': DatasetUpstreamLineageClass,
    'Upstream': UpstreamClass,
    'UpstreamLineage': UpstreamLineageClass,
    'CorpGroupInfo': CorpGroupInfoClass,
    'CorpUserEditableInfo': CorpUserEditableInfoClass,
    'CorpUserInfo': CorpUserInfoClass,
    'ChartSnapshot': ChartSnapshotClass,
    'CorpGroupSnapshot': CorpGroupSnapshotClass,
    'CorpUserSnapshot': CorpUserSnapshotClass,
    'DashboardSnapshot': DashboardSnapshotClass,
    'DataProcessSnapshot': DataProcessSnapshotClass,
    'DatasetSnapshot': DatasetSnapshotClass,
    'MLFeatureSnapshot': MLFeatureSnapshotClass,
    'MLModelSnapshot': MLModelSnapshotClass,
    'BaseData': BaseDataClass,
    'CaveatDetails': CaveatDetailsClass,
    'CaveatsAndRecommendations': CaveatsAndRecommendationsClass,
    'EthicalConsiderations': EthicalConsiderationsClass,
    'EvaluationData': EvaluationDataClass,
    'IntendedUse': IntendedUseClass,
    'IntendedUserType': IntendedUserTypeClass,
    'MLFeatureProperties': MLFeaturePropertiesClass,
    'MLModelFactorPrompts': MLModelFactorPromptsClass,
    'MLModelFactors': MLModelFactorsClass,
    'MLModelProperties': MLModelPropertiesClass,
    'Metrics': MetricsClass,
    'QuantitativeAnalyses': QuantitativeAnalysesClass,
    'SourceCode': SourceCodeClass,
    'SourceCodeUrl': SourceCodeUrlClass,
    'SourceCodeUrlType': SourceCodeUrlTypeClass,
    'TrainingData': TrainingDataClass,
    'MetadataChangeEvent': MetadataChangeEventClass,
    'ArrayType': ArrayTypeClass,
    'BinaryJsonSchema': BinaryJsonSchemaClass,
    'BooleanType': BooleanTypeClass,
    'BytesType': BytesTypeClass,
    'DatasetFieldForeignKey': DatasetFieldForeignKeyClass,
    'EnumType': EnumTypeClass,
    'EspressoSchema': EspressoSchemaClass,
    'FixedType': FixedTypeClass,
    'ForeignKeySpec': ForeignKeySpecClass,
    'KafkaSchema': KafkaSchemaClass,
    'KeyValueSchema': KeyValueSchemaClass,
    'MapType': MapTypeClass,
    'MySqlDDL': MySqlDDLClass,
    'NullType': NullTypeClass,
    'NumberType': NumberTypeClass,
    'OracleDDL': OracleDDLClass,
    'OrcSchema': OrcSchemaClass,
    'OtherSchema': OtherSchemaClass,
    'PrestoDDL': PrestoDDLClass,
    'RecordType': RecordTypeClass,
    'SchemaField': SchemaFieldClass,
    'SchemaFieldDataType': SchemaFieldDataTypeClass,
    'SchemaMetadata': SchemaMetadataClass,
    'Schemaless': SchemalessClass,
    'StringType': StringTypeClass,
    'UnionType': UnionTypeClass,
    'UrnForeignKey': UrnForeignKeyClass,
}

_json_converter = avrojson.AvroJsonConverter(use_logical_types=False, schema_types=__SCHEMA_TYPES)

