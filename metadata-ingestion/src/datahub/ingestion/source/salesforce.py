import json
import logging
import time
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional

import requests
from pydantic import Field, validator
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceAuthenticationFailed

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    ConfigurationError,
)
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_domain_to_entity_wu
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source_config.operation_config import (
    OperationConfig,
    is_profiling_enabled,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BooleanTypeClass,
    BytesTypeClass,
    DataPlatformInstanceClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DateTypeClass,
    EnumTypeClass,
    ForeignKeyConstraintClass,
    GlobalTagsClass,
    NullTypeClass,
    NumberTypeClass,
    OperationClass,
    OperationTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
)
from datahub.utilities import config_clean
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)


class SalesforceAuthType(Enum):
    USERNAME_PASSWORD = "USERNAME_PASSWORD"
    DIRECT_ACCESS_TOKEN = "DIRECT_ACCESS_TOKEN"
    JSON_WEB_TOKEN = "JSON_WEB_TOKEN"


class SalesforceProfilingConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description="Whether profiling should be done. Supports only table-level profiling at this stage",
    )
    operation_config: OperationConfig = Field(
        default_factory=OperationConfig,
        description="Experimental feature. To specify operation configs.",
    )

    # TODO - support field level profiling


class SalesforceConfig(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
):
    platform: str = "salesforce"

    auth: SalesforceAuthType = SalesforceAuthType.USERNAME_PASSWORD

    # Username, Password Auth
    username: Optional[str] = Field(description="Salesforce username")
    password: Optional[str] = Field(description="Password for Salesforce user")
    consumer_key: Optional[str] = Field(
        description="Consumer key for Salesforce JSON web token access"
    )
    private_key: Optional[str] = Field(
        description="Private key as a string for Salesforce JSON web token access"
    )
    security_token: Optional[str] = Field(
        description="Security token for Salesforce username"
    )
    # client_id, client_secret not required

    # Direct - Instance URL, Access Token Auth
    instance_url: Optional[str] = Field(
        description="Salesforce instance url. e.g. https://MyDomainName.my.salesforce.com"
    )
    # Flag to indicate whether the instance is production or sandbox
    is_sandbox: bool = Field(
        default=False, description="Connect to Sandbox instance of your Salesforce"
    )
    access_token: Optional[str] = Field(description="Access token for instance url")

    ingest_tags: Optional[bool] = Field(
        default=False,
        description="Ingest Tags from source. This will override Tags entered from UI",
    )

    object_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for Salesforce objects to filter in ingestion.",
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description='Regex patterns for tables/schemas to describe domain_key domain key (domain_key can be any string like "sales".) There can be multiple domain keys specified.',
    )
    api_version: Optional[str] = Field(
        description="If specified, overrides default version used by the Salesforce package. Example value: '59.0'"
    )

    profiling: SalesforceProfilingConfig = SalesforceProfilingConfig()

    profile_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for profiles to filter in ingestion, allowed by the `object_pattern`.",
    )

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    @validator("instance_url")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)


@dataclass
class SalesforceSourceReport(StaleEntityRemovalSourceReport):
    filtered: LossyList[str] = dataclass_field(default_factory=LossyList)

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)


# https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_calls_describesObjects_describesObjectresult.htm#FieldType
FIELD_TYPE_MAPPING = {
    "string": StringTypeClass,
    "boolean": BooleanTypeClass,
    "int": NumberTypeClass,
    "integer": NumberTypeClass,
    "long": NumberTypeClass,
    "double": NumberTypeClass,
    "date": DateTypeClass,
    "datetime": DateTypeClass,
    "time": DateTypeClass,
    "id": StringTypeClass,  # Primary Key
    "picklist": EnumTypeClass,
    "address": RecordTypeClass,
    "location": RecordTypeClass,
    "reference": StringTypeClass,  # Foreign Key
    "currency": NumberTypeClass,
    "textarea": StringTypeClass,
    "percent": NumberTypeClass,
    "phone": StringTypeClass,
    "url": StringTypeClass,
    "email": StringTypeClass,
    "combobox": StringTypeClass,
    "multipicklist": StringTypeClass,
    "base64": BytesTypeClass,
    "anyType": NullTypeClass,
    "encryptedstring": StringTypeClass,
}


@platform_name("Salesforce")
@config_class(SalesforceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    capability_name=SourceCapability.PLATFORM_INSTANCE,
    description="Can be equivalent to Salesforce organization",
)
@capability(
    capability_name=SourceCapability.DOMAINS,
    description="Supported via the `domain` config field",
)
@capability(
    capability_name=SourceCapability.DATA_PROFILING,
    description="Only table level profiling is supported via `profiling.enabled` config field",
)
@capability(
    capability_name=SourceCapability.DELETION_DETECTION,
    description="Not supported yet",
    supported=False,
)
@capability(
    capability_name=SourceCapability.SCHEMA_METADATA,
    description="Enabled by default",
)
@capability(
    capability_name=SourceCapability.TAGS,
    description="Enabled by default",
)
class SalesforceSource(StatefulIngestionSourceBase):
    base_url: str
    config: SalesforceConfig
    report: SalesforceSourceReport
    session: requests.Session
    sf: Salesforce
    fieldCounts: Dict[str, int]

    def __init__(self, config: SalesforceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.ctx = ctx
        self.config = config
        self.report = SalesforceSourceReport()
        self.session = requests.Session()
        self.platform: str = "salesforce"
        self.fieldCounts = {}
        common_args: Dict[str, Any] = {
            "domain": "test" if self.config.is_sandbox else None,
            "session": self.session,
        }
        if self.config.api_version:
            common_args["version"] = self.config.api_version

        try:
            if self.config.auth is SalesforceAuthType.DIRECT_ACCESS_TOKEN:
                logger.debug("Access Token Provided in Config")
                assert self.config.access_token is not None, (
                    "Config access_token is required for DIRECT_ACCESS_TOKEN auth"
                )
                assert self.config.instance_url is not None, (
                    "Config instance_url is required for DIRECT_ACCESS_TOKEN auth"
                )

                self.sf = Salesforce(
                    instance_url=self.config.instance_url,
                    session_id=self.config.access_token,
                    **common_args,
                )
            elif self.config.auth is SalesforceAuthType.USERNAME_PASSWORD:
                logger.debug("Username/Password Provided in Config")
                assert self.config.username is not None, (
                    "Config username is required for USERNAME_PASSWORD auth"
                )
                assert self.config.password is not None, (
                    "Config password is required for USERNAME_PASSWORD auth"
                )
                assert self.config.security_token is not None, (
                    "Config security_token is required for USERNAME_PASSWORD auth"
                )

                self.sf = Salesforce(
                    username=self.config.username,
                    password=self.config.password,
                    security_token=self.config.security_token,
                    **common_args,
                )

            elif self.config.auth is SalesforceAuthType.JSON_WEB_TOKEN:
                logger.debug("Json Web Token provided in the config")
                assert self.config.username is not None, (
                    "Config username is required for JSON_WEB_TOKEN auth"
                )
                assert self.config.consumer_key is not None, (
                    "Config consumer_key is required for JSON_WEB_TOKEN auth"
                )
                assert self.config.private_key is not None, (
                    "Config private_key is required for JSON_WEB_TOKEN auth"
                )

                self.sf = Salesforce(
                    username=self.config.username,
                    consumer_key=self.config.consumer_key,
                    privatekey=self.config.private_key,
                    **common_args,
                )

        except SalesforceAuthenticationFailed as e:
            logger.error(e)
            if "API_CURRENTLY_DISABLED" in str(e):
                # https://help.salesforce.com/s/articleView?id=001473830&type=1
                error = "Salesforce login failed. Please make sure user has API Enabled Access."
            else:
                error = "Salesforce login failed. Please verify your credentials."
                if (
                    self.config.instance_url
                    and "sandbox" in self.config.instance_url.lower()
                ):
                    error += "Please set `is_sandbox: True` in recipe if this is sandbox account."
            raise ConfigurationError(error) from e

        if not self.config.api_version:
            # List all REST API versions and use latest one
            versions_url = "https://{instance}/services/data/".format(
                instance=self.sf.sf_instance,
            )
            versions_response = self.sf._call_salesforce("GET", versions_url).json()
            latest_version = versions_response[-1]
            version = latest_version["version"]
            # we could avoid setting the version like below (after the Salesforce object has been already initiated
            # above), since, according to the docs:
            # https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_versions.htm
            # we don't need to be authenticated to list the versions (so we could perform this call before even
            # authenticating)
            self.sf.sf_version = version

        self.base_url = "https://{instance}/services/data/v{sf_version}/".format(
            instance=self.sf.sf_instance, sf_version=self.sf.sf_version
        )

        logger.debug(
            "Using Salesforce REST API version: {version}".format(
                version=self.sf.sf_version
            )
        )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        try:
            sObjects = self.get_salesforce_objects()
        except Exception as e:
            if "sObject type 'EntityDefinition' is not supported." in str(e):
                # https://developer.salesforce.com/docs/atlas.en-us.api_tooling.meta/api_tooling/tooling_api_objects_entitydefinition.htm
                raise ConfigurationError(
                    "Salesforce EntityDefinition query failed. "
                    "Please verify if user has 'View Setup and Configuration' permission."
                ) from e
            raise e
        else:
            for sObject in sObjects:
                yield from self.get_salesforce_object_workunits(sObject)

    def get_salesforce_object_workunits(
        self, sObject: dict
    ) -> Iterable[MetadataWorkUnit]:
        sObjectName = sObject["QualifiedApiName"]

        if not self.config.object_pattern.allowed(sObjectName):
            self.report.report_dropped(sObjectName)
            logger.debug(
                "Skipping {sObject}, as it is not allowed by object_pattern".format(
                    sObject=sObjectName
                )
            )
            return

        datasetUrn = builder.make_dataset_urn_with_platform_instance(
            self.platform,
            sObjectName,
            self.config.platform_instance,
            self.config.env,
        )

        customObject = {}
        if sObjectName.endswith("__c"):  # Is Custom Object
            customObject = self.get_custom_object_details(sObject["DeveloperName"])

            # Table Created, LastModified is available for Custom Object
            yield from self.get_operation_workunit(customObject, datasetUrn)

        yield self.get_properties_workunit(sObject, customObject, datasetUrn)

        yield from self.get_schema_metadata_workunit(
            sObjectName, sObject, customObject, datasetUrn
        )

        yield self.get_subtypes_workunit(sObjectName, datasetUrn)

        if self.config.platform_instance is not None:
            yield self.get_platform_instance_workunit(datasetUrn)

        if self.config.domain is not None:
            yield from self.get_domain_workunit(sObjectName, datasetUrn)

        if self.config.is_profiling_enabled() and self.config.profile_pattern.allowed(
            sObjectName
        ):
            yield from self.get_profile_workunit(sObjectName, datasetUrn)

    def get_custom_object_details(self, sObjectDeveloperName: str) -> dict:
        customObject = {}
        query_url = (
            self.base_url
            + "tooling/query/?q=SELECT Description, Language, ManageableState, "
            + "CreatedDate, CreatedBy.Username, LastModifiedDate, LastModifiedBy.Username "
            + f"FROM CustomObject where DeveloperName='{sObjectDeveloperName}'"
        )
        custom_objects_response = self.sf._call_salesforce("GET", query_url).json()
        if len(custom_objects_response["records"]) > 0:
            logger.debug("Salesforce CustomObject query returned with details")
            customObject = custom_objects_response["records"][0]
        return customObject

    def get_salesforce_objects(self) -> List:
        # Using Describe Global REST API returns many more objects than required.
        # Response does not have the attribute ("customizable") that can be used
        # to filter out entities not on ObjectManager UI. Hence SOQL on EntityDefinition
        # object is used instead, as suggested by salesforce support.

        query_url = (
            self.base_url
            + "tooling/query/?q=SELECT DurableId,QualifiedApiName,DeveloperName,"
            + "Label,PluralLabel,InternalSharingModel,ExternalSharingModel,DeploymentStatus "
            + "FROM EntityDefinition WHERE IsCustomizable = true"
        )
        entities_response = self.sf._call_salesforce("GET", query_url).json()
        logger.debug(
            "Salesforce EntityDefinition query returned {count} sObjects".format(
                count=len(entities_response["records"])
            )
        )
        return entities_response["records"]

    def get_domain_workunit(
        self, dataset_name: str, datasetUrn: str
    ) -> Iterable[MetadataWorkUnit]:
        domain_urn: Optional[str] = None

        for domain, pattern in self.config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = builder.make_domain_urn(domain)

        if domain_urn:
            yield from add_domain_to_entity_wu(
                domain_urn=domain_urn, entity_urn=datasetUrn
            )

    def get_platform_instance_workunit(self, datasetUrn: str) -> MetadataWorkUnit:
        dataPlatformInstance = DataPlatformInstanceClass(
            builder.make_data_platform_urn(self.platform),
            instance=builder.make_dataplatform_instance_urn(
                self.platform,
                self.config.platform_instance,  # type:ignore
            ),
        )

        return MetadataChangeProposalWrapper(
            entityUrn=datasetUrn, aspect=dataPlatformInstance
        ).as_workunit()

    def get_operation_workunit(
        self, customObject: dict, datasetUrn: str
    ) -> Iterable[MetadataWorkUnit]:
        reported_time: int = int(time.time() * 1000)

        if customObject.get("CreatedBy") and customObject.get("CreatedDate"):
            timestamp = self.get_time_from_salesforce_timestamp(
                customObject["CreatedDate"]
            )
            operation = OperationClass(
                timestampMillis=reported_time,
                operationType=OperationTypeClass.CREATE,
                lastUpdatedTimestamp=timestamp,
                actor=builder.make_user_urn(customObject["CreatedBy"]["Username"]),
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=datasetUrn, aspect=operation
            ).as_workunit()

            # Note - Object Level LastModified captures changes at table level metadata e.g. table
            # description and does NOT capture field level metadata e.g. new field added, existing
            # field updated

            if customObject.get("LastModifiedBy") and customObject.get(
                "LastModifiedDate"
            ):
                timestamp = self.get_time_from_salesforce_timestamp(
                    customObject["LastModifiedDate"]
                )
                operation = OperationClass(
                    timestampMillis=reported_time,
                    operationType=OperationTypeClass.ALTER,
                    lastUpdatedTimestamp=timestamp,
                    actor=builder.make_user_urn(
                        customObject["LastModifiedBy"]["Username"]
                    ),
                )
                yield MetadataChangeProposalWrapper(
                    entityUrn=datasetUrn, aspect=operation
                ).as_workunit()

    def get_time_from_salesforce_timestamp(self, date: str) -> int:
        return round(
            datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp() * 1000
        )

    def get_properties_workunit(
        self, sObject: dict, customObject: Dict[str, str], datasetUrn: str
    ) -> MetadataWorkUnit:
        propertyLabels = {
            # from EntityDefinition
            "DurableId": "Durable Id",
            "DeveloperName": "Developer Name",
            "QualifiedApiName": "Qualified API Name",
            "Label": "Label",
            "PluralLabel": "Plural Label",
            "InternalSharingModel": "Internal Sharing Model",
            "ExternalSharingModel": "External Sharing Model",
            # from CustomObject
            "ManageableState": "Manageable State",
            "Language": "Language",
        }

        sObjectProperties = {
            propertyLabels[k]: str(v)
            for k, v in sObject.items()
            if k in propertyLabels and v is not None
        }
        sObjectProperties.update(
            {
                propertyLabels[k]: str(v)
                for k, v in customObject.items()
                if k in propertyLabels and v is not None
            }
        )

        datasetProperties = DatasetPropertiesClass(
            name=sObject["Label"],
            description=customObject.get("Description"),
            customProperties=sObjectProperties,
        )
        return MetadataChangeProposalWrapper(
            entityUrn=datasetUrn, aspect=datasetProperties
        ).as_workunit()

    def get_subtypes_workunit(
        self, sObjectName: str, datasetUrn: str
    ) -> MetadataWorkUnit:
        subtypes: List[str] = []
        if sObjectName.endswith("__c"):
            subtypes.append(DatasetSubTypes.SALESFORCE_CUSTOM_OBJECT)
        else:
            subtypes.append(DatasetSubTypes.SALESFORCE_STANDARD_OBJECT)

        return MetadataChangeProposalWrapper(
            entityUrn=datasetUrn, aspect=SubTypesClass(typeNames=subtypes)
        ).as_workunit()

    def get_profile_workunit(
        self, sObjectName: str, datasetUrn: str
    ) -> Iterable[MetadataWorkUnit]:
        # Here approximate record counts as returned by recordCount API are used as rowCount
        # In future, count() SOQL query may be used instead, if required, might be more expensive
        sObject_records_count_url = (
            f"{self.base_url}limits/recordCount?sObjects={sObjectName}"
        )

        sObject_record_count_response = self.sf._call_salesforce(
            "GET", sObject_records_count_url
        ).json()

        logger.debug(
            "Received Salesforce {sObject} record count response".format(
                sObject=sObjectName
            )
        )

        for entry in sObject_record_count_response.get("sObjects", []):
            datasetProfile = DatasetProfileClass(
                timestampMillis=int(time.time() * 1000),
                rowCount=entry["count"],
                columnCount=self.fieldCounts[sObjectName],
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=datasetUrn, aspect=datasetProfile
            ).as_workunit()

    # Here field description is created from label, description and inlineHelpText
    def _get_field_description(self, field: dict, customField: dict) -> str:
        if "Label" not in field or field["Label"] is None:
            desc = ""
        elif field["Label"].startswith("#"):
            desc = "\\" + field["Label"]
        else:
            desc = field["Label"]

        text = field.get("FieldDefinition", {}).get("Description", None)
        if text:
            prefix = "\\" if text.startswith("#") else ""
            desc += f"\n\n{prefix}{text}"

        text = field.get("InlineHelpText", None)
        if text:
            prefix = "\\" if text.startswith("#") else ""
            desc += f"\n\n{prefix}{text}"

        return desc

    # Here jsonProps is used to add additional salesforce field level properties.
    def _get_field_json_props(self, field: dict, customField: dict) -> str:
        jsonProps = {}

        if field.get("IsUnique"):
            jsonProps["IsUnique"] = True

        return json.dumps(jsonProps)

    def _get_schema_field(
        self,
        sObjectName: str,
        fieldName: str,
        fieldType: str,
        field: dict,
        customField: dict,
    ) -> SchemaFieldClass:
        fieldPath = fieldName

        TypeClass = FIELD_TYPE_MAPPING.get(fieldType)
        if TypeClass is None:
            self.report.warning(
                message="Unable to map field type to metadata schema",
                context=f"{fieldType} for {fieldName} of {sObjectName}",
            )
            TypeClass = NullTypeClass

        fieldTags: List[str] = self.get_field_tags(fieldName, field)

        description = self._get_field_description(field, customField)

        schemaField = SchemaFieldClass(
            fieldPath=fieldPath,
            type=SchemaFieldDataTypeClass(type=TypeClass()),  # type:ignore
            description=description,
            # nativeDataType is set to data type shown on salesforce user interface,
            # not the corresponding API data type names.
            nativeDataType=field["FieldDefinition"]["DataType"],
            nullable=field["IsNillable"],
            globalTags=get_tags(fieldTags) if self.config.ingest_tags else None,
            jsonProps=self._get_field_json_props(field, customField),
        )

        # Created and LastModified Date and Actor are available for Custom Fields only
        if customField.get("CreatedDate") and customField.get("CreatedBy"):
            schemaField.created = self.get_audit_stamp(
                customField["CreatedDate"], customField["CreatedBy"]["Username"]
            )
        if customField.get("LastModifiedDate") and customField.get("LastModifiedBy"):
            schemaField.lastModified = self.get_audit_stamp(
                customField["LastModifiedDate"],
                customField["LastModifiedBy"]["Username"],
            )

        return schemaField

    def get_field_tags(self, fieldName: str, field: dict) -> List[str]:
        fieldTags: List[str] = []

        if fieldName.endswith("__c"):
            fieldTags.append("Custom")

        # https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/system_fields.htm
        sfSystemFields = [
            "Id",
            "IsDeleted",
            "CreatedById",
            "CreatedDate",
            "LastModifiedById",
            "LastModifiedDate",
            "SystemModstamp",
        ]

        if fieldName in sfSystemFields:
            fieldTags.append("SystemField")

        if field["FieldDefinition"]["ComplianceGroup"] is not None:
            # CCPA, COPPA, GDPR, HIPAA, PCI, PersonalInfo, PII
            fieldTags.extend(
                iter(field["FieldDefinition"]["ComplianceGroup"].split(";"))
            )
        return fieldTags

    def get_audit_stamp(self, date: str, username: str) -> AuditStampClass:
        return AuditStampClass(
            time=self.get_time_from_salesforce_timestamp(date),
            actor=builder.make_user_urn(username),
        )

    def get_schema_metadata_workunit(
        self, sObjectName: str, sObject: dict, customObject: dict, datasetUrn: str
    ) -> Iterable[MetadataWorkUnit]:
        sObject_fields_query_url = (
            self.base_url
            + "tooling/query?q=SELECT "
            + "QualifiedApiName,DeveloperName,Label, FieldDefinition.DataType, DataType,"
            + "FieldDefinition.LastModifiedDate, FieldDefinition.LastModifiedBy.Username,"
            + "Precision, Scale, Length, Digits ,FieldDefinition.IsIndexed, IsUnique,"
            + "IsCompound, IsComponent, ReferenceTo, FieldDefinition.ComplianceGroup,"
            + "RelationshipName, IsNillable, FieldDefinition.Description, InlineHelpText "
            + "FROM EntityParticle WHERE EntityDefinitionId='{}'".format(
                sObject["DurableId"]
            )
        )

        sObject_fields_response = self.sf._call_salesforce(
            "GET", sObject_fields_query_url
        ).json()

        logger.debug(f"Received Salesforce {sObjectName} fields response")

        sObject_custom_fields_query_url = (
            self.base_url
            + "tooling/query?q=SELECT "
            + "DeveloperName,CreatedDate,CreatedBy.Username,InlineHelpText,"
            + "LastModifiedDate,LastModifiedBy.Username "
            + "FROM CustomField WHERE EntityDefinitionId='{}'".format(
                sObject["DurableId"]
            )
        )

        customFields: Dict[str, Dict] = {}
        try:
            sObject_custom_fields_response = self.sf._call_salesforce(
                "GET", sObject_custom_fields_query_url
            ).json()

            logger.debug(
                "Received Salesforce {sObject} custom fields response".format(
                    sObject=sObjectName
                )
            )

        except Exception as e:
            error = "Salesforce CustomField query failed. "
            if "sObject type 'CustomField' is not supported." in str(e):
                # https://github.com/afawcett/apex-toolingapi/issues/19
                error += "Please verify if user has 'View All Data' permission."

            self.report.warning(message=error, exc=e)
        else:
            customFields = {
                record["DeveloperName"]: record
                for record in sObject_custom_fields_response["records"]
            }

        fields: List[SchemaFieldClass] = []
        primaryKeys: List[str] = []
        foreignKeys: List[ForeignKeyConstraintClass] = []

        for field in sObject_fields_response["records"]:
            customField = customFields.get(field["DeveloperName"], {})

            fieldName = field["QualifiedApiName"]
            fieldType = field["DataType"]

            # Skip compound fields. All Leaf fields are ingested instead.
            if fieldType in ("address", "location"):
                continue

            schemaField: SchemaFieldClass = self._get_schema_field(
                sObjectName, fieldName, fieldType, field, customField
            )
            fields.append(schemaField)

            if fieldType == "id":
                primaryKeys.append(fieldName)

            if (
                fieldType == "reference"
                and field["ReferenceTo"]["referenceTo"] is not None
            ):
                foreignKeys.extend(
                    list(self.get_foreign_keys_from_field(fieldName, field, datasetUrn))
                )

        schemaMetadata = SchemaMetadataClass(
            schemaName="",
            platform=builder.make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
            primaryKeys=primaryKeys,
            foreignKeys=foreignKeys or None,
        )

        # Created Date and Actor are available for Custom Object only
        if customObject.get("CreatedDate") and customObject.get("CreatedBy"):
            schemaMetadata.created = self.get_audit_stamp(
                customObject["CreatedDate"], customObject["CreatedBy"]["Username"]
            )
        self.fieldCounts[sObjectName] = len(fields)

        yield MetadataChangeProposalWrapper(
            entityUrn=datasetUrn, aspect=schemaMetadata
        ).as_workunit()

    def get_foreign_keys_from_field(
        self, fieldName: str, field: dict, datasetUrn: str
    ) -> Iterable[ForeignKeyConstraintClass]:
        # https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/field_types.htm#i1435823
        foreignDatasets = [
            builder.make_dataset_urn_with_platform_instance(
                self.platform,
                fsObject,
                self.config.platform_instance,
                self.config.env,
            )
            for fsObject in field["ReferenceTo"]["referenceTo"]
        ]

        for foreignDataset in foreignDatasets:
            yield ForeignKeyConstraintClass(
                name=field["RelationshipName"] if field.get("RelationshipName") else "",
                foreignDataset=foreignDataset,
                foreignFields=[builder.make_schema_field_urn(foreignDataset, "Id")],
                sourceFields=[builder.make_schema_field_urn(datasetUrn, fieldName)],
            )

    def get_report(self) -> SourceReport:
        return self.report


def get_tags(params: Optional[List[str]] = None) -> GlobalTagsClass:
    if params is None:
        params = []
    tags = [TagAssociationClass(tag=builder.make_tag_urn(tag)) for tag in params if tag]
    return GlobalTagsClass(tags=tags)
