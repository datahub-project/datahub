/**
 * The file contains a set of well-supported properties,
 * which the UI deeply understands.
 */
import {
    OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID,
    SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
    STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
} from '@app/tests/builder/steps/definition/builder/property/constants';
import { SelectInputMode, ValueTypeId } from '@app/tests/builder/steps/definition/builder/property/types/values';

import { EntityType } from '@types';

/**
 * A single well-supported property. Note that this is a "frontend"
 * property type. It is used for rendering the list of properties
 * in the property select portion of the Metadata Tests experience.
 */
export type Property = {
    id: string;
    displayName: string;
    description?: string;
    valueType?: ValueTypeId;
    valueOptions?: any;
    children?: Property[]; // Child Properties.
};

/**
 * Maps properties to their nested properties and types, which are required to render the correct
 * operator + predicate types. --> These all need to come from the server ideally in a one-time fetch.
 *
 * Example cases:
 *          "If an asset has an upstream that is tagged with sensitive, tag it with sensitive."
 *          "If an asset has > 10 upstreams, then mark it as important."
 *          "If an asset has no upstreams, then mark it as root."
 *          "Must have an owner of type Technical Owner"
 */
export const commonProps: Property[] = [
    {
        id: 'urn',
        displayName: 'Urn',
        description: 'The raw urn identifier of the asset.',
        valueType: ValueTypeId.STRING,
        valueOptions: {
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'entityType',
        displayName: 'Type',
        description: 'The type of the asset.',
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            mode: SelectInputMode.MULTIPLE,
            options: [
                {
                    id: 'dataset',
                    displayName: 'Dataset',
                },
                {
                    id: 'dashboard',
                    displayName: 'Dashboard',
                },
                {
                    id: 'chart',
                    displayName: 'Chart',
                },
                {
                    id: 'dataJob',
                    displayName: 'Data Job (Task)',
                },
                {
                    id: 'dataFlow',
                    displayName: 'Data Flow (Pipeline)',
                },
                {
                    id: 'container',
                    displayName: 'Container',
                },
                {
                    id: 'glossaryTerm',
                    displayName: 'Glossary Term',
                },
                {
                    id: 'glossaryNode',
                    displayName: 'Glossary Node',
                },
                {
                    id: 'domain',
                    displayName: 'Domain',
                },
                {
                    id: 'dataProduct',
                    displayName: 'Data Product',
                },
            ],
        },
    },
    {
        id: 'ownership.owners.owner',
        displayName: 'Owners',
        description: 'The owners of the asset.',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.CorpUser, EntityType.CorpGroup],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'deprecation.deprecated',
        displayName: 'Deprecated',
        description: 'Whether the asset is deprecated or not.',
        valueType: ValueTypeId.BOOLEAN,
    },
    {
        id: 'status.removed',
        displayName: 'Removed',
        description: 'Whether the asset has been removed/soft-deleted.',
        valueType: ValueTypeId.BOOLEAN,
    },
    {
        id: '__firstSynchronized',
        displayName: 'First Synchronized',
        description: 'The time at which the asset was first ingested into DataHub (ms).',
        valueType: ValueTypeId.TIMESTAMP,
    },
    {
        id: '__lastSynchronized',
        displayName: 'Last Synchronized',
        description: 'The time at which the asset was last ingested into DataHub (ms).',
        valueType: ValueTypeId.TIMESTAMP,
    },
    {
        id: '__lastObserved',
        displayName: 'Last Observed',
        description: 'The time at which the asset was last observed by DataHub (ms).',
        valueType: ValueTypeId.TIMESTAMP,
    },
];

/**
 * Properties that are common across most asset types but not necessarily all
 * (e.g., platform might not apply to glossaryTerm)
 */
export const assetProps: Property[] = [
    ...commonProps,
    {
        id: 'dataPlatformInstance.platform',
        displayName: 'Platform',
        description: 'The data platform where the asset lives.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.DataPlatform],
            mode: SelectInputMode.SINGLE,
        },
    },
    {
        id: 'globalTags.tags.tag',
        displayName: 'Tags',
        description: 'The tags attached to the asset.',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.Tag],
            mode: SelectInputMode.MULTIPLE,
        },
    },

    {
        id: 'subTypes.typeNames',
        displayName: 'Subtypes',
        description: 'The subtype(s) of the asset.',
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'ownership.owners.typeUrn',
        displayName: 'Ownership Types',
        description: 'The types of ownership assigned to the asset (e.g., Technical Owner, Business Owner).',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.CustomOwnershipType],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'glossaryTerms.terms.urn',
        displayName: 'Glossary Terms',
        description: 'The glossary terms attached to the asset.',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.GlossaryTerm],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'glossaryTerms.terms.urn.glossaryTermInfo.parentNode',
        displayName: 'Glossary Term Groups',
        description: 'The set of parent Term Groups for Glossary Terms attached to the asset.',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.GlossaryNode],
            mode: SelectInputMode.MULTIPLE,
        },
    },
];

/**
 * Properties that are specific to data assets (datasets, dashboards, charts, etc.)
 * but not applicable to other assets like glossaryTerm
 */
export const dataAssetProps: Property[] = [
    {
        id: 'browsePathsV2.path.urn',
        displayName: 'Browse Path container',
        description: 'A container in browse path of an asset.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Container],
            mode: SelectInputMode.SINGLE,
        },
    },
    {
        id: 'container.container',
        displayName: 'Container',
        description: 'The parent container of the asset.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Container],
            mode: SelectInputMode.SINGLE,
        },
    },
    {
        id: 'domains.domains',
        displayName: 'Domain',
        description: 'The domain that the asset is a part of.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Domain],
            mode: SelectInputMode.SINGLE,
        },
    },
];

const datasetProps: Property[] = [
    ...assetProps,
    ...dataAssetProps,

    // === CORE PROPERTIES ===
    // Basic identification and context
    {
        id: 'datasetProperties.name',
        displayName: 'Dataset Name',
        description: 'The name of the dataset as defined in the source data platform.',
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'datasetDescription',
        displayName: 'Description',
        children: [
            {
                id: 'editableDatasetProperties.description',
                displayName: 'DataHub Description',
                description: 'The description as authored inside DataHub directly.',
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'datasetProperties.description',
                displayName: 'Source Platform Description',
                description: 'The description as authored and ingested from the source data platform.',
                valueType: ValueTypeId.STRING,
            },
        ],
    },

    // === USAGE & IMPACT ===
    // For understanding adoption and importance
    {
        id: 'usageMetrics',
        displayName: 'Usage Metrics',
        children: [
            {
                id: 'usageFeatures.queryCountLast30Days',
                displayName: 'Query Count in Last 30 Days',
                description:
                    'The total query count in the past 30 days. This requires usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.uniqueUserCountLast30Days',
                displayName: 'Unique Users in the Last 30 Days',
                description:
                    'The unique user count in the past 30 days. This requires usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.queryCountPercentileLast30Days',
                displayName: 'Query Count Percentile in Last 30 Days (0-100)',
                description:
                    'The relative query count percentile for this dataset inside the data platform instance. This requires usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.writeCountLast30Days',
                displayName: 'Update Count in Last 30 Days',
                description:
                    'The total update/write count in the past 30 days. This requires usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.writeCountPercentileLast30Days',
                displayName: 'Update Count Percentile in Last 30 Days (0-100)',
                description:
                    'The relative update/write count percentile for this dataset within the data platform instance. This requires usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.uniqueUserPercentileLast30Days',
                displayName: 'Unique User Percentile in the Last 30 Days (0-100)',
                description:
                    'The relative unique user count percentile for this dataset within the data platform instance. This requires usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
        ],
    },

    // === LINEAGE ===
    // Critical for understanding data dependencies and impact analysis
    {
        id: 'upstreamLineage.upstreams',
        displayName: 'Upstream Assets',
        description: 'Assets that this dataset depends on (upstream in the data lineage).',
        valueType: ValueTypeId.EXISTS_LIST,
    },

    // === COLUMNS ===
    // Important for understanding data structure and content
    {
        id: 'schemaInformation',
        displayName: 'Columns',
        children: [
            {
                id: 'schemaFields',
                displayName: 'Column Names',
                description: 'The set of columns associated with the dataset.',
                valueType: ValueTypeId.SCHEMA_FIELD_LIST,
            },
            {
                id: 'schemaFields.length',
                displayName: 'Number of Columns',
                description: 'The number of columns associated with the dataset.',
                valueType: ValueTypeId.NUMBER,
            },
        ],
    },

    // === COLUMN-LEVEL METADATA ===
    // Detailed field-level information for granular rules
    {
        id: 'columnProperties',
        displayName: 'Column-Level Properties',
        children: [
            {
                id: 'schemaMetadata.fields.fieldPath',
                displayName: 'Column Names',
                description: 'The names (field paths) of individual columns in the dataset schema.',
                valueType: ValueTypeId.STRING_LIST,
            },
            {
                id: 'columnTags',
                displayName: 'Column Tags',
                children: [
                    {
                        id: 'editableSchemaMetadata.editableSchemaFieldInfo.globalTags',
                        displayName: 'DataHub Column Tags',
                        description: 'User-editable tags applied to individual columns/fields within DataHub.',
                        valueType: ValueTypeId.EXISTS_LIST,
                    },
                    {
                        id: 'schemaMetadata.fields.globalTags',
                        displayName: 'Source Platform Column Tags',
                        description:
                            'Tags applied to individual columns/fields as ingested from the source data platform.',
                        valueType: ValueTypeId.EXISTS_LIST,
                    },
                ],
            },
            {
                id: 'columnGlossaryTerms',
                displayName: 'Column Glossary Terms',
                children: [
                    {
                        id: 'editableSchemaMetadata.editableSchemaFieldInfo.glossaryTerms',
                        displayName: 'DataHub Column Glossary Terms',
                        description:
                            'User-editable glossary terms applied to individual columns/fields within DataHub.',
                        valueType: ValueTypeId.EXISTS_LIST,
                    },
                    {
                        id: 'schemaMetadata.fields.glossaryTerms',
                        displayName: 'Source Platform Column Glossary Terms',
                        description:
                            'Glossary terms applied to individual columns/fields as ingested from the source data platform.',
                        valueType: ValueTypeId.EXISTS_LIST,
                    },
                ],
            },
            {
                id: 'columnDescriptions',
                displayName: 'Column Descriptions',
                children: [
                    {
                        id: 'editableSchemaMetadata.editableSchemaFieldInfo.description',
                        displayName: 'DataHub Column Descriptions',
                        description: 'User-editable descriptions of individual columns/fields within DataHub.',
                        valueType: ValueTypeId.EXISTS_LIST,
                    },
                    {
                        id: 'schemaMetadata.fields.description',
                        displayName: 'Source Platform Column Descriptions',
                        description:
                            'Descriptions of individual columns/fields as ingested from the source data platform.',
                        valueType: ValueTypeId.EXISTS_LIST,
                    },
                ],
            },
            {
                id: SCHEMA_FIELD_STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
                displayName: 'Column Structured Property',
                description: 'Match datasets that have any columns with specific structured properties',
                valueType: ValueTypeId.NO_VALUE,
            },
        ],
    },

    // === STORAGE & SIZE METRICS ===
    // Important for understanding data volume and resource usage
    {
        id: 'storageMetrics',
        displayName: 'Storage & Size Metrics',
        children: [
            {
                id: 'storageFeatures.rowCount',
                displayName: 'Row Count Total',
                description:
                    'The total number of rows in the dataset. This requires data profiling to be enabled for connected data sources.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'storageFeatures.sizeInBytes',
                displayName: 'Size In Bytes',
                description:
                    'The total size of the dataset in bytes. This requires data profiling to be enabled for supported data sources.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'storageFeatures.rowCountPercentile',
                displayName: 'Row Count Percentile (0-100)',
                description:
                    'The relative row count percentile for this dataset within the data platform instance. This requires data profiling to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'storageFeatures.sizeInBytesPercentile',
                displayName: 'Size In Bytes Percentile (0-100)',
                description:
                    'The relative storage size percentile for this dataset within the data platform instance. This requires data profiling to be enabled for supported sources.',
                valueType: ValueTypeId.NUMBER,
            },
        ],
    },

    // === ADVANCED CONFIGURATION ===
    // Lower priority technical and administrative properties
    {
        id: 'datasetProperties.qualifiedName',
        displayName: 'Qualified Name',
        description: 'The fully-qualified name of the dataset.',
        valueType: ValueTypeId.STRING,
    },

    // === DATA QUALITY & RELIABILITY ===
    {
        id: 'dataQualityReliability',
        displayName: 'Data Quality & Reliability',
        children: [
            {
                id: 'dataContractStatus.state',
                displayName: 'Contract State',
                description: 'The current state of the data contract (ACTIVE, PENDING, etc.).',
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'assertions',
                displayName: 'Assertions',
                children: [
                    {
                        id: 'assertionsSummary.passingAssertionDetails',
                        displayName: 'Passing Assertions',
                        description: 'Passing assertions for the asset',
                        valueType: ValueTypeId.EXISTS_LIST,
                    },
                    {
                        id: 'assertionsSummary.failingAssertionDetails',
                        displayName: 'Failing Assertions',
                        description: 'Failing Assertions for the asset',
                        valueType: ValueTypeId.EXISTS_LIST,
                    },
                ],
            },
            {
                id: 'incidents',
                displayName: 'Incidents',
                children: [
                    {
                        id: 'incidentsSummary.activeIncidentDetails',
                        displayName: 'Active Incidents',
                        description: 'Active incidents for the asset',
                        valueType: ValueTypeId.EXISTS_LIST,
                    },
                    {
                        id: 'incidentsSummary.resolvedIncidentDetails',
                        displayName: 'Resolved Incidents',
                        description: 'Resolved incidents for the asset',
                        valueType: ValueTypeId.EXISTS_LIST,
                    },
                ],
            },
        ],
    },

    {
        id: STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Structured Property',
        description: 'Match assets that have a custom structured property',
        valueType: ValueTypeId.NO_VALUE,
    },
    {
        id: OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Ownership Type',
        description: 'Match assets that have a custom ownership type',
        valueType: ValueTypeId.NO_VALUE,
    },
];

const dataJobProps = [
    ...assetProps,
    ...dataAssetProps,

    // BASIC PROPERTIES
    {
        id: 'dataJobInfo.name',
        displayName: 'Job Name',
        description: 'The name of the job as defined in the source data platform.',
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'dataJobDescription',
        displayName: 'Description',
        children: [
            {
                id: 'editableDataJobProperties.description',
                displayName: 'DataHub Description',
                description: 'The description as authored inside DataHub directly.',
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'dataJobInfo.description',
                displayName: 'Source Platform Description',
                description: 'The description as authored and ingested from the source data platform.',
                valueType: ValueTypeId.STRING,
            },
        ],
    },

    // TECHNICAL PROPERTIES
    {
        id: STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Structured Property',
        description: 'Match assets that have a custom structured property',
        valueType: ValueTypeId.NO_VALUE,
    },
    {
        id: OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Ownership Type',
        description: 'Match assets that have a custom ownership type',
        valueType: ValueTypeId.NO_VALUE,
    },
];

const dataFlowProps = [
    ...assetProps,
    ...dataAssetProps,

    // BASIC PROPERTIES
    {
        id: 'dataFlowInfo.name',
        displayName: 'Flow Name',
        description: 'The name of the data flow as defined in the source data platform.',
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'dataFlowDescription',
        displayName: 'Description',
        children: [
            {
                id: 'editableDataFlowProperties.description',
                displayName: 'DataHub Description',
                description: 'The description as authored inside DataHub directly.',
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'dataFlowInfo.description',
                displayName: 'Source Platform Description',
                description: 'The description as authored and ingested from the source data platform.',
                valueType: ValueTypeId.STRING,
            },
        ],
    },

    // TECHNICAL PROPERTIES
    {
        id: STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Structured Property',
        description: 'Match assets that have a custom structured property',
        valueType: ValueTypeId.NO_VALUE,
    },
    {
        id: OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Ownership Type',
        description: 'Match assets that have a custom ownership type',
        valueType: ValueTypeId.NO_VALUE,
    },
];

const dashboardProps = [
    ...assetProps,
    ...dataAssetProps,

    // BASIC PROPERTIES
    {
        id: 'dashboardDescription',
        displayName: 'Description',
        children: [
            {
                id: 'dashboardInfo.description',
                displayName: 'Native Platform Description',
                description: 'The description as authored and ingested from an external Data Platform.',
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'editableDashboardProperties.description',
                displayName: 'DataHub Description',
                description: 'The description as authored inside DataHub directly.',
                valueType: ValueTypeId.STRING,
            },
        ],
    },
    {
        id: 'dashboardInfo.title',
        displayName: 'Dashboard Name',
        description: 'The name of the dashboard as defined in the source data platform.',
        valueType: ValueTypeId.STRING,
    },

    // USAGE METRICS
    {
        id: 'dashboardMetrics',
        displayName: 'Usage Metrics',
        children: [
            {
                id: 'usageFeatures.viewCountTotal',
                displayName: 'Total View Count',
                description:
                    'The total view count for this dashboard. This requires dashboard usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.viewCountLast30Days',
                displayName: 'View Count in Last 30 Days',
                description:
                    'The total view count for this dashboard in the past 30 days. This requires dashboard usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.viewCountPercentileLast30Days',
                displayName: 'View Count Percentile in Last 30 Days (0-100)',
                description:
                    'The relative view count percentile for this dashboard within the data platform instance in the past 30 days. This requires dashboard usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.uniqueUserCountLast30Days',
                displayName: 'Unique Users in the Last 30 Days',
                description:
                    'The total unique user count for this dashboard in the past 30 days. This requires dashboard usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.uniqueUserPercentileLast30Days',
                displayName: 'Unique User Percentile in the Last 30 Days (0-100)',
                description:
                    'The relative view count percentile for this dashboard within the data platform instance in the past 30 days. This requires dashboard usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
        ],
    },

    // TECHNICAL PROPERTIES
    {
        id: STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Structured Property',
        description: 'Match assets that have a custom structured property',
        valueType: ValueTypeId.NO_VALUE,
    },
    {
        id: OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Ownership Type',
        description: 'Match assets that have a custom ownership type',
        valueType: ValueTypeId.NO_VALUE,
    },
];

const chartProps = [
    ...assetProps,
    ...dataAssetProps,

    // BASIC PROPERTIES
    {
        id: 'chartDescription',
        displayName: 'Description',
        children: [
            {
                id: 'editableChartProperties.description',
                displayName: 'DataHub Description',
                description: 'The description as authored inside DataHub directly.',
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'chartInfo.description',
                displayName: 'Source Platform Description',
                description: 'The description as authored and ingested from the source data platform.',
                valueType: ValueTypeId.STRING,
            },
        ],
    },
    {
        id: 'chartInfo.title',
        displayName: 'Chart Name',
        description: 'The name of the chart as defined in the source data platform.',
        valueType: ValueTypeId.STRING,
    },

    // USAGE METRICS
    {
        id: 'chartMetrics',
        displayName: 'Usage Metrics',
        selectable: false,
        children: [
            {
                id: 'usageFeatures.viewCountTotal',
                displayName: 'Total View Count',
                description:
                    'The total view count for this chart. This requires chart usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.viewCountLast30Days',
                displayName: 'View Count in Last 30 Days',
                description:
                    'The total view count for this chart in the past 30 days. This requires chart usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.viewCountPercentileLast30Days',
                displayName: 'View Count Percentile in Last 30 Days (0-100)',
                description:
                    'The relative view count percentile for this chart within the data platform instance in the past 30 days. This requires chart usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.uniqueUserCountLast30Days',
                displayName: 'Unique Users in the Last 30 Days',
                description:
                    'The total unique user count for this chart in the past 30 days. This requires chart usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.uniqueUserPercentileLast30Days',
                displayName: 'Unique User Percentile in the Last 30 Days (0-100)',
                description:
                    'The relative view count percentile for this chart within the data platform instance in the past 30 days. This requires chart usage data ingestion to be enabled.',
                valueType: ValueTypeId.NUMBER,
            },
        ],
    },

    // TECHNICAL PROPERTIES
    {
        id: STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Structured Property',
        description: 'Match assets that have a custom structured property',
        valueType: ValueTypeId.NO_VALUE,
    },
    {
        id: OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Ownership Type',
        description: 'Match assets that have a custom ownership type',
        valueType: ValueTypeId.NO_VALUE,
    },
];

const containerProps = [
    ...assetProps,
    ...dataAssetProps,

    // BASIC PROPERTIES
    {
        id: 'containerProperties.name',
        displayName: 'Source Platform Name',
        description: 'The name of the container as defined in the source data platform.',
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'containerDescription',
        displayName: 'Description',
        children: [
            {
                id: 'editableContainerProperties.description',
                displayName: 'DataHub Description',
                description: 'The description as authored inside DataHub directly.',
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'containerProperties.description',
                displayName: 'Source Platform Description',
                description: 'The description as authored and ingested from the source data platform.',
                valueType: ValueTypeId.STRING,
            },
        ],
    },

    // TECHNICAL PROPERTIES
    {
        id: STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Structured Property',
        description: 'Match assets that have a custom structured property',
        valueType: ValueTypeId.NO_VALUE,
    },
    {
        id: OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID,
        displayName: 'Ownership Type',
        description: 'Match assets that have a custom ownership type',
        valueType: ValueTypeId.NO_VALUE,
    },
];

const glossaryTermProps = [
    ...commonProps,
    {
        id: 'glossaryTermInfo.name',
        displayName: 'Name',
        description: 'The name of the glossary term.',
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'glossaryTermInfo.definition',
        displayName: 'Description',
        description: 'The description of the glossary term.',
        valueType: ValueTypeId.STRING,
    },

    {
        id: 'glossaryTermInfo.parentNode',
        displayName: 'Parent Term Group',
        description: 'The parent term group of this glossary term.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.GlossaryNode],
            mode: SelectInputMode.SINGLE,
        },
    },
    {
        id: 'glossaryTermInfo.customProperties',
        displayName: 'Custom Properties',
        description: 'Custom properties defined for this glossary term.',
        valueType: ValueTypeId.ENTRY_LIST,
    },
];

const glossaryNodeProps = [
    ...commonProps,
    {
        id: 'glossaryNodeInfo.name',
        displayName: 'Name',
        description: 'Display name of the glossary node.',
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'glossaryNodeInfo.definition',
        displayName: 'Description',
        description: 'Description of the glossary node.',
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'glossaryNodeInfo.parentNode',
        displayName: 'Parent Node',
        description: 'Parent node of the glossary node.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.GlossaryNode],
            mode: SelectInputMode.SINGLE,
        },
    },
];

const domainProps = [
    ...commonProps,
    {
        id: 'domainProperties.name',
        displayName: 'Name',
        description: 'The name of the domain.',
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'domainProperties.description',
        displayName: 'Description',
        description: 'The description of the domain.',
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'domainProperties.parentDomain',
        displayName: 'Parent Domain',
        description: 'The parent domain of this domain.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Domain],
            mode: SelectInputMode.SINGLE,
        },
    },
];

const dataProductProps = [
    ...assetProps,

    // BASIC PROPERTIES
    {
        id: 'dataProductProperties.name',
        displayName: 'Name',
        description: 'The name of the data product.',
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'dataProductProperties.description',
        displayName: 'Description',
        description: 'The description of the data product.',
        valueType: ValueTypeId.STRING,
    },
];

/**
 * A list of entity types to the well-supported properties
 * that each can support.
 */
export const entityProperties = [
    {
        type: EntityType.Dataset,
        properties: datasetProps,
    },
    {
        type: EntityType.Dashboard,
        properties: dashboardProps,
    },
    {
        type: EntityType.Chart,
        properties: chartProps,
    },
    {
        type: EntityType.DataFlow,
        properties: dataFlowProps,
    },
    {
        type: EntityType.DataJob,
        properties: dataJobProps,
    },
    {
        type: EntityType.Container,
        properties: containerProps,
    },
    {
        type: EntityType.GlossaryTerm,
        properties: glossaryTermProps,
    },
    {
        type: EntityType.GlossaryNode,
        properties: glossaryNodeProps,
    },
    {
        type: EntityType.Domain,
        properties: domainProps,
    },
    {
        type: EntityType.DataProduct,
        properties: dataProductProps,
    },
];
