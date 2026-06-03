/**
 * The file contains a set of well-supported properties,
 * which the UI deeply understands.
 */
import i18next from 'i18next';

import {
    OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID,
    STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
} from '@app/sharedV2/queryBuilder/builder/property/constants';
import { SelectInputMode, ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';

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
 *          "If an asset has > 10 downstreams, then mark it as Heavy node"
 *          "Must have an owner of type Technical Owner"
 */
const commonProps: Property[] = [
    {
        id: 'urn',
        get displayName() {
            return i18next.t('shared.query-builder:prop.urn');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.urnDesc');
        },
        valueType: ValueTypeId.STRING,
        valueOptions: {
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'entityType',
        get displayName() {
            return i18next.t('shared.query-builder:prop.type');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.typeDesc');
        },
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            mode: SelectInputMode.MULTIPLE,
            options: [
                { id: 'dataset', displayName: 'Dataset' },
                { id: 'dashboard', displayName: 'Dashboard' },
                { id: 'chart', displayName: 'Chart' },
                { id: 'dataJob', displayName: 'Data Job (Task)' },
                { id: 'dataFlow', displayName: 'Data Flow (Pipeline)' },
                { id: 'container', displayName: 'Container' },
            ],
        },
    },
    // {
    //     id: 'name', // --> TODO Determine what this means for Datasets.
    //     displayName: 'Name',
    //     description: 'The name of the asset, as defined at the source.',
    //     valueType: ValueTypeId.STRING,
    // },
    // {
    //     id: 'editableDatasetProperties.description',
    //     displayName: 'Description',
    //     description: 'The description text for the asset, as displayed inside the Documentation tab.',
    //     valueType: ValueTypeId.STRING,
    // },
    {
        id: 'dataPlatformInstance.platform',
        get displayName() {
            return i18next.t('shared.query-builder:prop.platform');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.platformDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.DataPlatform],
            mode: SelectInputMode.SINGLE,
        },
    },
    {
        id: 'globalTags.tags.tag',
        get displayName() {
            return i18next.t('shared.query-builder:prop.tags');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.tagsDesc');
        },
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.Tag],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'glossaryTerms.terms.urn',
        get displayName() {
            return i18next.t('shared.query-builder:prop.glossaryTerms');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.glossaryTermsDesc');
        },
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.GlossaryTerm],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'glossaryTerms.terms.urn.glossaryTermInfo.parentNode',
        get displayName() {
            return i18next.t('shared.query-builder:prop.glossaryTermGroups');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.glossaryTermGroupsDesc');
        },
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.GlossaryNode],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'domains.domains',
        get displayName() {
            return i18next.t('shared.query-builder:prop.domain');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.domainDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Domain],
            mode: SelectInputMode.SINGLE,
        },
    },
    {
        id: 'ownership.owners.owner',
        get displayName() {
            return i18next.t('shared.query-builder:prop.owners');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.ownersDesc');
        },
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.CorpUser, EntityType.CorpGroup],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'browsePathsV2.path.urn',
        get displayName() {
            return i18next.t('shared.query-builder:prop.browsePathContainer');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.browsePathContainerDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Container],
            mode: SelectInputMode.SINGLE,
        },
    },
    {
        id: 'container.container',
        get displayName() {
            return i18next.t('shared.query-builder:prop.container');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.containerDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Container],
            mode: SelectInputMode.SINGLE,
        },
    },
    {
        id: 'deprecation.deprecated',
        get displayName() {
            return i18next.t('shared.query-builder:prop.deprecated');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.deprecatedDesc');
        },
        valueType: ValueTypeId.BOOLEAN,
    },
    {
        id: '__firstSynchronized',
        get displayName() {
            return i18next.t('shared.query-builder:prop.firstSynchronized');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.firstSynchronizedDesc');
        },
        valueType: ValueTypeId.TIMESTAMP,
    },
    {
        id: '__lastSynchronized',
        get displayName() {
            return i18next.t('shared.query-builder:prop.lastSynchronized');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.lastSynchronizedDesc');
        },
        valueType: ValueTypeId.TIMESTAMP,
    },
    {
        id: '__lastObserved',
        get displayName() {
            return i18next.t('shared.query-builder:prop.lastObserved');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.lastObservedDesc');
        },
        valueType: ValueTypeId.TIMESTAMP,
    },
];

const datasetProps: Property[] = [
    ...commonProps,
    {
        id: 'datasetDescription',
        get displayName() {
            return i18next.t('shared.query-builder:prop.description');
        },
        children: [
            {
                id: 'datasetProperties.description',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.nativePlatformDescription');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.nativePlatformDescriptionDesc');
                },
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'editableDatasetProperties.description',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.dataHubDescription');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.dataHubDescriptionDesc');
                },
                valueType: ValueTypeId.STRING,
            },
        ],
    },
    {
        id: 'subTypes.typeNames',
        get displayName() {
            return i18next.t('shared.query-builder:prop.subtype');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.subtypeDesc');
        },
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'schemaFields',
        get displayName() {
            return i18next.t('shared.query-builder:prop.columns');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.columnsDesc');
        },
        valueType: ValueTypeId.SCHEMA_FIELD_LIST,
    },
    {
        id: 'schemaFields.length',
        get displayName() {
            return i18next.t('shared.query-builder:prop.numColumns');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.numColumnsDesc');
        },
        valueType: ValueTypeId.NUMBER,
    },
    {
        id: 'datasetMetrics',
        get displayName() {
            return i18next.t('shared.query-builder:prop.metrics');
        },
        children: [
            {
                id: 'usageFeatures.usageCountLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.queryCountLast30Days');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.queryCountLast30DaysDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.queryCountPercentileLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.queryCountPercentile');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.queryCountPercentileDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.writeCountLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.updateCountLast30Days');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.updateCountLast30DaysDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.writeCountPercentileLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.updateCountPercentile');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.updateCountPercentileDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.uniqueUserCountLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.uniqueUsersLast30Days');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.datasetUniqueUsersDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.uniqueUserPercentileLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.uniqueUserPercentile');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.datasetUniqueUserPercentileDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'storageFeatures.rowCount',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.rowCount');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.rowCountDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'storageFeatures.rowCountPercentile',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.rowCountPercentile');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.rowCountPercentileDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'storageFeatures.sizeInBytes',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.sizeInBytes');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.sizeInBytesDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'storageFeatures.sizeInBytesPercentile',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.sizeInBytesPercentile');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.sizeInBytesPercentileDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
        ],
    },
    {
        id: 'assertions',
        get displayName() {
            return i18next.t('shared.query-builder:prop.assertions');
        },
        children: [
            {
                id: 'assertionsSummary.passingAssertionDetails',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.passingAssertions');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.passingAssertionsDesc');
                },
                valueType: ValueTypeId.EXISTS_LIST,
            },
            {
                id: 'assertionsSummary.failingAssertionDetails',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.failingAssertions');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.failingAssertionsDesc');
                },
                valueType: ValueTypeId.EXISTS_LIST,
            },
        ],
    },
    {
        id: 'incidents',
        get displayName() {
            return i18next.t('shared.query-builder:prop.incidents');
        },
        children: [
            {
                id: 'incidentsSummary.activeIncidentDetails',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.activeIncidents');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.activeIncidentsDesc');
                },
                valueType: ValueTypeId.EXISTS_LIST,
            },
            {
                id: 'incidentsSummary.resolvedIncidentDetails',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.resolvedIncidents');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.resolvedIncidentsDesc');
                },
                valueType: ValueTypeId.EXISTS_LIST,
            },
        ],
    },
    // Simply serves as an entry point to defining a structured property reference.
    // Once this property is selected, we'll transfer the user to the structured property predicate
    // builder experience.
    {
        id: STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
        get displayName() {
            return i18next.t('shared.query-builder:prop.structuredProperty');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.structuredPropertyDesc');
        },
        valueType: ValueTypeId.NO_VALUE,
    },
    // Simply serves as an entry point to defining a ownership type reference.
    // Once this type is selected, we'll transfer the user to the ownership type predicate
    // builder experience.
    {
        id: OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID,
        get displayName() {
            return i18next.t('shared.query-builder:prop.ownershipType');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.ownershipTypeDesc');
        },
        valueType: ValueTypeId.NO_VALUE,
    },
];

const dataJobProps = [
    ...commonProps,
    {
        id: 'dataJobDescription',
        get displayName() {
            return i18next.t('shared.query-builder:prop.description');
        },
        children: [
            {
                id: 'dataJobInfo.description',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.nativePlatformDescription');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.nativePlatformDescriptionDesc');
                },
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'editableDataJobProperties.description',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.dataHubDescription');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.dataHubDescriptionDesc');
                },
                valueType: ValueTypeId.STRING,
            },
        ],
    },
];

const dataFlowProps = [
    ...commonProps,
    {
        id: 'dataFlowDescription',
        get displayName() {
            return i18next.t('shared.query-builder:prop.description');
        },
        children: [
            {
                id: 'dataFlowInfo.description',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.nativePlatformDescription');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.nativePlatformDescriptionDesc');
                },
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'editableDataFlowProperties.description',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.dataHubDescription');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.dataHubDescriptionDesc');
                },
                valueType: ValueTypeId.STRING,
            },
        ],
    },
];

const dashboardProps = [
    ...commonProps,
    {
        id: 'dashboardDescription',
        get displayName() {
            return i18next.t('shared.query-builder:prop.description');
        },
        children: [
            {
                id: 'dashboardInfo.description',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.nativePlatformDescription');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.nativePlatformDescriptionDesc');
                },
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'editableDashboardProperties.description',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.dataHubDescription');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.dataHubDescriptionDesc');
                },
                valueType: ValueTypeId.STRING,
            },
        ],
    },
    {
        id: 'dashboardMetrics',
        get displayName() {
            return i18next.t('shared.query-builder:prop.metrics');
        },
        children: [
            {
                id: 'usageFeatures.viewCountTotal',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.totalViewCount');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.dashboardTotalViewCountDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.viewCountLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.viewCountLast30Days');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.dashboardViewCountLast30DaysDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.viewCountPercentileLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.viewCountPercentile');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.dashboardViewCountPercentileDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.uniqueUserCountLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.uniqueUsersLast30Days');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.dashboardUniqueUsersDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.uniqueUserPercentileLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.uniqueUserPercentile');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.dashboardUniqueUserPercentileDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
        ],
    },
];

const chartProps = [
    ...commonProps,
    {
        id: 'chartDescription',
        get displayName() {
            return i18next.t('shared.query-builder:prop.description');
        },
        children: [
            {
                id: 'chartInfo.description',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.nativePlatformDescription');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.nativePlatformDescriptionDesc');
                },
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'editableChartProperties.description',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.dataHubDescription');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.dataHubDescriptionDesc');
                },
                valueType: ValueTypeId.STRING,
            },
        ],
    },
    {
        id: 'chartMetrics',
        get displayName() {
            return i18next.t('shared.query-builder:prop.metrics');
        },
        selectable: false,
        children: [
            {
                id: 'usageFeatures.viewCountTotal',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.totalViewCount');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.chartTotalViewCountDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.viewCountLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.viewCountLast30Days');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.chartViewCountLast30DaysDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.viewCountPercentileLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.viewCountPercentile');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.chartViewCountPercentileDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.uniqueUserCountLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.uniqueUsersLast30Days');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.chartUniqueUsersDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
            {
                id: 'usageFeatures.uniqueUserPercentileLast30Days',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.uniqueUserPercentile');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.chartUniqueUserPercentileDesc');
                },
                valueType: ValueTypeId.NUMBER,
            },
        ],
    },
];

const containerProps = [
    ...commonProps,
    {
        id: 'containerDescription',
        get displayName() {
            return i18next.t('shared.query-builder:prop.description');
        },
        children: [
            {
                id: 'containerProperties.description',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.nativePlatformDescription');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.nativePlatformDescriptionDesc');
                },
                valueType: ValueTypeId.STRING,
            },
            {
                id: 'editableContainerProperties.description',
                get displayName() {
                    return i18next.t('shared.query-builder:prop.dataHubDescription');
                },
                get description() {
                    return i18next.t('shared.query-builder:prop.dataHubDescriptionDesc');
                },
                valueType: ValueTypeId.STRING,
            },
        ],
    },
    {
        id: 'subTypes.typeNames',
        get displayName() {
            return i18next.t('shared.query-builder:prop.subtypes');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.subtypesDesc');
        },
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
];
