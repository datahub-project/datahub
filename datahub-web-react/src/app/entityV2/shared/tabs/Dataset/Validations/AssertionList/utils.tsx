import React from 'react';
import Fuse from 'fuse.js';
import { decodeSchemaField } from '@src/app/lineage/utils/columnLineageUtils';
import styled from 'styled-components';
import { Typography } from 'antd';

import {
    Assertion,
    AssertionInfo,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AssertionSourceType,
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionType,
    AuditStamp,
    CronSchedule,
    DatasetAssertionInfo,
    DatasetAssertionScope,
    FieldAssertionInfo,
    FreshnessAssertionInfo,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    GlobalTags,
    IncrementingSegmentRowCountChange,
    Monitor,
    RowCountChange,
    SchemaAssertionCompatibility,
    SchemaAssertionInfo,
    SchemaFieldRef,
    VolumeAssertionInfo,
} from '@src/types.generated';
import {
    getIsRowCountChange,
    getOperatorDescription,
    getParameterDescription,
    getValueChangeTypeDescription,
    getVolumeTypeDescription,
    getVolumeTypeInfo,
} from '../utils';
import {
    getFieldDescription,
    getFieldOperatorDescription,
    getFieldParametersDescription,
    getFieldTransformDescription,
} from '../fieldDescriptionUtils';

import { getFormattedParameterValue } from '../assertionUtils';
import { AssertionWithMonitorDetails, createAssertionGroups, getAssertionGroupName } from '../acrylUtils';
import { isExternalAssertion } from '../assertion/profile/shared/isExternalAssertion';
import { AssertionGroupHeader } from './AssertionGroupHeader';
import {
    AssertionStatusGroup,
    AssertionTable,
    AssertionListFilter,
    AssertionListTableRow,
    AssertionFilterOptions,
    AssertionRecommendedFilter,
    AssertionWithDescription,
} from './types';
import { createCronText, createFixedIntervalText, createSinceTheLastCheckText } from '../FreshnessAssertionDescription';

/**
 * It refers the {@link getSchemaAggregationText} utility function to get plain text from html description.
 * @link getSchemaAggregationText
 * Returns the Plain Text to render for the aggregation portion of the Assertion Description
 * for Assertions on Dataset Schemas.
 *
 * Schema assertions require an aggregation.
 */
export const getSchemaAggregationPlainText = (
    aggregation: AssertionStdAggregation | undefined | null,
    fields: Array<SchemaFieldRef> | undefined | null,
) => {
    switch (aggregation) {
        case AssertionStdAggregation.ColumnCount:
            return 'Dataset column count is';
        case AssertionStdAggregation.Columns:
            return 'Dataset columns are';
        case AssertionStdAggregation.Native: {
            const fieldNames = fields?.map((field) => decodeSchemaField(field.path)) || [];
            return `Dataset columns ${JSON.stringify(fieldNames)} are `;
        }
        default:
            console.error(`Unsupported schema aggregation assertion ${aggregation} provided.`);
            return 'Dataset columns are';
    }
};

/**
 * It refers the {@link getRowsAggregationText} utility function to get plain text from html description.
 * @link getRowsAggregationText
 * for Assertions on Dataset Rows
 *
 * Row assertions require an aggregation.
 */
export const getRowsAggregationPlainText = (aggregation: AssertionStdAggregation | undefined | null) => {
    switch (aggregation) {
        case AssertionStdAggregation.RowCount:
            return 'Dataset row count is';
        case AssertionStdAggregation.Native:
            return 'Dataset rows are';
        default:
            console.error(`Unsupported Dataset Rows Aggregation ${aggregation} provided`);
            return 'Dataset rows are';
    }
};

/**
 *
 * It refers the {@link getColumnAggregationText} utility function to get plain text from html description.
 * @link getColumnAggregationText
 *
 * Returns the Plain Text to render for the aggregation portion of the Assertion Description
 * for Assertions on Dataset Columns
 */
export const getColumnAggregationPlainText = (
    aggregation: AssertionStdAggregation | undefined | null,
    field: SchemaFieldRef | undefined,
) => {
    let columnText = decodeSchemaField(field?.path || '');
    if (field === undefined) {
        columnText = 'undefined';
        console.error(`Invalid field provided for Dataset Assertion with scope Column ${JSON.stringify(field)}`);
    }
    switch (aggregation) {
        // Hybrid Aggregations
        case AssertionStdAggregation.UniqueCount: {
            return `Unique value count for column ${columnText} is`;
        }
        case AssertionStdAggregation.UniquePropotion: {
            return `Unique value proportion for column ${columnText} is`;
        }
        case AssertionStdAggregation.NullCount: {
            return `Null count for column ${columnText} is`;
        }
        case AssertionStdAggregation.NullProportion: {
            return `Null proportion for column ${columnText} is`;
        }
        // Numeric Aggregations
        case AssertionStdAggregation.Min: {
            return `Minimum value for column ${columnText} is`;
        }
        case AssertionStdAggregation.Max: {
            return `Maximum value for column ${columnText} is`;
        }
        case AssertionStdAggregation.Mean: {
            return `Mean value for column ${columnText} is`;
        }
        case AssertionStdAggregation.Median: {
            return `Median value for column ${columnText} is`;
        }
        case AssertionStdAggregation.Stddev: {
            return `Standard deviation for column ${columnText} is`;
        }
        // Native Aggregations
        case AssertionStdAggregation.Native: {
            return `Column ${columnText} values are`;
        }
        default:
            // No aggregation on the column at hand. Treat the column as a set of values.
            return `Column ${columnText} values are`;
    }
};

/**
 * It refers the {@link getAggregationText} utility function to get plain text from html description.
 * @link getAggregationText
 * Returns the Plain Text to render for the aggregation portion of the Assertion Description
 */
export const getAggregationPlainText = (
    scope: DatasetAssertionScope,
    aggregation: AssertionStdAggregation | undefined | null,
    fields: Array<SchemaFieldRef> | undefined | null,
) => {
    switch (scope) {
        case DatasetAssertionScope.DatasetSchema:
            return getSchemaAggregationPlainText(aggregation, fields);
        case DatasetAssertionScope.DatasetRows:
            return getRowsAggregationPlainText(aggregation);
        case DatasetAssertionScope.DatasetColumn:
            return getColumnAggregationPlainText(aggregation, fields?.length === 1 ? fields[0] : undefined);
        default:
            console.error(`Unsupported Dataset Assertion scope ${scope} provided`);
            return 'Dataset is';
    }
};

/**
 * It refers the {@link getOperatorText} utility function to get plain text from html description.
 * @link getOperatorText
 * Returns the Plain Text to render for the operator portion of the Assertion Description
 */
export const getOperatorPlainText = (
    op: AssertionStdOperator,
    parameters: AssertionStdParameters | undefined,
    nativeType: string | undefined,
) => {
    switch (op) {
        // Hybrid Operators
        case AssertionStdOperator.Between: {
            return `between ${getFormattedParameterValue(parameters?.minValue)} and ${getFormattedParameterValue(
                parameters?.maxValue,
            )}`;
        }
        case AssertionStdOperator.EqualTo: {
            const operatorText = 'equal to';
            return `${operatorText} ${getFormattedParameterValue(parameters?.value)}`;
        }
        case AssertionStdOperator.Contain: {
            const operatorText = 'contains';
            return `${operatorText} ${getFormattedParameterValue(parameters?.value)}`;
        }
        case AssertionStdOperator.In: {
            const operatorText = 'in';
            return `${operatorText} ${getFormattedParameterValue(parameters?.value)}`;
        }
        case AssertionStdOperator.NotNull: {
            const operatorText = 'not null';
            return operatorText;
        }
        case AssertionStdOperator.GreaterThan: {
            const operatorText = 'greater than';
            return `${operatorText} ${getFormattedParameterValue(parameters?.value)}`;
        }
        case AssertionStdOperator.GreaterThanOrEqualTo: {
            const operatorText = 'greater than or equal to';
            return `${operatorText} ${getFormattedParameterValue(parameters?.value)}`;
        }
        case AssertionStdOperator.LessThan: {
            const operatorText = 'less than';
            return `${operatorText} ${getFormattedParameterValue(parameters?.value)}`;
        }
        case AssertionStdOperator.LessThanOrEqualTo: {
            const operatorText = 'less than or equal to';
            return `${operatorText} ${getFormattedParameterValue(parameters?.value)}`;
        }
        case AssertionStdOperator.StartWith: {
            const operatorText = 'starts with';
            return `${operatorText} ${getFormattedParameterValue(parameters?.value)}`;
        }
        case AssertionStdOperator.EndWith: {
            const operatorText = 'ends with';
            return `${operatorText} ${getFormattedParameterValue(parameters?.value)}`;
        }
        case AssertionStdOperator.Native: {
            return `passing assertion ${nativeType}`;
        }
        default: {
            return `passing operator ${op} with value ${getFormattedParameterValue(parameters?.value)}`;
        }
    }
};

/**
 * It refers the {@link DatasetAssertionDescription} utility function to get plain text from html description.
 * @link DatasetAssertionDescription
 * A human-readable Plain Text description of a Dataset Assertion.
 */

export const getDatasetAssertionPlainTextDescription = (datasetAssertion: DatasetAssertionInfo): string => {
    const { scope, aggregation, fields, operator, parameters, nativeType } = datasetAssertion;
    const aggregationPlainText = getAggregationPlainText(scope, aggregation, fields);
    const operatorPlainText = getOperatorPlainText(operator, parameters || undefined, nativeType || undefined);
    return `${aggregationPlainText} ${operatorPlainText}`;
};

/**
 * It refers the {@link getAggregationText} utility function to get plain text from html description.
 * @link getAggregationText
 * A human-readable Plain Text description of a Volume Assertion.
 */
export const getVolumeAssertionPlainTextDescription = (assertionInfo: VolumeAssertionInfo): string => {
    const volumeType = assertionInfo.type;
    const volumeTypeInfo = getVolumeTypeInfo(assertionInfo);
    const volumeTypeDescription = getVolumeTypeDescription(volumeType);
    const operatorDescription = volumeTypeInfo ? getOperatorDescription(volumeTypeInfo.operator) : '';
    const parameterDescription = volumeTypeInfo ? getParameterDescription(volumeTypeInfo.parameters) : '';
    const valueChangeTypeDescription = getIsRowCountChange(volumeType)
        ? getValueChangeTypeDescription((volumeTypeInfo as RowCountChange | IncrementingSegmentRowCountChange).type)
        : 'rows';

    return `Table ${volumeTypeDescription} ${operatorDescription} ${parameterDescription} ${valueChangeTypeDescription}`;
};

/**
 * It refers the {@link getAggregationText} utility function to get plain text from html description.
 * @link getAggregationText
 * A human-readable Plain Text description of a Field Assertion.
 */
export const getFieldAssertionPlainTextDescription = (assertionInfo: FieldAssertionInfo) => {
    const field = getFieldDescription(assertionInfo);
    const operator = getFieldOperatorDescription(assertionInfo);
    const transform = getFieldTransformDescription(assertionInfo);
    const parameters = getFieldParametersDescription(assertionInfo);
    return `${transform} ${transform ? ' of ' : ''}${field} ${operator} ${parameters}`;
};

/**
 * It refers the {@link getAggregationText} utility function to get plain text from html description.
 * @link getAggregationText
 * A human-readable Plain Text description of a Schema Assertion.
 */
export const getSchemaAssertionPlainTextDescription = (assertionInfo: SchemaAssertionInfo) => {
    const { compatibility } = assertionInfo;
    const matchText = compatibility === SchemaAssertionCompatibility.ExactMatch ? 'exactly match' : 'include';
    const expectedColumnCount = assertionInfo?.fields?.length || 0;
    return `Actual table columns ${matchText} ${expectedColumnCount} expected columns`;
};

/** below functions are related to Freshness */

/**
 * A human-readable Plain Text description of an Freshness Assertion.
 */
export const getFreshnessAssertionPlainTextDescription = (
    assertionInfo: FreshnessAssertionInfo,
    monitorSchedule: CronSchedule,
) => {
    const scheduleType = assertionInfo.schedule?.type;
    const freshnessType = assertionInfo.type;

    let scheduleText = '';
    switch (scheduleType) {
        case FreshnessAssertionScheduleType.FixedInterval:
            scheduleText = createFixedIntervalText(assertionInfo.schedule?.fixedInterval, monitorSchedule);
            break;
        case FreshnessAssertionScheduleType.Cron:
            scheduleText = createCronText(assertionInfo.schedule?.cron as CronSchedule);
            break;
        case FreshnessAssertionScheduleType.SinceTheLastCheck:
            scheduleText = createSinceTheLastCheckText(monitorSchedule);
            break;
        default:
            scheduleText = 'within an unrecognized schedule window.';
            break;
    }
    return `${
        freshnessType === FreshnessAssertionType.DatasetChange ? 'Table was updated ' : 'Data Task is run successfully '
    }${scheduleText}`;
};

/** get assertion plain text description by assertion type */

export const getPlainTextDescriptionFromAssertion = (
    assertionInfo?: AssertionInfo,
    monitorSchedule?: CronSchedule,
): string => {
    let primaryLabel = '';
    if (assertionInfo?.description) {
        primaryLabel = assertionInfo.description;
    }
    switch (assertionInfo?.type) {
        case AssertionType.Dataset:
            primaryLabel = getDatasetAssertionPlainTextDescription(
                assertionInfo.datasetAssertion as DatasetAssertionInfo,
            );
            break;
        case AssertionType.Freshness:
            primaryLabel = getFreshnessAssertionPlainTextDescription(
                assertionInfo.freshnessAssertion as FreshnessAssertionInfo,
                monitorSchedule as CronSchedule,
            );
            break;
        case AssertionType.Volume:
            primaryLabel = getVolumeAssertionPlainTextDescription(assertionInfo.volumeAssertion as VolumeAssertionInfo);
            break;
        case AssertionType.Sql:
            primaryLabel = assertionInfo.description || '';
            break;
        case AssertionType.Field:
            primaryLabel = getFieldAssertionPlainTextDescription(assertionInfo.fieldAssertion as FieldAssertionInfo);
            break;
        case AssertionType.DataSchema:
            primaryLabel = getSchemaAssertionPlainTextDescription(assertionInfo.schemaAssertion as SchemaAssertionInfo);
            break;
        default:
            break;
    }
    return primaryLabel;
};

const ASSERTION_TYPE_NAME_MAP = {
    VOLUME: 'Volume',
    SQL: 'Sql',
    FIELD: 'Field',
    FRESHNESS: 'Freshness',
    DATASET: 'Other',
};

const ASSERTION_STATUS_NAME_MAP = {
    FAILURE: 'Failing',
    SUCCESS: 'Passing',
    ERROR: 'Error',
};

const STATUS_GROUP_NAME_MAP = { ...ASSERTION_TYPE_NAME_MAP, ...ASSERTION_STATUS_NAME_MAP };

const RECOMMENDED_FILTER_NAME_MAP = {
    [AssertionSourceType.External]: 'External',
    [AssertionSourceType.Native]: 'Native',
    [AssertionSourceType.Inferred]: 'Smart Assertions',
};

// Create Group's Summary to name and number of records for each group
const getGroupNameBySummary = (record) => {
    const TextContainer = styled.div`
        display: flex;
        align-items: center;
        justify-content: left;
        font-size: 14px;
    `;

    const Title = styled(Typography.Text)`
        && {
            padding-bottom: 0px;
            margin-bottom: 0px;
        }
    `;

    const Message = styled(Typography.Text)`
        && {
            font-size: 12px;
            margin-left: 8px;
        }
    `;

    const newSummary = record.summary;
    const list: string[] = [];
    Object.keys(newSummary).forEach((key) => {
        if (newSummary[key] > 0) {
            list.push(`${newSummary[key]} ${STATUS_GROUP_NAME_MAP[key]}`);
        }
    });

    return (
        <TextContainer>
            <Title strong>{STATUS_GROUP_NAME_MAP[record.name]}</Title>
            <Message type="secondary">{list.join(', ')}</Message>
        </TextContainer>
    );
};

// transform assertions into table data
const mapAssertionData = (assertions: AssertionWithMonitorDetails[] | Assertion[]): AssertionListTableRow[] => {
    return assertions.map((assertion: AssertionWithMonitorDetails) => {
        const mostRecentRun = assertion.runEvents?.runEvents?.[0];

        const monitor = assertion.monitor?.relationships?.[0]?.entity;
        const primaryPainTextLabel = getPlainTextDescriptionFromAssertion(assertion.info as AssertionInfo, monitor);
        const isCompleted = mostRecentRun?.status === AssertionRunStatus.Complete;
        const rowData: AssertionListTableRow = {
            type: assertion.info?.type,
            lastUpdated: assertion.info?.lastUpdated as AuditStamp,
            tags: assertion.tags as GlobalTags,
            descriptionHTML: null,
            description: primaryPainTextLabel,
            urn: assertion.urn,
            platform: assertion.platform,
            lastEvaluation: (isCompleted && mostRecentRun) as AssertionRunEvent,
            lastEvaluationTimeMs: mostRecentRun?.timestampMillis,
            lastEvaluationResult: (isCompleted && mostRecentRun?.result?.type) as AssertionResultType,
            lastEvaluationUrl: (isCompleted && mostRecentRun?.result?.externalUrl) || '',
            assertion: assertion as AssertionWithMonitorDetails,
            monitor: monitor as Monitor,
            status: mostRecentRun?.status as AssertionRunStatus,
        };
        return rowData;
    });
};

// Generate Assertion Group By Status
const generateAssertionGroupByStatus = (assertions: AssertionWithMonitorDetails[]): AssertionStatusGroup[] => {
    const STATUSES = [
        AssertionResultType.Success,
        AssertionResultType.Failure,
        AssertionResultType.Error,
        AssertionResultType.Init,
    ];

    const assertionGroup: AssertionStatusGroup[] = [];

    STATUSES.forEach((status) => {
        const filteredAssertions = assertions.filter((assertion) => {
            const mostRecentRun = assertion.runEvents?.runEvents?.[0];
            const resultType = mostRecentRun?.result?.type;
            return assertion.info?.type && resultType === status;
        });

        if (filteredAssertions.length > 0) {
            const summary = {};
            filteredAssertions.forEach((assertion) => {
                const assertionType = assertion?.info?.customAssertion?.type || assertion?.info?.type || 'Unknown';
                summary[assertionType] = (summary[assertionType] || 0) + 1;
            });
            const group: AssertionStatusGroup = {
                name: status,
                assertions: mapAssertionData(filteredAssertions),
                summary,
            };
            assertionGroup.push({ ...group, groupName: getGroupNameBySummary(group) });
        }
    });

    return assertionGroup;
};

/** return assertions table data structure to render on table from assertions */
export const transformAssertionData = (assertions: AssertionWithMonitorDetails[]): AssertionTable => {
    const assertionRawData: AssertionTable = { assertions: [], groupBy: { type: [], status: [] } };

    const assertionsTableData = mapAssertionData(assertions);
    assertionRawData.assertions = assertionsTableData;
    assertionRawData.groupBy.type = createAssertionGroups(assertions);
    assertionRawData.groupBy.status = generateAssertionGroupByStatus(assertions);
    return assertionRawData;
};

// Build the Filter Options as per the type & status
const buildFilterOptions = (key: string, value: Record<string, number>, filterOptions: AssertionFilterOptions) => {
    Object.entries(value).forEach(([name, count]) => {
        const displayName = key === 'type' ? getAssertionGroupName(name) : STATUS_GROUP_NAME_MAP[name] || name;
        const filterItem = { name, category: key, count, displayName } as AssertionRecommendedFilter;

        filterOptions.recommendedFilters.push(filterItem);
        filterOptions.filterGroupOptions[key].push(filterItem);
    });
};

/** Create filter option list as per the assertion data present 
 * for example
 * status :[
 * 
  {
    name: "SUCCESS",
    category: 'status',
    count:10,
    displayName: "Passing"
  }
 * ]
 * 
 * 
*/
const extractFilterOptionListFromAssertions = (assertions: AssertionWithMonitorDetails[]) => {
    const filterOptions: AssertionFilterOptions = {
        filterGroupOptions: {
            type: [],
            status: [],
            column: [],
            tags: [],
        },
        recommendedFilters: [],
    };

    const filterGroupCounts = {
        type: {} as Record<string, number>,
        status: {} as Record<string, number>,
        column: {} as Record<string, number>,
        tags: {} as Record<string, number>,
    };

    const others: Record<string, number> = {};

    assertions.forEach((assertion: AssertionWithMonitorDetails) => {
        const type = assertion.info?.type || '';
        filterGroupCounts.type[type] = (filterGroupCounts.type[type] || 0) + 1;

        const mostRecentRun = assertion.runEvents?.runEvents?.[0];
        const resultType = mostRecentRun?.result?.type || '';
        if (resultType) {
            filterGroupCounts.status[resultType] = (filterGroupCounts.status[resultType] || 0) + 1;
        }

        const tags = assertion.tags?.tags || [];
        tags.forEach((tag) => {
            const tagName = tag.tag.properties?.name || '';
            filterGroupCounts.tags[tagName] = (filterGroupCounts.tags[tagName] || 0) + 1;
        });

        switch (assertion.info?.source?.type) {
            case AssertionSourceType.Inferred:
                others[AssertionSourceType.Inferred] = (others[AssertionSourceType.Inferred] || 0) + 1;
                break;
            case AssertionSourceType.Native:
                others[AssertionSourceType.Native] = (others[AssertionSourceType.Native] || 0) + 1;
                break;
            default:
                if (isExternalAssertion(assertion)) {
                    others[AssertionSourceType.External] = (others[AssertionSourceType.External] || 0) + 1;
                }
                break;
        }
    });

    buildFilterOptions('type', filterGroupCounts.type, filterOptions);
    buildFilterOptions('status', filterGroupCounts.status, filterOptions);

    Object.entries(others).forEach(([name, count]) => {
        filterOptions.recommendedFilters.push({
            name,
            category: 'others',
            count,
            displayName: RECOMMENDED_FILTER_NAME_MAP[name] || name,
        });
    });

    return filterOptions;
};

// Assign Filtered Assertions to group
const assignFilteredAssertionToGroup = (filteredAssertions: AssertionWithDescription[]): AssertionTable => {
    const assertionRawData: AssertionTable = {
        assertions: [],
        groupBy: { type: [], status: [] },
        filterOptions: {},
    };
    assertionRawData.assertions = mapAssertionData(filteredAssertions);
    const assertionsByType = createAssertionGroups(filteredAssertions);
    assertionRawData.groupBy.type = assertionsByType;
    (assertionRawData.groupBy.type || []).forEach((item) => {
        const transformedData = mapAssertionData(item.assertions);
        // eslint-disable-next-line  no-param-reassign
        item.assertions = transformedData;
        // eslint-disable-next-line  no-param-reassign
        item.groupName = <AssertionGroupHeader group={item} />;
    });
    assertionRawData.groupBy.status = generateAssertionGroupByStatus(filteredAssertions);
    assertionRawData.filterOptions = extractFilterOptionListFromAssertions(filteredAssertions);
    return assertionRawData;
};

const getFilteredAssertions = (assertions: AssertionWithDescription[], filter: AssertionListFilter) => {
    const { type, status, others } = filter.filterCriteria;

    // Apply type, status, and other filters
    return assertions.filter((assertion: AssertionWithMonitorDetails) => {
        const resultType = assertion.runEvents?.runEvents?.[0]?.result?.type as AssertionResultType;

        const matchesType = type.length === 0 || type.includes(assertion.info?.type as AssertionType);
        const matchesStatus = status.length === 0 || status.includes(resultType);
        const matchesOthers =
            others.length === 0 ||
            others.includes(assertion.info?.source?.type as AssertionSourceType) ||
            (others.includes(AssertionSourceType.External) && isExternalAssertion(assertion));

        return matchesType && matchesStatus && matchesOthers;
    });
};

// Fuse.js setup for search functionality
const fuse = new Fuse<AssertionWithDescription>([], {
    keys: ['description'],
    threshold: 0.4,
});

/** Return return filter assertion as per selected type status and other things
 * it returns transformated into
 * 1. group of assertions as per type , status
 * 2. Transform data into {@link AssertionListTableRow }  data
 * 2. Filter out assertions as per the search text
 * 3. filter out assertions as per the selected type and status
 */
export const getFilteredTransformedAssertionData = (
    assertions: AssertionWithMonitorDetails[],
    filter: AssertionListFilter,
): AssertionTable => {
    // Add descriptions to assertions
    const assertionsWithDescription = assertions.map((assertion) => {
        const monitor = assertion.monitor?.relationships?.[0]?.entity;
        const description = getPlainTextDescriptionFromAssertion(assertion.info as AssertionInfo, monitor);
        return {
            ...assertion,
            description,
        };
    });

    // Apply search filter if searchText is provided
    let filteredAssertions = assertionsWithDescription;
    const { searchText } = filter.filterCriteria;

    if (searchText) {
        fuse.setCollection(assertionsWithDescription || []);
        const result = fuse.search(searchText);
        filteredAssertions = result.map((match) => match.item as AssertionWithDescription);
    }

    // Apply type, status, and other filters
    filteredAssertions = getFilteredAssertions(filteredAssertions, filter);

    // Transform filtered assertions
    const assertionRawData = assignFilteredAssertionToGroup(filteredAssertions);
    return assertionRawData;
};

/** Build the Assertion Redirect Search Param URL to help add with location pathname for redirection */
export const buildAssertionUrlSearch = ({
    type,
    status,
}: {
    type?: AssertionType;
    status?: AssertionResultType;
}): string => {
    const search = window.location.search;
    const params = new URLSearchParams(search);

    if (type) {
        params.set('assertion_type', type);
    }
    if (status) {
        params.set('assertion_status', status);
    }

    return params.toString() ? `?${params.toString()}` : '';
};
