import React from 'react';
import { decodeSchemaField } from '@src/app/lineage/utils/columnLineageUtils';
import cronstrue from 'cronstrue';
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
    FixedIntervalSchedule,
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
import { AssertionWithMonitorDetails, createAssertionGroups } from '../acrylUtils';
import { AssertionGroupHeader } from './AssertionGroupHeader';
import { AssertionStatusGroup, AssertionTableType, IFilter, TableRowType } from './types';
import { UNKNOWN_DATA_PLATFORM } from './AssertionName';
import { isExternalAssertion } from '@src/app/entity/shared/tabs/Dataset/Validations/assertion/profile/shared/isExternalAssertion';

/**
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
 * Returns the Plain Text to render for the aggregation portion of the Assertion Description
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
 *
 * A human-readable Plain Text description of a Dataset Assertion.
 */

export const getDatasetAssertionPlainTextDescription = (datasetAssertion: DatasetAssertionInfo): string => {
    const { scope, aggregation, fields, operator, parameters, nativeType } = datasetAssertion;
    const aggregationPlainText = getAggregationPlainText(scope, aggregation, fields);
    const operatorPlainText = getOperatorPlainText(operator, parameters || undefined, nativeType || undefined);
    return `${aggregationPlainText} ${operatorPlainText}`;
};

/**
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
 * A human-readable Plain Text description of a Schema Assertion.
 */
export const getSchemaAssertionPlainTextDescription = (assertionInfo: SchemaAssertionInfo) => {
    const { compatibility } = assertionInfo;
    const matchText = compatibility === SchemaAssertionCompatibility.ExactMatch ? 'exactly match' : 'include';
    const expectedColumnCount = assertionInfo?.fields?.length || 0;
    return `Actual table columns ${matchText} ${expectedColumnCount} expected columns`;
};

/** below functions are related to Freshness */

const getCronAsLabel = (cronSchedule: CronSchedule) => {
    const { cron, timezone } = cronSchedule;
    if (!cron) {
        return '';
    }
    return `${cronstrue.toString(cron).toLocaleLowerCase().replace('at', '')} (${timezone})`;
};

const createCronText = (cronSchedule: CronSchedule) => {
    return `between cron windows scheduled at ${getCronAsLabel(cronSchedule)}`;
};

const createFixedIntervalText = (
    fixedIntervalSchedule?: FixedIntervalSchedule | null,
    monitorSchedule?: CronSchedule,
) => {
    if (!fixedIntervalSchedule) {
        return 'No interval found!';
    }
    const { multiple, unit } = fixedIntervalSchedule;
    const cronText = monitorSchedule ? `, as of ${getCronAsLabel(monitorSchedule)}` : '';
    return `in the past ${multiple} ${unit.toLocaleLowerCase()}s${cronText}`;
};

const createSinceTheLastCheckText = (monitorSchedule?: CronSchedule) => {
    const cronText = monitorSchedule ? `, as of ${getCronAsLabel(monitorSchedule)}` : '';
    return `since the previous check${cronText}.`;
};

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

    const NAME_MAP = {
        FAILURE: 'Failing',
        SUCCESS: 'Passing',
        ERROR: 'Error',
        VOLUME: 'Volume',
        SQL: 'Sql',
        FIELD: 'Field',
        FRESHNESS: 'Freshness',
        DATASET: 'Other',
    };
    const newSummary = record.summary;
    const list: string[] = [];
    Object.keys(newSummary).forEach((key) => {
        if (newSummary[key] > 0) {
            list.push(`${newSummary[key]} ${NAME_MAP[key]}`);
        }
    });

    return (
        <TextContainer>
            <Title strong>{NAME_MAP[record.name]}</Title>
            <Message type="secondary">{list.join(', ')}</Message>
        </TextContainer>
    );
};

// transform assertions into table data
const mapAssertionData = (assertions: AssertionWithMonitorDetails[] | Assertion[]): TableRowType[] => {
    return assertions.map((assertion: AssertionWithMonitorDetails) => {
        const mostRecentRun = assertion.runEvents?.runEvents?.[0];

        const monitor = assertion.monitor?.relationships?.[0]?.entity;
        const primaryPainTextLabel = getPlainTextDescriptionFromAssertion(assertion.info as AssertionInfo, monitor);
        const isCompleted = mostRecentRun?.status === AssertionRunStatus.Complete;
        const rowData: TableRowType = {
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
export const transformAssertionData = (assertions: AssertionWithMonitorDetails[]): AssertionTableType => {
    const assertionRawData: AssertionTableType = { assertions: [], groupBy: { type: [], status: [] } };

    const assertionsTableData = mapAssertionData(assertions);
    assertionRawData.assertions = assertionsTableData;
    assertionRawData.groupBy.type = createAssertionGroups(assertions);
    assertionRawData.groupBy.status = generateAssertionGroupByStatus(assertions);
    return assertionRawData;
};

/** create filter Option List */
const extractFilterOptionListFromAssertions = (assertions: AssertionWithMonitorDetails[]) => {
    const filterOptions: any = {
        filterGroupOptions: {
            type: [],
            status: [],
            column: [],
            tags: [],
        },
        groupByOptions: ['Type', 'Status'],
        recommendedFilters: [],
    };
    const filterGroupOptions = {
        type: {},
        status: {},
        column: {},
        tags: {},
    };
    const others: any = {};
    assertions.forEach((assertion: AssertionWithMonitorDetails) => {
        const type = assertion.info?.type || '';
        filterGroupOptions.type[type] = (filterGroupOptions.type[type] || 0) + 1;

        const mostRecentRun = assertion.runEvents?.runEvents?.[0];
        const resultType = mostRecentRun?.result?.type || '';
        if (resultType) {
            filterGroupOptions.status[resultType] = (filterGroupOptions.status[resultType] || 0) + 1;
        }
        const tags = assertion.tags?.tags || [];
        tags.forEach((tag) => {
            const name = tag.tag.properties?.name || '';
            filterGroupOptions.tags[name] = (filterGroupOptions.tags[name] || 0) + 1;
        });

        // TODO Need to check for Column Option List
        // const column = assertion.column || '';
        // filterGroupOptions.column[column] = (filterGroupOptions.column[column] || 0) + 1;

        const assertionInfo = assertion.info;
        const isSmartAssertion = assertionInfo?.source?.type === AssertionSourceType.Inferred;
        if (isSmartAssertion) {
            others.smartAssertions = (others.smartAssertions || 0) + 1;
        }
        const isExternal = isExternalAssertion(assertion);
        if (isExternal) {
            others.external = (others.external || 0) + 1;
        }
        const isNative = assertion?.info?.source?.type !== AssertionSourceType.Native;
        if (isNative) {
            others.native = (others.native || 0) + 1;
        }
    });

    for (const [key, value] of Object.entries(filterGroupOptions)) {
        if (['type', 'status'].includes(key)) {
            for (const [key2, value2] of Object.entries(value)) {
                filterOptions.recommendedFilters.push({ name: key2, category: key, count: value2 });
                if (filterOptions.filterGroupOptions[key]) {
                    filterOptions.filterGroupOptions[key].push({ name: key2, category: key, count: value2 });
                } else {
                    filterOptions.filterGroupOptions[key] = [{ name: key2, category: key, count: value2 }];
                }
            }
        }
    }
    for (const [key, value] of Object.entries(others)) {
        filterOptions.recommendedFilters.push({ name: key, category: 'others', count: value });
    }
    return filterOptions;
};

/** return fitlered transformed assertions */
export const getFilteredTransformedAssertionData = (
    assertions: AssertionWithMonitorDetails[],
    filter: IFilter,
): AssertionTableType => {
    const assertionRawData: AssertionTableType = {
        assertions: [],
        groupBy: { type: [], status: [] },
        filterOptions: {},
    };
    assertionRawData.filterOptions = extractFilterOptionListFromAssertions(assertions);

    const filteredAssertions = assertions.filter((assertion: AssertionWithMonitorDetails) => {
        const { searchText, type, status } = filter.filterCriteria;
        const mostRecentRun = assertion.runEvents?.runEvents?.[0];
        const monitor = assertion.monitor?.relationships?.[0]?.entity;
        const description = getPlainTextDescriptionFromAssertion(assertion.info as AssertionInfo, monitor);

        if (searchText && description.indexOf(searchText) === -1) {
            return false;
        }
        if (type.length > 0 && !type.includes(assertion.info?.type || '')) {
            return false;
        }
        if (status.length > 0 && !status.includes(mostRecentRun?.status || '')) {
            return false;
        }
        return true;
    });

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
    return assertionRawData;
};
