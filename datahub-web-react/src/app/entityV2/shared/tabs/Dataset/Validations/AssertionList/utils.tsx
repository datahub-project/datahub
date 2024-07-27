import { decodeSchemaField } from '@src/app/lineage/utils/columnLineageUtils';
import cronstrue from 'cronstrue';

import {
    AssertionInfo,
    AssertionResultType,
    AssertionRunStatus,
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionType,
    CronSchedule,
    DatasetAssertionInfo,
    DatasetAssertionScope,
    FieldAssertionInfo,
    FixedIntervalSchedule,
    FreshnessAssertionInfo,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    IncrementingSegmentRowCountChange,
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
import { useBuildAssertionDescriptionLabels } from '../assertion/profile/summary/utils';

/**
 * Returns the React Component to render for the aggregation portion of the Assertion Description
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
            return ' Dataset columns JSON.stringify(fieldNames) are ';
        }
        default:
            console.error(`Unsupported schema aggregation assertion ${aggregation} provided.`);
            return 'Dataset columns are';
    }
};

/**
 * Returns the React Component to render for the aggregation portion of the Assertion Description
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
 * Returns the React Component to render for the aggregation portion of the Assertion Description
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
 * Returns the React Component to render for the aggregation portion of the Assertion Description
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
 * Returns the React Component to render for the operator portion of the Assertion Description
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

    //TODO need to verify whether this is needed or not
    // <div ref={labelRef}>
    //     <Typography.Text>
    //         Actual table columns {matchText} {expectedColumnCount} expected columns
    //     </Typography.Text>
    //     {showSchemaSummary && !!assertionInfo.schema && (
    //         <SchemaSummaryModal schema={assertionInfo.schema} onClose={() => setShowSchemaSummary(false)} />
    //     )}
    // </div>
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
            scheduleText = createCronText(assertionInfo.schedule?.cron as any);
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
            break;
        case AssertionType.DataSchema:
            primaryLabel = getSchemaAssertionPlainTextDescription(assertionInfo.schemaAssertion as SchemaAssertionInfo);
            break;
    }
    return primaryLabel;
};

// Generate Assertion
const getAssertionSummaryByStatus = (assertions, status) => {
    const typeToAssertions = assertions
        .filter((assertion) => assertion.info?.type)
        .reduce((map, assertion) => {
            const assertionType = assertion?.info?.customAssertion?.type || assertion?.info?.type || 'Unknown';
            map[assertionType] = (map[assertionType] || 0) + 1;
            return map;
        }, {});

    return {
        status,
        assertions,
        summary: typeToAssertions,
    };
};

// Generate Assertion Group By Status
const generateAssertionGroupByStatus = (assertions: AssertionWithMonitorDetails[]) => {
    const STATUSES = [AssertionResultType.Success, AssertionResultType.Failure];

    const assertionGroup: any[] = [];

    STATUSES.forEach((status) => {
        const filteredAssertions = assertions.filter((assertion) => {
            const mostRecentRun = assertion.runEvents?.runEvents?.[0];
            const resultType = mostRecentRun?.result?.type;
            return assertion.info?.type && resultType === status;
        });
        const typeToAssertions = filteredAssertions.reduce((map, assertion) => {
            const assertionType = assertion?.info?.customAssertion?.type || assertion?.info?.type || 'Unknown';
            map[assertionType] = (map[assertionType] || 0) + 1;
            return map;
        }, {});
        assertionGroup.push({
            name: status,
            assertions: filteredAssertions,
            summary: typeToAssertions,
        });
    });

    return assertionGroup;
};

/** return assertion table data from assertions */
export const transformAssertionData = (assertions: AssertionWithMonitorDetails[]) => {
    const assertionRawData: any = { allAssertions: [], groupBy: { type: [], status: [] } };

    const assertionsTableData = assertions.map((assertion) => {
        const mostRecentRun = assertion.runEvents?.runEvents?.[0];

        const monitor = assertion.monitor?.relationships?.[0]?.entity;
        const primaryPainTextLabel = getPlainTextDescriptionFromAssertion(assertion.info as AssertionInfo, monitor);

        return {
            type: assertion.info?.type,
            lastUpdated: assertion.info?.lastUpdated,
            tags: assertion.tags,
            descriptionHTML: null,
            description: primaryPainTextLabel,
            urn: assertion.urn,
            platform: assertion.platform,
            lastEvaluation: mostRecentRun?.status === AssertionRunStatus.Complete && mostRecentRun,
            lastEvaluationTimeMs: mostRecentRun?.timestampMillis,
            lastEvaluationResult: mostRecentRun?.status === AssertionRunStatus.Complete && mostRecentRun?.result?.type,
            lastEvaluationUrl:
                mostRecentRun?.status === AssertionRunStatus.Complete && mostRecentRun?.result?.externalUrl,
            assertion,
            monitor,
        };
    });

    assertionRawData.allAssertions = assertionsTableData;
    assertionRawData.groupBy.type = createAssertionGroups(assertions);
    assertionRawData.groupBy.status = generateAssertionGroupByStatus(assertions);

    console.log('assertionRawData>>', assertionRawData);

    return assertionsTableData;
};
