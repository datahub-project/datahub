import React from 'react';

import { Typography } from 'antd';
import { Tooltip } from '@components';
import { Maybe } from 'graphql/jsutils/Maybe';
import {
    AssertionInfo,
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionType,
    CronSchedule,
    DatasetAssertionInfo,
    DatasetAssertionScope,
    EntityType,
    FieldAssertionInfo,
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
import { decodeSchemaField } from '@src/app/lineage/utils/columnLineageUtils';
import { DatasetAssertionDescription } from '../../../DatasetAssertionDescription';
import {
    createCronText,
    createFixedIntervalText,
    createSinceTheLastCheckText,
    FreshnessAssertionDescription,
} from '../../../FreshnessAssertionDescription';
import { VolumeAssertionDescription } from '../../../VolumeAssertionDescription';
import { SqlAssertionDescription } from '../../../SqlAssertionDescription';
import { FieldAssertionDescription } from '../../../FieldAssertionDescription';
import { SchemaAssertionDescription } from '../../../SchemaAssertionDescription';
import { useEntityRegistry } from '../../../../../../../../useEntityRegistry';
import { useGetUserQuery } from '../../../../../../../../../graphql/user.generated';
import { ANTD_GRAY_V2 } from '../../../../../../constants';
import { getFormattedParameterValue } from '../../../assertionUtils';
import {
    getIsRowCountChange,
    getOperatorDescription,
    getParameterDescription,
    getValueChangeTypeDescription,
    getVolumeTypeDescription,
    getVolumeTypeInfo,
} from '../../../utils';
import {
    getFieldDescription,
    getFieldOperatorDescription,
    getFieldParametersDescription,
    getFieldTransformDescription,
} from '../../../fieldDescriptionUtils';

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
    const transform = getFieldTransformDescription(assertionInfo);
    // Do not pluralize if this is a metric assertion since you're checking one metric, not multiple values
    const operator = getFieldOperatorDescription({ assertionInfo, isPlural: !transform });
    const parameters = getFieldParametersDescription(assertionInfo);
    return `${transform} ${transform ? ' of ' : ''}${field} ${
        (transform && 'column') || 'Values'
    } ${operator} ${parameters}`;
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

/**
 * Returns a text element describing the given assertion
 * If the assertion has a user-defined assertion, it'll prioritize displaying that.
 * Else it'll infer a description.
 * @IMPORTANT if you modify this, also modify {@link #getPlainTextDescriptionFromAssertion()} below
 * @param assertionInfo
 * @param monitorSchedule
 * @returns {JSX.Element}
 */
const useBuildPrimaryLabel = (
    assertionInfo?: Maybe<AssertionInfo>,
    monitorSchedule?: Maybe<CronSchedule>,
    options?: { showColumnTag?: boolean },
): JSX.Element => {
    let primaryLabel = <Typography.Text>No description found</Typography.Text>;
    if (assertionInfo?.description && assertionInfo?.type !== AssertionType.Field) {
        primaryLabel = <Typography.Text>{assertionInfo.description}</Typography.Text>;
    } else {
        switch (assertionInfo?.type) {
            case AssertionType.Dataset:
                primaryLabel = (
                    <DatasetAssertionDescription
                        assertionInfo={assertionInfo.datasetAssertion as DatasetAssertionInfo}
                    />
                );
                break;
            case AssertionType.Freshness:
                primaryLabel = (
                    <FreshnessAssertionDescription
                        assertionInfo={assertionInfo.freshnessAssertion as FreshnessAssertionInfo}
                        monitorSchedule={monitorSchedule}
                    />
                );
                break;
            case AssertionType.Volume:
                primaryLabel = (
                    <VolumeAssertionDescription assertionInfo={assertionInfo.volumeAssertion as VolumeAssertionInfo} />
                );
                break;
            case AssertionType.Sql:
                primaryLabel = <SqlAssertionDescription assertionInfo={assertionInfo} />;
                break;
            case AssertionType.Field:
                primaryLabel = (
                    <FieldAssertionDescription
                        assertionDescription={assertionInfo?.description}
                        assertionInfo={assertionInfo.fieldAssertion as FieldAssertionInfo}
                        showColumnTag={options?.showColumnTag}
                    />
                );
                break;
            case AssertionType.DataSchema:
                primaryLabel = (
                    <SchemaAssertionDescription assertionInfo={assertionInfo.schemaAssertion as SchemaAssertionInfo} />
                );
                break;
            default:
                break;
        }
    }
    return primaryLabel;
};

/**
 * Builds secondary label component containing information about who created and last updated the assertion
 * Has a tooltip on hover with richer context
 * @param assertionInfo
 * @returns {JSX.Element} if sufficient data is present
 */
const useBuildSecondaryLabel = (assertionInfo?: Maybe<AssertionInfo>): JSX.Element | null => {
    const entityRegistry = useEntityRegistry();

    // 1. Fetching the most recent actor data.
    const creatorActorUrn = assertionInfo?.source?.created?.actor;
    const updatedActorUrn = assertionInfo?.lastUpdated?.actor;
    const { data: createdActor } = useGetUserQuery({
        variables: { urn: creatorActorUrn ?? '', groupsCount: 0 },
        fetchPolicy: 'cache-first',
        skip: !creatorActorUrn,
    });
    const { data: lastUpdatedActor } = useGetUserQuery({
        variables: { urn: updatedActorUrn ?? '', groupsCount: 0 },
        fetchPolicy: 'cache-first',
        skip: !updatedActorUrn,
    });

    // 2. Define the secondary label message and the richer tooltip messages
    let secondaryLabelMessage = '';
    let tooltipCreatedByMessage = '';
    let tooltipLastUpdatedByMessage = '';

    // 2.1 Creator info if available
    if (createdActor?.corpUser && entityRegistry.getDisplayName(EntityType.CorpUser, createdActor.corpUser)) {
        secondaryLabelMessage = `Created by ${entityRegistry.getDisplayName(
            EntityType.CorpUser,
            createdActor.corpUser,
        )}`;

        tooltipCreatedByMessage = `Created by: ${entityRegistry.getDisplayName(
            EntityType.CorpUser,
            createdActor.corpUser,
        )}${createdActor.corpUser?.info?.email ? ` (${createdActor.corpUser.info.email})` : ''} on ${
            assertionInfo?.source?.created?.time
                ? new Date(assertionInfo.source.created.time).toLocaleString()
                : 'an unknown time' // should never get here
        }.`;
    }
    // 2.2 Last updated info if available
    if (lastUpdatedActor?.corpUser && entityRegistry.getDisplayName(EntityType.CorpUser, lastUpdatedActor.corpUser)) {
        // Special handling if creator is last updater
        if (updatedActorUrn === creatorActorUrn) {
            const prefix =
                assertionInfo?.source?.created?.time === assertionInfo?.lastUpdated?.time
                    ? `Created by`
                    : `Last updated by`;
            secondaryLabelMessage = `${prefix} ${entityRegistry.getDisplayName(
                EntityType.CorpUser,
                lastUpdatedActor.corpUser,
            )}`;
        } else {
            secondaryLabelMessage = `Last updated by ${entityRegistry.getDisplayName(
                EntityType.CorpUser,
                lastUpdatedActor.corpUser,
            )}`;
        }

        // Show tooltip label for last updated if either the updater != creator OR if updatedTime != createdTime
        if (
            updatedActorUrn !== creatorActorUrn ||
            assertionInfo?.lastUpdated?.time !== assertionInfo?.source?.created?.time
        ) {
            tooltipLastUpdatedByMessage = `Last updated by ${entityRegistry.getDisplayName(
                EntityType.CorpUser,
                lastUpdatedActor.corpUser,
            )}${lastUpdatedActor.corpUser?.info?.email ? ` (${lastUpdatedActor.corpUser.info.email})` : ''} on ${
                assertionInfo?.lastUpdated?.time
                    ? new Date(assertionInfo.lastUpdated.time).toLocaleString()
                    : 'an unknown time' // should never get here
            }.`;
        }
    }

    // 3. Construct the secondary label component if sufficient data exists
    return secondaryLabelMessage ? (
        <Tooltip
            title={
                <>
                    {tooltipCreatedByMessage ? [tooltipCreatedByMessage, <br />] : null}
                    {tooltipLastUpdatedByMessage}
                </>
            }
        >
            <Typography.Text style={{ color: ANTD_GRAY_V2['6'], fontSize: 12 }}>
                {secondaryLabelMessage}
            </Typography.Text>
        </Tooltip>
    ) : null;
};

/**
 * @IMPORTANT if you modify this, also modify {@link #getPlainTextDescriptionFromAssertion()} below
 */
export const useBuildAssertionDescriptionLabels = (
    assertionInfo?: Maybe<AssertionInfo>,
    monitorSchedule?: Maybe<CronSchedule>,
    options?: { showColumnTag?: boolean },
): {
    primaryLabel: JSX.Element;
    secondaryLabel: JSX.Element | null;
} => {
    // ------- Primary label with assertion description ------ //
    // IMPORTANT: if you modify this, also modify {@link #getPlainTextDescriptionFromAssertion} below
    const primaryLabel = useBuildPrimaryLabel(assertionInfo, monitorSchedule, options);

    // ----------- Try displaying secondary label showing creator/updater context ------------ //
    const secondaryLabel = useBuildSecondaryLabel(assertionInfo);

    return {
        primaryLabel,
        secondaryLabel,
    };
};

/**
 * Similar to {@link #useBuildPrimaryLabel}, but returns plaintext instead of jsx.
 * Primarily used for building the search index!
 */
export const getPlainTextDescriptionFromAssertion = (assertionInfo?: AssertionInfo): string => {
    // if description is present don't generate dynamic description
    if (assertionInfo?.description) {
        return assertionInfo.description;
    }

    return assertionInfo
        ? getDatasetAssertionPlainTextDescription(assertionInfo.datasetAssertion as DatasetAssertionInfo)
        : '';
};
