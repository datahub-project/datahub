import { Tooltip } from '@components';
import { Typography } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import i18next from 'i18next';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';

import {
    DatasetAssertionDescription,
    getAggregationDescriptor,
    getOperatorKey,
} from '@app/entityV2/shared/tabs/Dataset/Validations/DatasetAssertionDescription';
import { FieldAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/FieldAssertionDescription';
import { FreshnessAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/FreshnessAssertionDescription';
import { SchemaAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/SchemaAssertionDescription';
import { SqlAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/SqlAssertionDescription';
import { VolumeAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/VolumeAssertionDescription';
import {
    getCustomAssertionFields,
    hasStructuredAssertionDescriptionFields,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/shared/structuredAssertionUtils';
import { getFormattedParameterValue } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import {
    getFieldDescription,
    getFieldDescriptionDescriptor,
} from '@app/entityV2/shared/tabs/Dataset/Validations/fieldDescriptionUtils';
import {
    getIsRowCountChange,
    getParameterDescription,
    getParameterInterpolation,
    getVolumeOperatorKeyPart,
    getVolumeTypeInfo,
} from '@app/entityV2/shared/tabs/Dataset/Validations/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import {
    AssertionInfo,
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionType,
    AssertionValueChangeType,
    CronSchedule,
    DatasetAssertionScope,
    EntityType,
    FieldAssertionInfo,
    FreshnessAssertionInfo,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    Maybe as GeneratedMaybe,
    IncrementingSegmentRowCountChange,
    RowCountChange,
    SchemaAssertionCompatibility,
    SchemaAssertionInfo,
    SchemaFieldRef,
    StringMapEntry,
    VolumeAssertionInfo,
} from '@src/types.generated';
import { cronToString, removeTimePrefix } from '@utils/cronstrue';

import { useGetUserQuery } from '@graphql/user.generated';

type StructuredDescriptionFields = {
    scope?: GeneratedMaybe<DatasetAssertionScope>;
    aggregation?: GeneratedMaybe<AssertionStdAggregation>;
    operator?: GeneratedMaybe<AssertionStdOperator>;
    fields?: GeneratedMaybe<Array<SchemaFieldRef>>;
    parameters?: GeneratedMaybe<AssertionStdParameters>;
    nativeType?: GeneratedMaybe<string>;
    nativeParameters?: GeneratedMaybe<Array<StringMapEntry>>;
    logic?: GeneratedMaybe<string>;
};

const getStructuredAssertionPlainTextDescription = (fields: StructuredDescriptionFields): string => {
    const { scope, aggregation, operator, parameters, nativeType } = fields;
    const agg = getAggregationDescriptor(scope, aggregation, fields.fields);
    const operatorKey = getOperatorKey(operator || undefined);
    return i18next
        .t(`entity.profile.validations:datasetDescription.${agg.key}.${operatorKey}`, {
            column: agg.column ?? '',
            columns: agg.columns ?? '',
            value: getFormattedParameterValue(parameters?.value),
            minValue: getFormattedParameterValue(parameters?.minValue),
            maxValue: getFormattedParameterValue(parameters?.maxValue),
            nativeType: nativeType ?? '',
            operator: operator ?? '',
        })
        .replace(/<\/?bold>/g, '');
};

const getVolumeAssertionPlainTextDescription = (assertionInfo: VolumeAssertionInfo): string => {
    const volumeType = assertionInfo.type;
    const volumeTypeInfo = getVolumeTypeInfo(assertionInfo);
    const isChange = getIsRowCountChange(volumeType);
    const parameterDescription = volumeTypeInfo ? getParameterDescription(volumeTypeInfo.parameters) : undefined;
    const interpolation = getParameterInterpolation(parameterDescription);

    const operatorKeyPart = volumeTypeInfo ? getVolumeOperatorKeyPart(volumeTypeInfo.operator) : null;
    if (!operatorKeyPart) {
        return i18next.t('entity.profile.validations:volumeDescription.unknown');
    }

    let key: string;
    if (isChange) {
        const isPercentage =
            (volumeTypeInfo as RowCountChange | IncrementingSegmentRowCountChange).type ===
            AssertionValueChangeType.Percentage;
        key = `volumeDescription.change${operatorKeyPart}${isPercentage ? 'Percent' : 'Rows'}`;
    } else {
        key = `volumeDescription.total${operatorKeyPart}`;
    }

    return i18next.t(`entity.profile.validations:${key}`, interpolation);
};

const getFieldAssertionPlainTextDescription = (assertionInfo: FieldAssertionInfo): string => {
    const field = getFieldDescription(assertionInfo);
    try {
        const descriptor = getFieldDescriptionDescriptor(assertionInfo);
        return i18next
            .t(`entity.profile.validations:fieldDescription.${descriptor.shape}.${descriptor.operatorKey}`, {
                field: descriptor.field,
                transform: descriptor.transformLabelKey
                    ? i18next.t(`entity.profile.validations:${descriptor.transformLabelKey}`)
                    : '',
                metric: descriptor.metricLabelKey
                    ? i18next.t(`entity.profile.validations:${descriptor.metricLabelKey}`)
                    : '',
                value: descriptor.tokens.value,
                minValue: descriptor.tokens.minValue,
                maxValue: descriptor.tokens.maxValue,
            })
            .replace(/<\/?bold>/g, '');
    } catch (e) {
        // Helpers throw on unsupported enum values (e.g. operator = _NATIVE_ from external
        // integrations). Fall back to a generic description rather than breaking the search path.
        console.warn('Failed to render field assertion plain text description', e);
        return i18next
            .t('entity.profile.validations:fieldDescription.customCheckOn', {
                field: field ?? i18next.t('entity.profile.validations:fieldDescription.fieldFallback'),
            })
            .replace(/<\/?bold>/g, '');
    }
};

const getSchemaAssertionPlainTextDescription = (assertionInfo: SchemaAssertionInfo) => {
    const { compatibility } = assertionInfo;
    const isExactMatch = compatibility === SchemaAssertionCompatibility.ExactMatch;
    const expectedColumnCount = assertionInfo?.fields?.length || 0;
    return i18next.t(
        `entity.profile.validations:${isExactMatch ? 'schemaDescription.exactMatch' : 'schemaDescription.include'}`,
        { count: expectedColumnCount },
    );
};

const getFreshnessAssertionPlainTextDescription = (assertionInfo: FreshnessAssertionInfo) => {
    const scheduleType = assertionInfo.schedule?.type;
    const freshnessType = assertionInfo.type;
    const prefix = freshnessType === FreshnessAssertionType.DatasetChange ? 'datasetChange' : 'dataTask';

    const getCronLabel = (cron: CronSchedule) => {
        const { cron: cronExpr, timezone } = cron;
        if (!cronExpr) return '';
        return `${removeTimePrefix(cronToString(cronExpr).toLocaleLowerCase())} (${timezone})`;
    };

    switch (scheduleType) {
        case FreshnessAssertionScheduleType.FixedInterval: {
            const fixedInterval = assertionInfo.schedule?.fixedInterval;
            if (!fixedInterval) {
                return i18next.t(`entity.profile.validations:freshnessDescription.${prefix}.noInterval`);
            }
            return i18next.t(`entity.profile.validations:freshnessDescription.${prefix}.fixedInterval`, {
                multiple: fixedInterval.multiple,
                unit: `${fixedInterval.unit.toLocaleLowerCase()}s`,
            });
        }
        case FreshnessAssertionScheduleType.Cron:
            return i18next.t(`entity.profile.validations:freshnessDescription.${prefix}.cron`, {
                schedule: getCronLabel(assertionInfo.schedule?.cron as CronSchedule),
            });
        case FreshnessAssertionScheduleType.SinceTheLastCheck:
            return i18next.t(`entity.profile.validations:freshnessDescription.${prefix}.sinceLastCheck`);
        default:
            return i18next.t(`entity.profile.validations:freshnessDescription.${prefix}.unknown`);
    }
};

/**
 * Returns a text element describing the given assertion
 * If the assertion has a user-defined assertion, it'll prioritize displaying that.
 * Else it'll infer a description.
 * @IMPORTANT if you modify this, also modify {@link #getPlainTextDescriptionFromAssertion()} below
 * @param assertionInfo
 * @returns {JSX.Element}
 */
export const useBuildAssertionPrimaryLabel = (
    assertionInfo?: Maybe<AssertionInfo>,
    options?: { showColumnTag?: boolean },
): JSX.Element => {
    const { t } = useTranslation('entity.profile.validations');
    let primaryLabel = <Typography.Text>{t('datasetDescription.fallback.noDescription')}</Typography.Text>;
    if (assertionInfo?.description && assertionInfo?.type !== AssertionType.Field) {
        primaryLabel = <Typography.Text>{assertionInfo.description}</Typography.Text>;
    } else {
        switch (assertionInfo?.type) {
            case AssertionType.Dataset:
                primaryLabel = (
                    <DatasetAssertionDescription
                        scope={assertionInfo.datasetAssertion?.scope}
                        aggregation={assertionInfo.datasetAssertion?.aggregation}
                        operator={assertionInfo.datasetAssertion?.operator}
                        fields={assertionInfo.datasetAssertion?.fields}
                        parameters={assertionInfo.datasetAssertion?.parameters}
                        nativeType={assertionInfo.datasetAssertion?.nativeType}
                        nativeParameters={assertionInfo.datasetAssertion?.nativeParameters}
                        logic={assertionInfo.datasetAssertion?.logic}
                    />
                );
                break;
            case AssertionType.Freshness:
                primaryLabel = (
                    <FreshnessAssertionDescription
                        assertionInfo={assertionInfo.freshnessAssertion as FreshnessAssertionInfo}
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
            case AssertionType.Custom: {
                const custom = assertionInfo.customAssertion;
                if (
                    custom &&
                    hasStructuredAssertionDescriptionFields({
                        scope: custom.scope,
                        operator: custom.operator,
                        aggregation: custom.aggregation,
                        nativeType: custom.nativeType,
                    })
                ) {
                    primaryLabel = (
                        <DatasetAssertionDescription
                            scope={custom.scope}
                            aggregation={custom.aggregation}
                            operator={custom.operator}
                            fields={getCustomAssertionFields(custom)}
                            parameters={custom.parameters}
                            nativeType={custom.nativeType}
                            nativeParameters={custom.nativeParameters}
                            logic={custom.logic}
                        />
                    );
                } else if (custom?.type) {
                    primaryLabel = <Typography.Text>{custom.type}</Typography.Text>;
                }
                break;
            }
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
    const theme = useTheme();
    const { t } = useTranslation('entity.profile.validations');
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
        const creator = entityRegistry.getDisplayName(EntityType.CorpUser, createdActor.corpUser);
        secondaryLabelMessage = t('datasetDescription.label.createdByTemplate', { creator });

        const extra = createdActor.corpUser?.info?.email ? ` (${createdActor.corpUser.info.email})` : '';
        const date = assertionInfo?.source?.created?.time
            ? new Date(assertionInfo.source.created.time).toLocaleString()
            : t('datasetDescription.tooltip.anUnknownTime');
        tooltipCreatedByMessage = t('datasetDescription.tooltip.createdBy', { creator, extra, date });
    }
    // 2.2 Last updated info if available
    if (lastUpdatedActor?.corpUser && entityRegistry.getDisplayName(EntityType.CorpUser, lastUpdatedActor.corpUser)) {
        const updater = entityRegistry.getDisplayName(EntityType.CorpUser, lastUpdatedActor.corpUser);
        // Special handling if creator is last updater
        if (updatedActorUrn === creatorActorUrn) {
            secondaryLabelMessage =
                assertionInfo?.source?.created?.time === assertionInfo?.lastUpdated?.time
                    ? t('datasetDescription.label.createdByTemplate', { creator: updater })
                    : t('datasetDescription.label.lastUpdatedByTemplate', { updater });
        } else {
            secondaryLabelMessage = t('datasetDescription.label.lastUpdatedByTemplate', { updater });
        }

        // Show tooltip label for last updated if either the updater != creator OR if updatedTime != createdTime
        if (
            updatedActorUrn !== creatorActorUrn ||
            assertionInfo?.lastUpdated?.time !== assertionInfo?.source?.created?.time
        ) {
            const extra = lastUpdatedActor.corpUser?.info?.email ? ` (${lastUpdatedActor.corpUser.info.email})` : '';
            const date = assertionInfo?.lastUpdated?.time
                ? new Date(assertionInfo.lastUpdated.time).toLocaleString()
                : t('datasetDescription.tooltip.anUnknownTime');
            tooltipLastUpdatedByMessage = t('datasetDescription.tooltip.lastUpdatedBy', { updater, extra, date });
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
            <Typography.Text style={{ color: theme.colors.textDisabled, fontSize: 12 }}>
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
    options?: { showColumnTag?: boolean },
): {
    primaryLabel: JSX.Element;
    secondaryLabel: JSX.Element | null;
} => {
    // ------- Primary label with assertion description ------ //
    // IMPORTANT: if you modify this, also modify {@link #getPlainTextDescriptionFromAssertion} below
    const primaryLabel = useBuildAssertionPrimaryLabel(assertionInfo, options);

    // ----------- Try displaying secondary label showing creator/updater context ------------ //
    const secondaryLabel = useBuildSecondaryLabel(assertionInfo);

    return {
        primaryLabel,
        secondaryLabel,
    };
};

/**
 * Similar to {@link #useBuildAssertionPrimaryLabel}, but returns plaintext instead of jsx.
 * Primarily used for building the search index!
 */
export const getPlainTextDescriptionFromAssertion = (assertionInfo?: AssertionInfo): string => {
    // if description is present don't generate dynamic description
    if (assertionInfo?.description) {
        return assertionInfo.description;
    }

    let primaryLabel = '';
    switch (assertionInfo?.type) {
        case AssertionType.Dataset:
            primaryLabel = getStructuredAssertionPlainTextDescription({
                scope: assertionInfo.datasetAssertion?.scope,
                aggregation: assertionInfo.datasetAssertion?.aggregation,
                operator: assertionInfo.datasetAssertion?.operator,
                fields: assertionInfo.datasetAssertion?.fields,
                parameters: assertionInfo.datasetAssertion?.parameters,
                nativeType: assertionInfo.datasetAssertion?.nativeType,
            });
            break;
        case AssertionType.Freshness:
            primaryLabel = getFreshnessAssertionPlainTextDescription(
                assertionInfo.freshnessAssertion as FreshnessAssertionInfo,
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
        case AssertionType.Custom: {
            const custom = assertionInfo.customAssertion;
            if (
                custom &&
                hasStructuredAssertionDescriptionFields({
                    scope: custom.scope,
                    operator: custom.operator,
                    aggregation: custom.aggregation,
                    nativeType: custom.nativeType,
                })
            ) {
                primaryLabel = getStructuredAssertionPlainTextDescription({
                    scope: custom.scope,
                    aggregation: custom.aggregation,
                    operator: custom.operator,
                    fields: getCustomAssertionFields(custom),
                    parameters: custom.parameters,
                    nativeType: custom.nativeType,
                });
            } else {
                primaryLabel = custom?.type || '';
            }
            break;
        }
        default:
            break;
    }
    return primaryLabel;
};
