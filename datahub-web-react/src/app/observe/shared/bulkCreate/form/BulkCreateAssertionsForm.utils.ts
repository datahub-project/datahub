import { DEFAULT_SMART_ASSERTION_SENSITIVITY } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/InferenceSensitivityAdjuster';
import { DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/LookBackWindowAdjuster';
import {
    AssertionMonitorBuilderState,
    FreshnessAssertionScheduleBuilderTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { BulkCreateDatasetAssertionsSpec, FreshnessCriteria } from '@app/observe/shared/bulkCreate/constants';
import { DEFAULT_CRON_SCHEDULE } from '@app/observe/shared/bulkCreate/form/BulkCreateAssertionsForm.constants';
import { FreshnessFormState, SubscriptionsFormState, VolumeFormState } from '@app/observe/shared/bulkCreate/form/types';
import { OperatorId } from '@app/tests/builder/steps/definition/builder/property/types/operators';
import { LogicalOperatorType, LogicalPredicate } from '@app/tests/builder/steps/definition/builder/types';

import {
    AssertionExclusionWindowInput,
    DatasetFreshnessAssertionParametersInput,
    DatasetVolumeAssertionParametersInput,
    DayOfWeek,
    EntityType,
    FreshnessAssertionScheduleType,
} from '@types';

/**
 * Maps exclusion windows from the builder state format to the API format.
 * This is a utility function to avoid duplicating the mapping logic across spec builders.
 * @internal visible for testing
 * @param exclusionWindows - The exclusion windows from the inference settings.
 * @returns The mapped exclusion windows for the API.
 */
export const mapExclusionWindows = (
    exclusionWindows?: NonNullable<AssertionMonitorBuilderState['inferenceSettings']>['exclusionWindows'],
): AssertionExclusionWindowInput[] => {
    return (
        exclusionWindows?.map((exclusionWindow) => ({
            type: exclusionWindow.type,
            displayName: exclusionWindow.displayName,
            fixedRange: exclusionWindow.fixedRange
                ? {
                      startTimeMillis: exclusionWindow.fixedRange.startTimeMillis,
                      endTimeMillis: exclusionWindow.fixedRange.endTimeMillis,
                  }
                : undefined,
            holiday: exclusionWindow.holiday
                ? {
                      name: exclusionWindow.holiday.name,
                      region: exclusionWindow.holiday.region,
                      timezone: exclusionWindow.holiday.timezone,
                  }
                : undefined,
            weekly: exclusionWindow.weekly
                ? {
                      daysOfWeek: exclusionWindow.weekly.daysOfWeek?.map((day) => day as DayOfWeek),
                      startTime: exclusionWindow.weekly.startTime,
                      endTime: exclusionWindow.weekly.endTime,
                      timezone: exclusionWindow.weekly.timezone,
                  }
                : undefined,
        })) ?? []
    );
};

/**
 * Validates and transforms the asset selector filters to ensure they are valid.
 * This is called in the onUpdate handler of the SimpleSelect component for the asset selector filters.
 * @param newFilters - The new filters to validate and transform.
 * @returns The transformed filters.
 */
export const validateAndTransformAssetSelectorFilters = (
    newFilters?: LogicalPredicate,
): LogicalPredicate | undefined => {
    if (!newFilters) {
        return undefined;
    }
    if (newFilters.operator !== LogicalOperatorType.AND) {
        throw new Error(
            'Top-level AND filter cannot be changed. Please click "Add Group" to support more complex filters.',
        );
    }
    const transformedFilters: LogicalPredicate = {
        type: 'logical',
        operator: LogicalOperatorType.AND,
        operands: [...(newFilters?.operands || [])],
    };
    const maybeDatasetFilter = newFilters?.operands?.find(
        (operand) => operand.type === 'property' && operand.property === '_entityType',
    );
    if (
        !maybeDatasetFilter ||
        maybeDatasetFilter.type !== 'property' ||
        !maybeDatasetFilter.values?.includes(EntityType.Dataset)
    ) {
        throw new Error('Filter for Dataset entities cannot be changed or removed.');
    }

    const platformFilter = newFilters?.operands?.find(
        (operand) => operand.type === 'property' && operand.property === 'platform',
    );
    if (!platformFilter || platformFilter.operator !== OperatorId.EQUAL_TO) {
        throw new Error('Filter for a Platform is required.');
    }
    return transformedFilters;
};

/**
 * Builds the freshness assertion spec from the state of the form.
 * @internal visible for testing
 * @param freshnessFormState - The state of the freshness form.
 * @returns The freshness assertion spec.
 */
export const buildFreshnessAssertionSpec = (
    freshnessFormState: FreshnessFormState,
): BulkCreateDatasetAssertionsSpec['freshnessAssertionSpec'] => {
    const {
        freshnessAssertionEnabled,
        freshnessAssertionType,
        freshnessAssertionInterval,
        freshnessAssertionIntervalUnit,
        freshnessInferenceSettings,
        freshnessSchedule,
        freshnessActions,
        freshnessSourceType,
    } = freshnessFormState;
    let freshnessAssertionSpec: BulkCreateDatasetAssertionsSpec['freshnessAssertionSpec'];
    if (freshnessAssertionEnabled) {
        const evaluationParameters: DatasetFreshnessAssertionParametersInput = {
            sourceType: freshnessSourceType,
        };
        const criteria: FreshnessCriteria =
            freshnessAssertionType === FreshnessAssertionScheduleBuilderTypeOptions.AiInferred
                ? {
                      type: 'AI',
                      inferenceSettings: {
                          sensitivity: {
                              level:
                                  freshnessInferenceSettings?.sensitivity?.level ?? DEFAULT_SMART_ASSERTION_SENSITIVITY,
                          },
                          trainingDataLookbackWindowDays:
                              freshnessInferenceSettings?.trainingDataLookbackWindowDays ??
                              DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS,
                          exclusionWindows: mapExclusionWindows(freshnessInferenceSettings?.exclusionWindows),
                      },
                  }
                : {
                      type: 'MANUAL',
                      schedule:
                          freshnessAssertionType === FreshnessAssertionScheduleBuilderTypeOptions.FixedInterval
                              ? {
                                    type: FreshnessAssertionScheduleType.FixedInterval,
                                    fixedInterval: {
                                        unit: freshnessAssertionIntervalUnit,
                                        multiple: freshnessAssertionInterval,
                                    },
                                    cron: {
                                        cron: freshnessSchedule?.cron ?? DEFAULT_CRON_SCHEDULE.cron,
                                        timezone: freshnessSchedule?.timezone ?? DEFAULT_CRON_SCHEDULE.timezone,
                                    },
                                }
                              : {
                                    type: FreshnessAssertionScheduleType.SinceTheLastCheck,
                                    cron: {
                                        cron: freshnessSchedule?.cron ?? DEFAULT_CRON_SCHEDULE.cron,
                                        timezone: freshnessSchedule?.timezone ?? DEFAULT_CRON_SCHEDULE.timezone,
                                    },
                                },
                  };
        freshnessAssertionSpec = {
            criteria,
            evaluationParameters,
            actions: freshnessActions,
            evaluationSchedule: freshnessSchedule
                ? {
                      cron: freshnessSchedule.cron,
                      timezone: freshnessSchedule.timezone,
                  }
                : DEFAULT_CRON_SCHEDULE,
        };
    }

    return freshnessAssertionSpec;
};

/**
 * Builds the volume assertion spec from the state of the form.
 * @internal visible for testing
 * @param volumeFormState - The state of the volume form.
 * @returns The volume assertion spec.
 */
export const buildVolumeAssertionSpec = (
    volumeFormState: VolumeFormState,
): BulkCreateDatasetAssertionsSpec['volumeAssertionSpec'] => {
    const { volumeAssertionEnabled, volumeInferenceSettings, volumeSchedule, volumeActions, volumeSourceType } =
        volumeFormState;
    let volumeAssertionSpec: BulkCreateDatasetAssertionsSpec['volumeAssertionSpec'];
    if (volumeAssertionEnabled) {
        const evaluationParameters: DatasetVolumeAssertionParametersInput = {
            sourceType: volumeSourceType,
        };
        volumeAssertionSpec = {
            evaluationParameters,
            inferenceSettings: {
                sensitivity: {
                    level: volumeInferenceSettings?.sensitivity?.level ?? DEFAULT_SMART_ASSERTION_SENSITIVITY,
                },
                trainingDataLookbackWindowDays:
                    volumeInferenceSettings?.trainingDataLookbackWindowDays ??
                    DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS,
                exclusionWindows: mapExclusionWindows(volumeInferenceSettings?.exclusionWindows),
            },
            actions: volumeActions,
            evaluationSchedule: volumeSchedule
                ? {
                      cron: volumeSchedule.cron,
                      timezone: volumeSchedule.timezone,
                  }
                : DEFAULT_CRON_SCHEDULE,
        };
    }

    return volumeAssertionSpec;
};

export const buildSubscriptionSpecs = (
    subscriptionFormState: SubscriptionsFormState,
): BulkCreateDatasetAssertionsSpec['subscriptionSpecs'] => {
    const {
        personalSubscriptionEnabled,
        personalEntityChangeTypes,
        personalUserUrn,
        groupSubscriptionEnabled,
        selectedGroups,
        groupEntityChangeTypes,
    } = subscriptionFormState;

    if (!personalSubscriptionEnabled && !groupSubscriptionEnabled) {
        return undefined;
    }

    const subscriptionSpecs: NonNullable<BulkCreateDatasetAssertionsSpec['subscriptionSpecs']> = [];

    // Add personal subscription if enabled
    if (personalSubscriptionEnabled && personalEntityChangeTypes.length > 0) {
        subscriptionSpecs.push({
            subscriberUrn: personalUserUrn,
            entityChangeTypes: personalEntityChangeTypes,
        });
    }

    // Add group subscriptions if enabled and groups are selected
    if (groupSubscriptionEnabled && selectedGroups.length > 0 && groupEntityChangeTypes.length > 0) {
        selectedGroups.forEach((groupUrn) => {
            subscriptionSpecs.push({
                subscriberUrn: groupUrn,
                entityChangeTypes: groupEntityChangeTypes,
            });
        });
    }

    return subscriptionSpecs.length > 0 ? subscriptionSpecs : undefined;
};

/**
 * Converts the state of the form into a BulkCreateDatasetAssertionsSpec.
 * This is called in the onCreateAssertions handler of the BulkCreateAssertionsForm component.
 * @param state - The state of the form.
 * @param currentUserUrn - The URN of the currently authenticated user.
 * @returns The BulkCreateDatasetAssertionsSpec.
 */
export const stateToBulkCreateDatasetAssertionsSpec = ({
    filters,
    freshnessFormState,
    volumeFormState,
    subscriptionFormState,
}: {
    filters: LogicalPredicate;
    freshnessFormState: FreshnessFormState;
    volumeFormState: VolumeFormState;
    subscriptionFormState: SubscriptionsFormState;
}): BulkCreateDatasetAssertionsSpec => {
    const freshnessAssertionSpec = buildFreshnessAssertionSpec(freshnessFormState);
    const volumeAssertionSpec = buildVolumeAssertionSpec(volumeFormState);
    const subscriptionSpecs = buildSubscriptionSpecs(subscriptionFormState);

    return {
        assetSelector: {
            filters,
        },
        freshnessAssertionSpec,
        volumeAssertionSpec,
        subscriptionSpecs,
    };
};
