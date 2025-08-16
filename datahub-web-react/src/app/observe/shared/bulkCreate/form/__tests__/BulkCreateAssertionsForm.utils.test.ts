import {
    AssertionMonitorBuilderState,
    FreshnessAssertionScheduleBuilderTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import {
    buildFreshnessAssertionSpec,
    buildSubscriptionSpecs,
    buildVolumeAssertionSpec,
    mapExclusionWindows,
    validateAndTransformAssetSelectorFilters,
} from '@app/observe/shared/bulkCreate/form/BulkCreateAssertionsForm.utils';
import { FreshnessFormState, SubscriptionsFormState, VolumeFormState } from '@app/observe/shared/bulkCreate/form/types';
import { OperatorId } from '@app/tests/builder/steps/definition/builder/property/types/operators';
import { LogicalOperatorType, LogicalPredicate } from '@app/tests/builder/steps/definition/builder/types';

import {
    AssertionActionsInput,
    AssertionExclusionWindowType,
    DatasetFreshnessSourceType,
    DatasetVolumeSourceType,
    DateInterval,
    DayOfWeek,
    EntityChangeDetailsInput,
    EntityChangeType,
    EntityType,
    FreshnessAssertionScheduleType,
} from '@types';

describe('BulkCreateAssertionsForm.utils', () => {
    describe('validateAndTransformAssetSelectorFilters', () => {
        it('should return undefined when no filters provided', () => {
            const result = validateAndTransformAssetSelectorFilters(undefined);
            expect(result).toBeUndefined();
        });

        it('should throw error when top-level operator is not AND', () => {
            const filters: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.OR,
                operands: [],
            };

            expect(() => validateAndTransformAssetSelectorFilters(filters)).toThrow(
                'Top-level AND filter cannot be changed. Please click "Add Group" to support more complex filters.',
            );
        });

        it('should throw error when no dataset filter exists', () => {
            const filters: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: 'platform',
                        operator: OperatorId.EQUAL_TO,
                        values: ['snowflake'],
                    },
                ],
            };

            expect(() => validateAndTransformAssetSelectorFilters(filters)).toThrow(
                'Filter for Dataset entities cannot be changed or removed.',
            );
        });

        it('should throw error when dataset filter does not include Dataset entity type', () => {
            const filters: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: '_entityType',
                        operator: OperatorId.EQUAL_TO,
                        values: [EntityType.Dashboard],
                    },
                    {
                        type: 'property',
                        property: 'platform',
                        operator: OperatorId.EQUAL_TO,
                        values: ['snowflake'],
                    },
                ],
            };

            expect(() => validateAndTransformAssetSelectorFilters(filters)).toThrow(
                'Filter for Dataset entities cannot be changed or removed.',
            );
        });

        it('should throw error when no platform filter exists', () => {
            const filters: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: '_entityType',
                        operator: OperatorId.EQUAL_TO,
                        values: [EntityType.Dataset],
                    },
                ],
            };

            expect(() => validateAndTransformAssetSelectorFilters(filters)).toThrow(
                'Filter for a Platform is required.',
            );
        });

        it('should throw error when platform filter does not use EQUAL_TO operator', () => {
            const filters: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: '_entityType',
                        operator: OperatorId.EQUAL_TO,
                        values: [EntityType.Dataset],
                    },
                    {
                        type: 'property',
                        property: 'platform',
                        operator: OperatorId.CONTAINS_STR,
                        values: ['snowflake'],
                    },
                ],
            };

            expect(() => validateAndTransformAssetSelectorFilters(filters)).toThrow(
                'Filter for a Platform is required.',
            );
        });

        it('should return transformed filters when all validations pass', () => {
            const filters: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: '_entityType',
                        operator: OperatorId.EQUAL_TO,
                        values: [EntityType.Dataset],
                    },
                    {
                        type: 'property',
                        property: 'platform',
                        operator: OperatorId.EQUAL_TO,
                        values: ['snowflake'],
                    },
                ],
            };

            const result = validateAndTransformAssetSelectorFilters(filters);

            expect(result).toEqual({
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: '_entityType',
                        operator: OperatorId.EQUAL_TO,
                        values: [EntityType.Dataset],
                    },
                    {
                        type: 'property',
                        property: 'platform',
                        operator: OperatorId.EQUAL_TO,
                        values: ['snowflake'],
                    },
                ],
            });
        });

        it('should handle filters with additional valid operands', () => {
            const filters: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: '_entityType',
                        operator: OperatorId.EQUAL_TO,
                        values: [EntityType.Dataset],
                    },
                    {
                        type: 'property',
                        property: 'platform',
                        operator: OperatorId.EQUAL_TO,
                        values: ['snowflake'],
                    },
                    {
                        type: 'property',
                        property: 'tags',
                        operator: OperatorId.CONTAINS_STR,
                        values: ['important'],
                    },
                ],
            };

            const result = validateAndTransformAssetSelectorFilters(filters);

            expect(result).toEqual({
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: '_entityType',
                        operator: OperatorId.EQUAL_TO,
                        values: [EntityType.Dataset],
                    },
                    {
                        type: 'property',
                        property: 'platform',
                        operator: OperatorId.EQUAL_TO,
                        values: ['snowflake'],
                    },
                    {
                        type: 'property',
                        property: 'tags',
                        operator: OperatorId.CONTAINS_STR,
                        values: ['important'],
                    },
                ],
            });
        });

        it('should handle dataset filter with multiple entity types including Dataset', () => {
            const filters: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: '_entityType',
                        operator: OperatorId.EQUAL_TO,
                        values: [EntityType.Dataset, EntityType.Dashboard],
                    },
                    {
                        type: 'property',
                        property: 'platform',
                        operator: OperatorId.EQUAL_TO,
                        values: ['snowflake'],
                    },
                ],
            };

            const result = validateAndTransformAssetSelectorFilters(filters);

            expect(result).toEqual({
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: '_entityType',
                        operator: OperatorId.EQUAL_TO,
                        values: [EntityType.Dataset, EntityType.Dashboard],
                    },
                    {
                        type: 'property',
                        property: 'platform',
                        operator: OperatorId.EQUAL_TO,
                        values: ['snowflake'],
                    },
                ],
            });
        });

        it('should handle empty operands array', () => {
            const filters: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [],
            };

            expect(() => validateAndTransformAssetSelectorFilters(filters)).toThrow(
                'Filter for Dataset entities cannot be changed or removed.',
            );
        });

        it('should handle filters with logical operands (nested filters)', () => {
            const filters: LogicalPredicate = {
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: '_entityType',
                        operator: OperatorId.EQUAL_TO,
                        values: [EntityType.Dataset],
                    },
                    {
                        type: 'property',
                        property: 'platform',
                        operator: OperatorId.EQUAL_TO,
                        values: ['snowflake'],
                    },
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property',
                                property: 'tags',
                                operator: OperatorId.CONTAINS_STR,
                                values: ['important'],
                            },
                            {
                                type: 'property',
                                property: 'tags',
                                operator: OperatorId.CONTAINS_STR,
                                values: ['critical'],
                            },
                        ],
                    },
                ],
            };

            const result = validateAndTransformAssetSelectorFilters(filters);

            expect(result).toEqual({
                type: 'logical',
                operator: LogicalOperatorType.AND,
                operands: [
                    {
                        type: 'property',
                        property: '_entityType',
                        operator: OperatorId.EQUAL_TO,
                        values: [EntityType.Dataset],
                    },
                    {
                        type: 'property',
                        property: 'platform',
                        operator: OperatorId.EQUAL_TO,
                        values: ['snowflake'],
                    },
                    {
                        type: 'logical',
                        operator: LogicalOperatorType.OR,
                        operands: [
                            {
                                type: 'property',
                                property: 'tags',
                                operator: OperatorId.CONTAINS_STR,
                                values: ['important'],
                            },
                            {
                                type: 'property',
                                property: 'tags',
                                operator: OperatorId.CONTAINS_STR,
                                values: ['critical'],
                            },
                        ],
                    },
                ],
            });
        });

        describe('buildFreshnessAssertionSpec', () => {
            const mockActions: AssertionActionsInput = {
                onFailure: [],
                onSuccess: [],
            };

            const mockCronSchedule = {
                cron: '0 * * * *',
                timezone: 'UTC',
            };

            const mockInferenceSettings: AssertionMonitorBuilderState['inferenceSettings'] = {
                sensitivity: { level: 0.8 },
                trainingDataLookbackWindowDays: 30,
                exclusionWindows: [],
            };

            it('should return undefined when freshness assertion is not enabled', () => {
                const freshnessFormState: FreshnessFormState = {
                    freshnessAssertionEnabled: false,
                    freshnessAssertionType: FreshnessAssertionScheduleBuilderTypeOptions.AiInferred,
                    freshnessAssertionInterval: 6,
                    freshnessAssertionIntervalUnit: DateInterval.Hour,
                    freshnessInferenceSettings: mockInferenceSettings,
                    freshnessSchedule: mockCronSchedule,
                    freshnessActions: mockActions,
                    freshnessSourceType: DatasetFreshnessSourceType.DatahubOperation,
                };

                const result = buildFreshnessAssertionSpec(freshnessFormState);

                expect(result).toBeUndefined();
            });

            it('should build AI inferred freshness assertion spec correctly', () => {
                const freshnessFormState: FreshnessFormState = {
                    freshnessAssertionEnabled: true,
                    freshnessAssertionType: FreshnessAssertionScheduleBuilderTypeOptions.AiInferred,
                    freshnessAssertionInterval: 6,
                    freshnessAssertionIntervalUnit: DateInterval.Hour,
                    freshnessInferenceSettings: mockInferenceSettings,
                    freshnessSchedule: mockCronSchedule,
                    freshnessActions: mockActions,
                    freshnessSourceType: DatasetFreshnessSourceType.AuditLog,
                };

                const result = buildFreshnessAssertionSpec(freshnessFormState);

                expect(result).toEqual({
                    criteria: {
                        type: 'AI',
                        inferenceSettings: {
                            sensitivity: {
                                level: 0.8,
                            },
                            trainingDataLookbackWindowDays: 30,
                            exclusionWindows: [],
                        },
                    },
                    evaluationParameters: {
                        sourceType: DatasetFreshnessSourceType.AuditLog,
                    },
                    actions: mockActions,
                    evaluationSchedule: {
                        cron: '0 * * * *',
                        timezone: 'UTC',
                    },
                });
            });

            it('should build fixed interval freshness assertion spec correctly', () => {
                const freshnessFormState: FreshnessFormState = {
                    freshnessAssertionEnabled: true,
                    freshnessAssertionType: FreshnessAssertionScheduleBuilderTypeOptions.FixedInterval,
                    freshnessAssertionInterval: 12,
                    freshnessAssertionIntervalUnit: DateInterval.Hour,
                    freshnessInferenceSettings: mockInferenceSettings,
                    freshnessSchedule: mockCronSchedule,
                    freshnessActions: mockActions,
                    freshnessSourceType: DatasetFreshnessSourceType.InformationSchema,
                };

                const result = buildFreshnessAssertionSpec(freshnessFormState);

                expect(result).toEqual({
                    criteria: {
                        type: 'MANUAL',
                        schedule: {
                            type: FreshnessAssertionScheduleType.FixedInterval,
                            fixedInterval: {
                                unit: DateInterval.Hour,
                                multiple: 12,
                            },
                            cron: {
                                cron: '0 * * * *',
                                timezone: 'UTC',
                            },
                        },
                    },
                    evaluationParameters: {
                        sourceType: DatasetFreshnessSourceType.InformationSchema,
                    },
                    actions: mockActions,
                    evaluationSchedule: {
                        cron: '0 * * * *',
                        timezone: 'UTC',
                    },
                });
            });

            it('should build since the last check freshness assertion spec correctly', () => {
                const freshnessFormState: FreshnessFormState = {
                    freshnessAssertionEnabled: true,
                    freshnessAssertionType: FreshnessAssertionScheduleBuilderTypeOptions.SinceTheLastCheck,
                    freshnessAssertionInterval: 6,
                    freshnessAssertionIntervalUnit: DateInterval.Hour,
                    freshnessInferenceSettings: mockInferenceSettings,
                    freshnessSchedule: mockCronSchedule,
                    freshnessActions: mockActions,
                    freshnessSourceType: DatasetFreshnessSourceType.FieldValue,
                };

                const result = buildFreshnessAssertionSpec(freshnessFormState);

                expect(result).toEqual({
                    criteria: {
                        type: 'MANUAL',
                        schedule: {
                            type: FreshnessAssertionScheduleType.SinceTheLastCheck,
                            cron: {
                                cron: '0 * * * *',
                                timezone: 'UTC',
                            },
                        },
                    },
                    evaluationParameters: {
                        sourceType: DatasetFreshnessSourceType.FieldValue,
                    },
                    actions: mockActions,
                    evaluationSchedule: {
                        cron: '0 * * * *',
                        timezone: 'UTC',
                    },
                });
            });

            it('should use default cron schedule when schedule is undefined', () => {
                const freshnessFormState: FreshnessFormState = {
                    freshnessAssertionEnabled: true,
                    freshnessAssertionType: FreshnessAssertionScheduleBuilderTypeOptions.FixedInterval,
                    freshnessAssertionInterval: 24,
                    freshnessAssertionIntervalUnit: DateInterval.Hour,
                    freshnessInferenceSettings: mockInferenceSettings,
                    freshnessSchedule: undefined,
                    freshnessActions: mockActions,
                    freshnessSourceType: DatasetFreshnessSourceType.FileMetadata,
                };

                const result = buildFreshnessAssertionSpec(freshnessFormState);

                // The result should use the DEFAULT_CRON_SCHEDULE
                expect(result?.criteria?.type).toBe('MANUAL');
                expect(result?.evaluationSchedule).toBeDefined();
                // We can't easily test the exact default values without importing them,
                // but we can verify the structure is correct
            });

            it('should handle AI inference with custom inference settings', () => {
                const customInferenceSettings: AssertionMonitorBuilderState['inferenceSettings'] = {
                    sensitivity: { level: 0.9 },
                    trainingDataLookbackWindowDays: 60,
                    exclusionWindows: [
                        {
                            type: AssertionExclusionWindowType.Weekly,
                            displayName: 'Weekends',
                            weekly: {
                                daysOfWeek: ['SATURDAY', 'SUNDAY'],
                                startTime: '00:00',
                                endTime: '23:59',
                                timezone: 'UTC',
                            },
                        },
                    ],
                };

                const freshnessFormState: FreshnessFormState = {
                    freshnessAssertionEnabled: true,
                    freshnessAssertionType: FreshnessAssertionScheduleBuilderTypeOptions.AiInferred,
                    freshnessAssertionInterval: 6,
                    freshnessAssertionIntervalUnit: DateInterval.Hour,
                    freshnessInferenceSettings: customInferenceSettings,
                    freshnessSchedule: mockCronSchedule,
                    freshnessActions: mockActions,
                    freshnessSourceType: DatasetFreshnessSourceType.DatahubOperation,
                };

                const result = buildFreshnessAssertionSpec(freshnessFormState);

                expect(result?.criteria?.type).toBe('AI');
                if (result?.criteria?.type === 'AI') {
                    expect(result.criteria.inferenceSettings.sensitivity?.level).toBe(0.9);
                    expect(result.criteria.inferenceSettings.trainingDataLookbackWindowDays).toBe(60);
                    expect(result.criteria.inferenceSettings.exclusionWindows).toEqual([
                        {
                            type: AssertionExclusionWindowType.Weekly,
                            displayName: 'Weekends',
                            fixedRange: undefined,
                            holiday: undefined,
                            weekly: {
                                daysOfWeek: [DayOfWeek.Saturday, DayOfWeek.Sunday],
                                startTime: '00:00',
                                endTime: '23:59',
                                timezone: 'UTC',
                            },
                        },
                    ]);
                }
            });

            it('should handle undefined inference settings with defaults', () => {
                const freshnessFormState: FreshnessFormState = {
                    freshnessAssertionEnabled: true,
                    freshnessAssertionType: FreshnessAssertionScheduleBuilderTypeOptions.AiInferred,
                    freshnessAssertionInterval: 6,
                    freshnessAssertionIntervalUnit: DateInterval.Hour,
                    freshnessInferenceSettings: undefined,
                    freshnessSchedule: mockCronSchedule,
                    freshnessActions: mockActions,
                    freshnessSourceType: DatasetFreshnessSourceType.DatahubOperation,
                };

                const result = buildFreshnessAssertionSpec(freshnessFormState);

                expect(result?.criteria?.type).toBe('AI');
                if (result?.criteria?.type === 'AI') {
                    // Should use default values
                    expect(result.criteria.inferenceSettings.exclusionWindows).toEqual([]);
                    expect(result.criteria.inferenceSettings.sensitivity).toBeDefined();
                    expect(result.criteria.inferenceSettings.trainingDataLookbackWindowDays).toBeDefined();
                }
            });
        });

        describe('buildVolumeAssertionSpec', () => {
            const mockActions: AssertionActionsInput = {
                onFailure: [],
                onSuccess: [],
            };

            const mockCronSchedule = {
                cron: '0 * * * *',
                timezone: 'UTC',
            };

            const mockInferenceSettings: AssertionMonitorBuilderState['inferenceSettings'] = {
                sensitivity: { level: 0.7 },
                trainingDataLookbackWindowDays: 45,
                exclusionWindows: [],
            };

            it('should return undefined when volume assertion is not enabled', () => {
                const volumeFormState: VolumeFormState = {
                    volumeAssertionEnabled: false,
                    volumeInferenceSettings: mockInferenceSettings,
                    volumeSchedule: mockCronSchedule,
                    volumeActions: mockActions,
                    volumeSourceType: DatasetVolumeSourceType.DatahubDatasetProfile,
                };

                const result = buildVolumeAssertionSpec(volumeFormState);

                expect(result).toBeUndefined();
            });

            it('should build volume assertion spec correctly when enabled', () => {
                const volumeFormState: VolumeFormState = {
                    volumeAssertionEnabled: true,
                    volumeInferenceSettings: mockInferenceSettings,
                    volumeSchedule: mockCronSchedule,
                    volumeActions: mockActions,
                    volumeSourceType: DatasetVolumeSourceType.InformationSchema,
                };

                const result = buildVolumeAssertionSpec(volumeFormState);

                expect(result).toEqual({
                    evaluationParameters: {
                        sourceType: DatasetVolumeSourceType.InformationSchema,
                    },
                    inferenceSettings: {
                        sensitivity: {
                            level: 0.7,
                        },
                        trainingDataLookbackWindowDays: 45,
                        exclusionWindows: [],
                    },
                    actions: mockActions,
                    evaluationSchedule: {
                        cron: '0 * * * *',
                        timezone: 'UTC',
                    },
                });
            });

            it('should use default schedule when schedule is undefined', () => {
                const volumeFormState: VolumeFormState = {
                    volumeAssertionEnabled: true,
                    volumeInferenceSettings: mockInferenceSettings,
                    volumeSchedule: undefined,
                    volumeActions: mockActions,
                    volumeSourceType: DatasetVolumeSourceType.Query,
                };

                const result = buildVolumeAssertionSpec(volumeFormState);

                expect(result?.evaluationSchedule).toBeDefined();
                expect(result?.evaluationParameters.sourceType).toBe(DatasetVolumeSourceType.Query);
            });

            it('should handle undefined inference settings with defaults', () => {
                const volumeFormState: VolumeFormState = {
                    volumeAssertionEnabled: true,
                    volumeInferenceSettings: undefined,
                    volumeSchedule: mockCronSchedule,
                    volumeActions: mockActions,
                    volumeSourceType: DatasetVolumeSourceType.DatahubDatasetProfile,
                };

                const result = buildVolumeAssertionSpec(volumeFormState);

                expect(result?.inferenceSettings).toBeDefined();
                expect(result?.inferenceSettings.exclusionWindows).toEqual([]);
                expect(result?.inferenceSettings.sensitivity).toBeDefined();
                expect(result?.inferenceSettings.trainingDataLookbackWindowDays).toBeDefined();
            });

            it('should handle custom inference settings with exclusion windows', () => {
                const customInferenceSettings: AssertionMonitorBuilderState['inferenceSettings'] = {
                    sensitivity: { level: 0.95 },
                    trainingDataLookbackWindowDays: 90,
                    exclusionWindows: [
                        {
                            type: AssertionExclusionWindowType.Holiday,
                            displayName: 'Christmas',
                            holiday: {
                                name: 'Christmas Day',
                                region: 'US',
                                timezone: 'America/New_York',
                            },
                        },
                        {
                            type: AssertionExclusionWindowType.FixedRange,
                            displayName: 'Maintenance Window',
                            fixedRange: {
                                startTimeMillis: 1640995200000,
                                endTimeMillis: 1641081600000,
                            },
                        },
                    ],
                };

                const volumeFormState: VolumeFormState = {
                    volumeAssertionEnabled: true,
                    volumeInferenceSettings: customInferenceSettings,
                    volumeSchedule: mockCronSchedule,
                    volumeActions: mockActions,
                    volumeSourceType: DatasetVolumeSourceType.InformationSchema,
                };

                const result = buildVolumeAssertionSpec(volumeFormState);

                expect(result?.inferenceSettings.sensitivity?.level).toBe(0.95);
                expect(result?.inferenceSettings.trainingDataLookbackWindowDays).toBe(90);
                expect(result?.inferenceSettings.exclusionWindows).toEqual([
                    {
                        type: AssertionExclusionWindowType.Holiday,
                        displayName: 'Christmas',
                        fixedRange: undefined,
                        holiday: {
                            name: 'Christmas Day',
                            region: 'US',
                            timezone: 'America/New_York',
                        },
                        weekly: undefined,
                    },
                    {
                        type: AssertionExclusionWindowType.FixedRange,
                        displayName: 'Maintenance Window',
                        fixedRange: {
                            startTimeMillis: 1640995200000,
                            endTimeMillis: 1641081600000,
                        },
                        holiday: undefined,
                        weekly: undefined,
                    },
                ]);
            });

            it('should handle all volume source types', () => {
                const testCases = [
                    DatasetVolumeSourceType.DatahubDatasetProfile,
                    DatasetVolumeSourceType.InformationSchema,
                    DatasetVolumeSourceType.Query,
                ];

                testCases.forEach((sourceType) => {
                    const volumeFormState: VolumeFormState = {
                        volumeAssertionEnabled: true,
                        volumeInferenceSettings: mockInferenceSettings,
                        volumeSchedule: mockCronSchedule,
                        volumeActions: mockActions,
                        volumeSourceType: sourceType,
                    };

                    const result = buildVolumeAssertionSpec(volumeFormState);

                    expect(result?.evaluationParameters.sourceType).toBe(sourceType);
                });
            });

            it('should handle partial inference settings', () => {
                const partialInferenceSettings: AssertionMonitorBuilderState['inferenceSettings'] = {
                    sensitivity: { level: 0.6 },
                    // Missing trainingDataLookbackWindowDays and exclusionWindows
                };

                const volumeFormState: VolumeFormState = {
                    volumeAssertionEnabled: true,
                    volumeInferenceSettings: partialInferenceSettings,
                    volumeSchedule: mockCronSchedule,
                    volumeActions: mockActions,
                    volumeSourceType: DatasetVolumeSourceType.Query,
                };

                const result = buildVolumeAssertionSpec(volumeFormState);

                expect(result?.inferenceSettings.sensitivity?.level).toBe(0.6);
                expect(result?.inferenceSettings.trainingDataLookbackWindowDays).toBeDefined(); // Should use default
                expect(result?.inferenceSettings.exclusionWindows).toEqual([]); // Should map to empty array
            });
        });
    });

    describe('buildSubscriptionSpecs', () => {
        const mockPersonalEntityChangeTypes: EntityChangeDetailsInput[] = [
            { entityChangeType: EntityChangeType.AssertionFailed },
            { entityChangeType: EntityChangeType.AssertionPassed },
        ];

        const mockGroupEntityChangeTypes: EntityChangeDetailsInput[] = [
            { entityChangeType: EntityChangeType.IncidentRaised },
            { entityChangeType: EntityChangeType.IncidentResolved },
        ];

        const mockPersonalUserUrn = 'urn:li:corpuser:testuser';
        const mockGroupUrns = ['urn:li:corpGroup:group1', 'urn:li:corpGroup:group2'];

        it('should return undefined when neither personal nor group subscriptions are enabled', () => {
            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: false,
                personalEntityChangeTypes: mockPersonalEntityChangeTypes,
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: false,
                selectedGroups: mockGroupUrns,
                groupEntityChangeTypes: mockGroupEntityChangeTypes,
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toBeUndefined();
        });

        it('should return personal subscription spec when personal subscription is enabled with entity change types', () => {
            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: true,
                personalEntityChangeTypes: mockPersonalEntityChangeTypes,
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: false,
                selectedGroups: [],
                groupEntityChangeTypes: [],
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toEqual([
                {
                    subscriberUrn: mockPersonalUserUrn,
                    entityChangeTypes: mockPersonalEntityChangeTypes,
                },
            ]);
        });

        it('should not include personal subscription when enabled but no entity change types selected', () => {
            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: true,
                personalEntityChangeTypes: [],
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: false,
                selectedGroups: [],
                groupEntityChangeTypes: [],
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toBeUndefined();
        });

        it('should return group subscription specs when group subscription is enabled with groups and entity change types', () => {
            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: false,
                personalEntityChangeTypes: [],
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: true,
                selectedGroups: mockGroupUrns,
                groupEntityChangeTypes: mockGroupEntityChangeTypes,
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toEqual([
                {
                    subscriberUrn: 'urn:li:corpGroup:group1',
                    entityChangeTypes: mockGroupEntityChangeTypes,
                },
                {
                    subscriberUrn: 'urn:li:corpGroup:group2',
                    entityChangeTypes: mockGroupEntityChangeTypes,
                },
            ]);
        });

        it('should not include group subscriptions when enabled but no groups selected', () => {
            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: false,
                personalEntityChangeTypes: [],
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: true,
                selectedGroups: [],
                groupEntityChangeTypes: mockGroupEntityChangeTypes,
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toBeUndefined();
        });

        it('should not include group subscriptions when enabled but no entity change types selected', () => {
            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: false,
                personalEntityChangeTypes: [],
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: true,
                selectedGroups: mockGroupUrns,
                groupEntityChangeTypes: [],
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toBeUndefined();
        });

        it('should return both personal and group subscription specs when both are enabled', () => {
            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: true,
                personalEntityChangeTypes: mockPersonalEntityChangeTypes,
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: true,
                selectedGroups: ['urn:li:corpGroup:group1'],
                groupEntityChangeTypes: mockGroupEntityChangeTypes,
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toEqual([
                {
                    subscriberUrn: mockPersonalUserUrn,
                    entityChangeTypes: mockPersonalEntityChangeTypes,
                },
                {
                    subscriberUrn: 'urn:li:corpGroup:group1',
                    entityChangeTypes: mockGroupEntityChangeTypes,
                },
            ]);
        });

        it('should handle single entity change type for personal subscription', () => {
            const singleEntityChangeType: EntityChangeDetailsInput[] = [
                { entityChangeType: EntityChangeType.AssertionError },
            ];

            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: true,
                personalEntityChangeTypes: singleEntityChangeType,
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: false,
                selectedGroups: [],
                groupEntityChangeTypes: [],
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toEqual([
                {
                    subscriberUrn: mockPersonalUserUrn,
                    entityChangeTypes: singleEntityChangeType,
                },
            ]);
        });

        it('should handle single group for group subscription', () => {
            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: false,
                personalEntityChangeTypes: [],
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: true,
                selectedGroups: ['urn:li:corpGroup:singleGroup'],
                groupEntityChangeTypes: mockGroupEntityChangeTypes,
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toEqual([
                {
                    subscriberUrn: 'urn:li:corpGroup:singleGroup',
                    entityChangeTypes: mockGroupEntityChangeTypes,
                },
            ]);
        });

        it('should handle multiple entity change types', () => {
            const multipleEntityChangeTypes: EntityChangeDetailsInput[] = [
                { entityChangeType: EntityChangeType.AssertionFailed },
                { entityChangeType: EntityChangeType.AssertionPassed },
                { entityChangeType: EntityChangeType.AssertionError },
                { entityChangeType: EntityChangeType.IncidentRaised },
                { entityChangeType: EntityChangeType.IncidentResolved },
                { entityChangeType: EntityChangeType.OperationColumnAdded },
            ];

            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: true,
                personalEntityChangeTypes: multipleEntityChangeTypes,
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: false,
                selectedGroups: [],
                groupEntityChangeTypes: [],
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toEqual([
                {
                    subscriberUrn: mockPersonalUserUrn,
                    entityChangeTypes: multipleEntityChangeTypes,
                },
            ]);
        });

        it('should handle multiple groups with different entity change types for personal and group subscriptions', () => {
            const personalEntityChangeTypes: EntityChangeDetailsInput[] = [
                { entityChangeType: EntityChangeType.AssertionFailed },
            ];

            const groupEntityChangeTypes: EntityChangeDetailsInput[] = [
                { entityChangeType: EntityChangeType.IncidentRaised },
                { entityChangeType: EntityChangeType.OperationColumnAdded },
            ];

            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: true,
                personalEntityChangeTypes,
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: true,
                selectedGroups: ['urn:li:corpGroup:group1', 'urn:li:corpGroup:group2', 'urn:li:corpGroup:group3'],
                groupEntityChangeTypes,
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toEqual([
                {
                    subscriberUrn: mockPersonalUserUrn,
                    entityChangeTypes: personalEntityChangeTypes,
                },
                {
                    subscriberUrn: 'urn:li:corpGroup:group1',
                    entityChangeTypes: groupEntityChangeTypes,
                },
                {
                    subscriberUrn: 'urn:li:corpGroup:group2',
                    entityChangeTypes: groupEntityChangeTypes,
                },
                {
                    subscriberUrn: 'urn:li:corpGroup:group3',
                    entityChangeTypes: groupEntityChangeTypes,
                },
            ]);
        });

        it('should return only personal subscription when personal is enabled but group conditions are not met', () => {
            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: true,
                personalEntityChangeTypes: mockPersonalEntityChangeTypes,
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: true,
                selectedGroups: [], // No groups selected
                groupEntityChangeTypes: mockGroupEntityChangeTypes,
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toEqual([
                {
                    subscriberUrn: mockPersonalUserUrn,
                    entityChangeTypes: mockPersonalEntityChangeTypes,
                },
            ]);
        });

        it('should return only group subscriptions when group is enabled but personal conditions are not met', () => {
            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: true,
                personalEntityChangeTypes: [], // No entity change types
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: true,
                selectedGroups: mockGroupUrns,
                groupEntityChangeTypes: mockGroupEntityChangeTypes,
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toEqual([
                {
                    subscriberUrn: 'urn:li:corpGroup:group1',
                    entityChangeTypes: mockGroupEntityChangeTypes,
                },
                {
                    subscriberUrn: 'urn:li:corpGroup:group2',
                    entityChangeTypes: mockGroupEntityChangeTypes,
                },
            ]);
        });

        it('should return undefined when all subscriptions are enabled but none meet the conditions', () => {
            const subscriptionFormState: SubscriptionsFormState = {
                personalSubscriptionEnabled: true,
                personalEntityChangeTypes: [], // No entity change types
                personalUserUrn: mockPersonalUserUrn,
                groupSubscriptionEnabled: true,
                selectedGroups: [], // No groups selected
                groupEntityChangeTypes: mockGroupEntityChangeTypes,
            };

            const result = buildSubscriptionSpecs(subscriptionFormState);

            expect(result).toBeUndefined();
        });
    });

    describe('mapExclusionWindows', () => {
        it('should return empty array when no exclusion windows provided', () => {
            const result = mapExclusionWindows(undefined);
            expect(result).toEqual([]);
        });

        it('should return empty array when empty exclusion windows provided', () => {
            const result = mapExclusionWindows([]);
            expect(result).toEqual([]);
        });

        it('should map fixed range exclusion window correctly', () => {
            const exclusionWindows: NonNullable<AssertionMonitorBuilderState['inferenceSettings']>['exclusionWindows'] =
                [
                    {
                        type: AssertionExclusionWindowType.FixedRange,
                        displayName: 'Holiday Break',
                        fixedRange: {
                            startTimeMillis: 1640995200000, // 2022-01-01 00:00:00 UTC
                            endTimeMillis: 1641081600000, // 2022-01-02 00:00:00 UTC
                        },
                    },
                ];

            const result = mapExclusionWindows(exclusionWindows);

            expect(result).toEqual([
                {
                    type: AssertionExclusionWindowType.FixedRange,
                    displayName: 'Holiday Break',
                    fixedRange: {
                        startTimeMillis: 1640995200000,
                        endTimeMillis: 1641081600000,
                    },
                    holiday: undefined,
                    weekly: undefined,
                },
            ]);
        });

        it('should map holiday exclusion window correctly', () => {
            const exclusionWindows: NonNullable<AssertionMonitorBuilderState['inferenceSettings']>['exclusionWindows'] =
                [
                    {
                        type: AssertionExclusionWindowType.Holiday,
                        displayName: 'Christmas',
                        holiday: {
                            name: 'Christmas Day',
                            region: 'US',
                            timezone: 'America/New_York',
                        },
                    },
                ];

            const result = mapExclusionWindows(exclusionWindows);

            expect(result).toEqual([
                {
                    type: AssertionExclusionWindowType.Holiday,
                    displayName: 'Christmas',
                    fixedRange: undefined,
                    holiday: {
                        name: 'Christmas Day',
                        region: 'US',
                        timezone: 'America/New_York',
                    },
                    weekly: undefined,
                },
            ]);
        });

        it('should map weekly exclusion window correctly', () => {
            const exclusionWindows: NonNullable<AssertionMonitorBuilderState['inferenceSettings']>['exclusionWindows'] =
                [
                    {
                        type: AssertionExclusionWindowType.Weekly,
                        displayName: 'Weekends',
                        weekly: {
                            daysOfWeek: ['SATURDAY', 'SUNDAY'],
                            startTime: '00:00',
                            endTime: '23:59',
                            timezone: 'UTC',
                        },
                    },
                ];

            const result = mapExclusionWindows(exclusionWindows);

            expect(result).toEqual([
                {
                    type: AssertionExclusionWindowType.Weekly,
                    displayName: 'Weekends',
                    fixedRange: undefined,
                    holiday: undefined,
                    weekly: {
                        daysOfWeek: [DayOfWeek.Saturday, DayOfWeek.Sunday],
                        startTime: '00:00',
                        endTime: '23:59',
                        timezone: 'UTC',
                    },
                },
            ]);
        });

        it('should handle exclusion window with only type and no optional fields', () => {
            const exclusionWindows: NonNullable<AssertionMonitorBuilderState['inferenceSettings']>['exclusionWindows'] =
                [
                    {
                        type: AssertionExclusionWindowType.Holiday,
                    },
                ];

            const result = mapExclusionWindows(exclusionWindows);

            expect(result).toEqual([
                {
                    type: AssertionExclusionWindowType.Holiday,
                    displayName: undefined,
                    fixedRange: undefined,
                    holiday: undefined,
                    weekly: undefined,
                },
            ]);
        });

        it('should handle holiday exclusion window with partial holiday data', () => {
            const exclusionWindows: NonNullable<AssertionMonitorBuilderState['inferenceSettings']>['exclusionWindows'] =
                [
                    {
                        type: AssertionExclusionWindowType.Holiday,
                        displayName: 'New Year',
                        holiday: {
                            name: 'New Year Day',
                            // region and timezone are optional
                        },
                    },
                ];

            const result = mapExclusionWindows(exclusionWindows);

            expect(result).toEqual([
                {
                    type: AssertionExclusionWindowType.Holiday,
                    displayName: 'New Year',
                    fixedRange: undefined,
                    holiday: {
                        name: 'New Year Day',
                        region: undefined,
                        timezone: undefined,
                    },
                    weekly: undefined,
                },
            ]);
        });

        it('should handle weekly exclusion window with partial weekly data', () => {
            const exclusionWindows: NonNullable<AssertionMonitorBuilderState['inferenceSettings']>['exclusionWindows'] =
                [
                    {
                        type: AssertionExclusionWindowType.Weekly,
                        displayName: 'Maintenance Window',
                        weekly: {
                            startTime: '02:00',
                            endTime: '04:00',
                            // daysOfWeek and timezone are optional
                        },
                    },
                ];

            const result = mapExclusionWindows(exclusionWindows);

            expect(result).toEqual([
                {
                    type: AssertionExclusionWindowType.Weekly,
                    displayName: 'Maintenance Window',
                    fixedRange: undefined,
                    holiday: undefined,
                    weekly: {
                        daysOfWeek: undefined,
                        startTime: '02:00',
                        endTime: '04:00',
                        timezone: undefined,
                    },
                },
            ]);
        });

        it('should handle weekly exclusion window with empty daysOfWeek array', () => {
            const exclusionWindows: NonNullable<AssertionMonitorBuilderState['inferenceSettings']>['exclusionWindows'] =
                [
                    {
                        type: AssertionExclusionWindowType.Weekly,
                        displayName: 'Empty Days',
                        weekly: {
                            daysOfWeek: [],
                            startTime: '10:00',
                            endTime: '11:00',
                            timezone: 'UTC',
                        },
                    },
                ];

            const result = mapExclusionWindows(exclusionWindows);

            expect(result).toEqual([
                {
                    type: AssertionExclusionWindowType.Weekly,
                    displayName: 'Empty Days',
                    fixedRange: undefined,
                    holiday: undefined,
                    weekly: {
                        daysOfWeek: [],
                        startTime: '10:00',
                        endTime: '11:00',
                        timezone: 'UTC',
                    },
                },
            ]);
        });

        it('should map multiple exclusion windows of different types', () => {
            const exclusionWindows: NonNullable<AssertionMonitorBuilderState['inferenceSettings']>['exclusionWindows'] =
                [
                    {
                        type: AssertionExclusionWindowType.FixedRange,
                        displayName: 'Deployment Window',
                        fixedRange: {
                            startTimeMillis: 1640995200000,
                            endTimeMillis: 1641081600000,
                        },
                    },
                    {
                        type: AssertionExclusionWindowType.Weekly,
                        displayName: 'Weekend Maintenance',
                        weekly: {
                            daysOfWeek: ['SATURDAY'],
                            startTime: '01:00',
                            endTime: '03:00',
                            timezone: 'UTC',
                        },
                    },
                    {
                        type: AssertionExclusionWindowType.Holiday,
                        displayName: 'Independence Day',
                        holiday: {
                            name: 'July 4th',
                            region: 'US',
                        },
                    },
                ];

            const result = mapExclusionWindows(exclusionWindows);

            expect(result).toEqual([
                {
                    type: AssertionExclusionWindowType.FixedRange,
                    displayName: 'Deployment Window',
                    fixedRange: {
                        startTimeMillis: 1640995200000,
                        endTimeMillis: 1641081600000,
                    },
                    holiday: undefined,
                    weekly: undefined,
                },
                {
                    type: AssertionExclusionWindowType.Weekly,
                    displayName: 'Weekend Maintenance',
                    fixedRange: undefined,
                    holiday: undefined,
                    weekly: {
                        daysOfWeek: [DayOfWeek.Saturday],
                        startTime: '01:00',
                        endTime: '03:00',
                        timezone: 'UTC',
                    },
                },
                {
                    type: AssertionExclusionWindowType.Holiday,
                    displayName: 'Independence Day',
                    fixedRange: undefined,
                    holiday: {
                        name: 'July 4th',
                        region: 'US',
                        timezone: undefined,
                    },
                    weekly: undefined,
                },
            ]);
        });

        it('should handle exclusion window with undefined fixed range', () => {
            const exclusionWindows: NonNullable<AssertionMonitorBuilderState['inferenceSettings']>['exclusionWindows'] =
                [
                    {
                        type: AssertionExclusionWindowType.FixedRange,
                        displayName: 'No Range Specified',
                        fixedRange: undefined,
                    },
                ];

            const result = mapExclusionWindows(exclusionWindows);

            expect(result).toEqual([
                {
                    type: AssertionExclusionWindowType.FixedRange,
                    displayName: 'No Range Specified',
                    fixedRange: undefined,
                    holiday: undefined,
                    weekly: undefined,
                },
            ]);
        });

        it('should handle exclusion window with undefined holiday', () => {
            const exclusionWindows: NonNullable<AssertionMonitorBuilderState['inferenceSettings']>['exclusionWindows'] =
                [
                    {
                        type: AssertionExclusionWindowType.Holiday,
                        displayName: 'No Holiday Specified',
                        holiday: undefined,
                    },
                ];

            const result = mapExclusionWindows(exclusionWindows);

            expect(result).toEqual([
                {
                    type: AssertionExclusionWindowType.Holiday,
                    displayName: 'No Holiday Specified',
                    fixedRange: undefined,
                    holiday: undefined,
                    weekly: undefined,
                },
            ]);
        });

        it('should handle exclusion window with undefined weekly', () => {
            const exclusionWindows: NonNullable<AssertionMonitorBuilderState['inferenceSettings']>['exclusionWindows'] =
                [
                    {
                        type: AssertionExclusionWindowType.Weekly,
                        displayName: 'No Weekly Schedule Specified',
                        weekly: undefined,
                    },
                ];

            const result = mapExclusionWindows(exclusionWindows);

            expect(result).toEqual([
                {
                    type: AssertionExclusionWindowType.Weekly,
                    displayName: 'No Weekly Schedule Specified',
                    fixedRange: undefined,
                    holiday: undefined,
                    weekly: undefined,
                },
            ]);
        });
    });
});
