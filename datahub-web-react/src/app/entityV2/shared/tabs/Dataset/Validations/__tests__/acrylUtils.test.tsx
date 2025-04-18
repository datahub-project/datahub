import { describe, expect, it } from 'vitest';

import {
    AssertionWithMonitorDetails,
    canManageAssertionMonitor,
    createAssertionGroups,
    extractLatestGeneratedAt,
    getAssertionGroupName,
    getAssertionGroupSummaryMessage,
    getAssertionType,
    getAssertionTypesForEntityType,
    getAssertionsSummary,
    getCronAsText,
    getEntityUrnForAssertion,
    getLegacyAssertionsSummary,
    getNextScheduleEvaluationTimeMs,
    getPreviousScheduleEvaluationTimeMs,
    getSiblingWithUrn,
    getSiblings,
    isMonitorActive,
} from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';

import {
    Assertion,
    AssertionResultType,
    AssertionRunStatus,
    AssertionStdOperator,
    AssertionType,
    CronSchedule,
    DatasetAssertionScope,
    DatasetFreshnessSourceType,
    DatasetVolumeSourceType,
    EntityType,
    Monitor,
    MonitorMode,
    MonitorType,
} from '@types';

describe('acrylUtils', () => {
    describe('getAssertionGroupName', () => {
        it('should return proper name for known assertion type', () => {
            expect(getAssertionGroupName('FRESHNESS')).toBe('Freshness');
        });

        it('should return title case for unknown assertion type', () => {
            expect(getAssertionGroupName('UNKNOWN TYPE')).toBe('Unknown Type');
        });
    });

    describe('getAssertionType', () => {
        it('should return type from custom assertion', () => {
            const assertion: Partial<Assertion> = {
                info: {
                    type: AssertionType.Custom,
                    customAssertion: {
                        type: 'customType',
                        entityUrn: 'test-urn',
                    },
                },
            };
            expect(getAssertionType(assertion as Assertion)).toBe('CUSTOMTYPE');
        });

        it('should return type from regular assertion', () => {
            const assertion: Partial<Assertion> = {
                info: {
                    type: AssertionType.Freshness,
                },
            };
            expect(getAssertionType(assertion as Assertion)).toBe('FRESHNESS');
        });

        it('should return undefined for invalid assertion', () => {
            const assertion: Partial<Assertion> = {
                info: {
                    type: AssertionType.Freshness,
                },
            };
            expect(getAssertionType(assertion as Assertion)).toBe('FRESHNESS');
        });
    });

    describe('getAssertionsSummary', () => {
        it('should correctly count passing, failing, and erroring assertions', () => {
            const assertions: Partial<Assertion>[] = [
                {
                    runEvents: {
                        runEvents: [
                            {
                                result: { type: AssertionResultType.Success },
                                timestampMillis: Date.now(),
                                asserteeUrn: 'test-urn',
                                assertionUrn: 'test-urn',
                                runId: 'test-run',
                                status: AssertionRunStatus.Complete,
                            },
                        ],
                        succeeded: 1,
                        failed: 0,
                        errored: 0,
                        total: 1,
                    },
                },
                {
                    runEvents: {
                        runEvents: [
                            {
                                result: { type: AssertionResultType.Failure },
                                timestampMillis: Date.now(),
                                asserteeUrn: 'test-urn',
                                assertionUrn: 'test-urn',
                                runId: 'test-run',
                                status: AssertionRunStatus.Complete,
                            },
                        ],
                        succeeded: 0,
                        failed: 1,
                        errored: 0,
                        total: 1,
                    },
                },
                {
                    runEvents: {
                        runEvents: [
                            {
                                result: { type: AssertionResultType.Error },
                                timestampMillis: Date.now(),
                                asserteeUrn: 'test-urn',
                                assertionUrn: 'test-urn',
                                runId: 'test-run',
                                status: AssertionRunStatus.Complete,
                            },
                        ],
                        succeeded: 0,
                        failed: 0,
                        errored: 1,
                        total: 1,
                    },
                },
            ];

            const summary = getAssertionsSummary(assertions as Assertion[]);
            expect(summary.passing).toBe(1);
            expect(summary.failing).toBe(1);
            expect(summary.erroring).toBe(1);
            expect(summary.total).toBe(3);
        });

        it('should skip inactive monitors', () => {
            const assertions: Partial<AssertionWithMonitorDetails>[] = [
                {
                    monitors: [
                        {
                            info: {
                                type: MonitorType.Assertion,
                                status: { mode: MonitorMode.Inactive },
                            },
                            urn: 'test-monitor-urn',
                            type: EntityType.Monitor,
                        },
                    ],
                    runEvents: {
                        runEvents: [
                            {
                                result: { type: AssertionResultType.Success },
                                timestampMillis: Date.now(),
                                asserteeUrn: 'test-urn',
                                assertionUrn: 'test-urn',
                                runId: 'test-run',
                                status: AssertionRunStatus.Complete,
                            },
                        ],
                        succeeded: 1,
                        failed: 0,
                        errored: 0,
                        total: 1,
                    },
                },
            ];

            const summary = getAssertionsSummary(assertions as Assertion[]);
            expect(summary.passing).toBe(0);
            expect(summary.total).toBe(0);
        });
    });

    describe('getLegacyAssertionsSummary', () => {
        it('should convert new summary format to legacy format', () => {
            const assertions: Partial<Assertion>[] = [
                {
                    runEvents: {
                        runEvents: [
                            {
                                result: { type: AssertionResultType.Success },
                                timestampMillis: Date.now(),
                                asserteeUrn: 'test-urn',
                                assertionUrn: 'test-urn',
                                runId: 'test-run',
                                status: AssertionRunStatus.Complete,
                            },
                        ],
                        succeeded: 1,
                        failed: 0,
                        errored: 0,
                        total: 1,
                    },
                },
                {
                    runEvents: {
                        runEvents: [
                            {
                                result: { type: AssertionResultType.Failure },
                                timestampMillis: Date.now(),
                                asserteeUrn: 'test-urn',
                                assertionUrn: 'test-urn',
                                runId: 'test-run',
                                status: AssertionRunStatus.Complete,
                            },
                        ],
                        succeeded: 0,
                        failed: 1,
                        errored: 0,
                        total: 1,
                    },
                },
            ];

            const summary = getLegacyAssertionsSummary(assertions as Assertion[]);
            expect(summary.succeededRuns).toBe(1);
            expect(summary.failedRuns).toBe(1);
            expect(summary.totalRuns).toBe(2);
        });
    });

    describe('getNextScheduleEvaluationTimeMs', () => {
        it('should return next evaluation time for valid cron schedule', () => {
            const schedule: CronSchedule = {
                cron: '0 0 * * *',
                timezone: 'UTC',
            };
            const nextTime = getNextScheduleEvaluationTimeMs(schedule);
            expect(nextTime).toBeDefined();
            expect(typeof nextTime).toBe('number');
        });

        it('should handle invalid cron expression', () => {
            const schedule: CronSchedule = {
                cron: 'invalid',
                timezone: 'UTC',
            };
            const nextTime = getNextScheduleEvaluationTimeMs(schedule);
            expect(nextTime).toBeUndefined();
        });
    });

    describe('getPreviousScheduleEvaluationTimeMs', () => {
        it('should return previous evaluation time for valid cron schedule', () => {
            const schedule: CronSchedule = {
                cron: '0 0 * * *',
                timezone: 'UTC',
            };
            const prevTime = getPreviousScheduleEvaluationTimeMs(schedule);
            expect(prevTime).toBeDefined();
            expect(typeof prevTime).toBe('number');
        });

        it('should handle invalid cron expression', () => {
            const schedule: CronSchedule = {
                cron: 'invalid',
                timezone: 'UTC',
            };
            const prevTime = getPreviousScheduleEvaluationTimeMs(schedule);
            expect(prevTime).toBeUndefined();
        });
    });

    describe('getAssertionTypesForEntityType', () => {
        it('should return enabled assertion types for dataset entity', () => {
            const types = getAssertionTypesForEntityType(EntityType.Dataset, true);
            expect(types.length).toBeGreaterThan(0);
            expect(
                types.every(
                    (type) => type.enabled || typeof type.requiresConnectionSupportedByMonitors === 'undefined',
                ),
            ).toBe(true);
        });

        it('should filter out types requiring connection when connection does not exist', () => {
            const types = getAssertionTypesForEntityType(EntityType.Dataset, false);
            expect(types.some((type) => !type.enabled)).toBe(true);
        });
    });

    describe('extractLatestGeneratedAt', () => {
        it('should return latest generated timestamp', () => {
            const monitor: Partial<Monitor> = {
                info: {
                    type: MonitorType.Assertion,
                    status: { mode: MonitorMode.Active },
                    assertionMonitor: {
                        assertions: [
                            {
                                assertion: {} as any,
                                schedule: {} as any,
                                context: {
                                    inferenceDetails: {
                                        generatedAt: new Date('2024-01-01').getTime(),
                                    },
                                },
                            },
                            {
                                assertion: {} as any,
                                schedule: {} as any,
                                context: {
                                    inferenceDetails: {
                                        generatedAt: new Date('2024-01-02').getTime(),
                                    },
                                },
                            },
                        ],
                    },
                },
            };

            const latest = extractLatestGeneratedAt(monitor as Monitor);
            expect(latest).toBe(new Date('2024-01-02').getTime());
        });

        it('should return undefined when no timestamps exist', () => {
            const monitor: Partial<Monitor> = {
                info: {
                    type: MonitorType.Assertion,
                    status: { mode: MonitorMode.Active },
                    assertionMonitor: {
                        assertions: [],
                    },
                },
            };

            const latest = extractLatestGeneratedAt(monitor as Monitor);
            expect(latest).toBeUndefined();
        });
    });

    describe('isMonitorActive', () => {
        it('should return true for active monitor', () => {
            const monitor: Partial<Monitor> = {
                info: {
                    type: MonitorType.Assertion,
                    status: {
                        mode: MonitorMode.Active,
                    },
                },
            };
            expect(isMonitorActive(monitor as Monitor)).toBe(true);
        });

        it('should return false for inactive monitor', () => {
            const monitor: Partial<Monitor> = {
                info: {
                    type: MonitorType.Assertion,
                    status: {
                        mode: MonitorMode.Inactive,
                    },
                },
            };
            expect(isMonitorActive(monitor as Monitor)).toBe(false);
        });
    });

    describe('getCronAsText', () => {
        it('should return human readable text for valid cron expression', () => {
            const result = getCronAsText('0 0 * * *');
            expect(result.text).toBeDefined();
            expect(result.error).toBe(false);
        });

        it('should handle invalid cron expression', () => {
            const result = getCronAsText('invalid');
            expect(result.text).toBeUndefined();
            expect(result.error).toBe(true);
        });
    });

    describe('canManageAssertionMonitor', () => {
        it('should return true when connection exists', () => {
            expect(canManageAssertionMonitor({}, true)).toBe(true);
        });

        it('should return true for datahub operation source', () => {
            const monitor = {
                info: {
                    assertionMonitor: {
                        assertions: [
                            {
                                parameters: {
                                    datasetFreshnessParameters: {
                                        sourceType: DatasetFreshnessSourceType.DatahubOperation,
                                    },
                                },
                            },
                        ],
                    },
                },
            };
            expect(canManageAssertionMonitor(monitor, false)).toBe(true);
        });

        it('should return true for datahub dataset profile source', () => {
            const monitor = {
                info: {
                    assertionMonitor: {
                        assertions: [
                            {
                                parameters: {
                                    datasetVolumeParameters: {
                                        sourceType: DatasetVolumeSourceType.DatahubDatasetProfile,
                                    },
                                },
                            },
                        ],
                    },
                },
            };
            expect(canManageAssertionMonitor(monitor, false)).toBe(true);
        });
    });

    describe('getEntityUrnForAssertion', () => {
        it('should return dataset urn for dataset assertion', () => {
            const assertion: Partial<Assertion> = {
                info: {
                    type: AssertionType.Dataset,
                    datasetAssertion: {
                        datasetUrn: 'test-urn',
                        operator: AssertionStdOperator.EqualTo,
                        scope: DatasetAssertionScope.DatasetRows,
                    },
                },
            };
            expect(getEntityUrnForAssertion(assertion as Assertion)).toBe('test-urn');
        });

        it('should return undefined for unrecognized assertion type', () => {
            const assertion: Partial<Assertion> = {
                info: {
                    type: 'UNRECOGNIZED' as AssertionType,
                },
            };
            expect(getEntityUrnForAssertion(assertion as Assertion)).toBeUndefined();
        });
    });

    describe('getSiblingWithUrn', () => {
        it('should return matching sibling entity', () => {
            const entityData = {
                urn: 'main-urn',
                siblingsSearch: {
                    searchResults: [
                        {
                            entity: {
                                urn: 'sibling-urn',
                                type: EntityType.Dataset,
                            },
                            matchedFields: [],
                        },
                    ],
                    count: 1,
                    total: 1,
                },
            };
            expect(getSiblingWithUrn(entityData, 'sibling-urn')).toBeDefined();
            expect(getSiblingWithUrn(entityData, 'sibling-urn')?.urn).toBe('sibling-urn');
        });

        it('should return undefined for non-matching urn', () => {
            const entityData = {
                urn: 'main-urn',
                siblingsSearch: {
                    searchResults: [
                        {
                            entity: {
                                urn: 'sibling-urn',
                                type: EntityType.Dataset,
                            },
                            matchedFields: [],
                        },
                    ],
                    count: 1,
                    total: 1,
                },
            };
            expect(getSiblingWithUrn(entityData, 'non-matching-urn')).toBeUndefined();
        });
    });

    describe('getSiblings', () => {
        it('should return array of sibling entities', () => {
            const entityData = {
                siblingsSearch: {
                    searchResults: [
                        {
                            entity: {
                                urn: 'sibling-1',
                                type: EntityType.Dataset,
                            },
                            matchedFields: [],
                        },
                        {
                            entity: {
                                urn: 'sibling-2',
                                type: EntityType.Dataset,
                            },
                            matchedFields: [],
                        },
                    ],
                    count: 2,
                    total: 2,
                },
            };
            const siblings = getSiblings(entityData);
            expect(siblings).toHaveLength(2);
            expect(siblings.map((s) => s.urn)).toEqual(['sibling-1', 'sibling-2']);
        });

        it('should return empty array for null entity data', () => {
            expect(getSiblings(null)).toEqual([]);
        });
    });

    describe('createAssertionGroups', () => {
        it('should group assertions by type', () => {
            const assertions: Partial<Assertion>[] = [
                {
                    info: {
                        type: AssertionType.Freshness,
                    },
                    runEvents: {
                        runEvents: [
                            {
                                result: { type: AssertionResultType.Success },
                                timestampMillis: Date.now(),
                                asserteeUrn: 'test-urn',
                                assertionUrn: 'test-urn',
                                runId: 'test-run',
                                status: AssertionRunStatus.Complete,
                            },
                        ],
                        succeeded: 1,
                        failed: 0,
                        errored: 0,
                        total: 1,
                    },
                },
                {
                    info: {
                        type: AssertionType.Volume,
                    },
                    runEvents: {
                        runEvents: [
                            {
                                result: { type: AssertionResultType.Failure },
                                timestampMillis: Date.now(),
                                asserteeUrn: 'test-urn',
                                assertionUrn: 'test-urn',
                                runId: 'test-run',
                                status: AssertionRunStatus.Complete,
                            },
                        ],
                        succeeded: 0,
                        failed: 1,
                        errored: 0,
                        total: 1,
                    },
                },
            ];

            const groups = createAssertionGroups(assertions as Assertion[]);
            expect(groups).toHaveLength(2);
            expect(groups[0].type).toBe('FRESHNESS');
            expect(groups[1].type).toBe('VOLUME');
            expect(groups[0].assertions).toHaveLength(1);
            expect(groups[1].assertions).toHaveLength(1);
        });

        it('should handle assertions without type', () => {
            const assertions: Partial<Assertion>[] = [
                {
                    info: {
                        type: AssertionType.Freshness,
                    },
                    runEvents: {
                        runEvents: [
                            {
                                result: { type: AssertionResultType.Success },
                                timestampMillis: Date.now(),
                                asserteeUrn: 'test-urn',
                                assertionUrn: 'test-urn',
                                runId: 'test-run',
                                status: AssertionRunStatus.Complete,
                            },
                        ],
                        succeeded: 1,
                        failed: 0,
                        errored: 0,
                        total: 1,
                    },
                },
            ];

            const groups = createAssertionGroups(assertions as Assertion[]);
            expect(groups).toHaveLength(1);
            expect(groups[0].type).toBe('FRESHNESS');
        });

        it('should sort assertions by most recent execution', () => {
            const now = Date.now();
            const assertions: Partial<Assertion>[] = [
                {
                    info: {
                        type: AssertionType.Freshness,
                    },
                    runEvents: {
                        runEvents: [
                            {
                                result: { type: AssertionResultType.Success },
                                timestampMillis: now - 1000,
                                asserteeUrn: 'test-urn',
                                assertionUrn: 'test-urn',
                                runId: 'test-run',
                                status: AssertionRunStatus.Complete,
                            },
                        ],
                        succeeded: 1,
                        failed: 0,
                        errored: 0,
                        total: 1,
                    },
                },
                {
                    info: {
                        type: AssertionType.Freshness,
                    },
                    runEvents: {
                        runEvents: [
                            {
                                result: { type: AssertionResultType.Failure },
                                timestampMillis: now,
                                asserteeUrn: 'test-urn',
                                assertionUrn: 'test-urn',
                                runId: 'test-run',
                                status: AssertionRunStatus.Complete,
                            },
                        ],
                        succeeded: 0,
                        failed: 1,
                        errored: 0,
                        total: 1,
                    },
                },
            ];

            const groups = createAssertionGroups(assertions as Assertion[]);
            expect(groups[0].assertions[0].runEvents?.runEvents?.[0].timestampMillis).toBe(now);
            expect(groups[0].assertions[1].runEvents?.runEvents?.[0].timestampMillis).toBe(now - 1000);
        });
    });

    describe('getAssertionGroupSummaryMessage', () => {
        it('should return "No assertions have run" when total is 0', () => {
            const summary = {
                passing: 0,
                failing: 0,
                erroring: 0,
                total: 0,
                totalAssertions: 0,
            };
            expect(getAssertionGroupSummaryMessage(summary)).toBe('No assertions have run');
        });

        it('should return "All assertions are passing" when all pass', () => {
            const summary = {
                passing: 3,
                failing: 0,
                erroring: 0,
                total: 3,
                totalAssertions: 3,
            };
            expect(getAssertionGroupSummaryMessage(summary)).toBe('All assertions are passing');
        });

        it('should return "An error is preventing some assertions from running" when there are errors', () => {
            const summary = {
                passing: 2,
                failing: 0,
                erroring: 1,
                total: 3,
                totalAssertions: 3,
            };
            expect(getAssertionGroupSummaryMessage(summary)).toBe(
                'An error is preventing some assertions from running',
            );
        });

        it('should return "All assertions are failing" when all fail', () => {
            const summary = {
                passing: 0,
                failing: 3,
                erroring: 0,
                total: 3,
                totalAssertions: 3,
            };
            expect(getAssertionGroupSummaryMessage(summary)).toBe('All assertions are failing');
        });

        it('should return "Some assertions are failing" when some fail', () => {
            const summary = {
                passing: 2,
                failing: 1,
                erroring: 0,
                total: 3,
                totalAssertions: 3,
            };
            expect(getAssertionGroupSummaryMessage(summary)).toBe('Some assertions are failing');
        });
    });
});
