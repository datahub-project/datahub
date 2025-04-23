import { describe, expect, it } from 'vitest';

import {
    computeAverageUpdateFrequencyInMillis,
    getFormattedTimeStringTimeSince,
    mostRecentOperationsTimeSinceInMillis,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/freshness/utils';

import { Operation, OperationType } from '@types';

describe('freshnessUtils', () => {
    describe('computeAverageUpdateFrequencyInMillis', () => {
        it('should compute average with 3 operations', () => {
            describe('freshnessUtils', () => {
                describe('computeAverageUpdateFrequencyInMillis', () => {
                    it('should compute average with 3 operations', () => {
                        const operations: Operation[] = [
                            { lastUpdatedTimestamp: 1000, timestampMillis: 1000, operationType: OperationType.Update },
                            { lastUpdatedTimestamp: 2000, timestampMillis: 2000, operationType: OperationType.Update },
                            { lastUpdatedTimestamp: 4000, timestampMillis: 4000, operationType: OperationType.Update },
                        ];
                        const result = computeAverageUpdateFrequencyInMillis(operations);
                        expect(result).toBe(1500); // (2000-1000 + 4000-2000) / 2
                    });

                    it('should return undefined for an empty array', () => {
                        const operations: Operation[] = [];
                        const result = computeAverageUpdateFrequencyInMillis(operations);
                        expect(result).toBeUndefined();
                    });

                    it('should return NaN for a single timestamp', () => {
                        const operations: Operation[] = [
                            { lastUpdatedTimestamp: 1000, timestampMillis: 1000, operationType: OperationType.Update },
                        ];
                        const result = computeAverageUpdateFrequencyInMillis(operations);
                        expect(result).toBeNaN();
                    });

                    it('should handle timestamps in descending order', () => {
                        const operations: Operation[] = [
                            { lastUpdatedTimestamp: 4000, timestampMillis: 4000, operationType: OperationType.Update },
                            { lastUpdatedTimestamp: 2000, timestampMillis: 2000, operationType: OperationType.Update },
                            { lastUpdatedTimestamp: 1000, timestampMillis: 1000, operationType: OperationType.Update },
                        ];
                        const result = computeAverageUpdateFrequencyInMillis(operations);
                        expect(result).toBe(1500); // (4000-2000 + 2000-1000) / 2
                    });

                    it('should handle timestamps with irregular intervals', () => {
                        const operations: Operation[] = [
                            { lastUpdatedTimestamp: 7000, timestampMillis: 7000, operationType: OperationType.Update },
                            { lastUpdatedTimestamp: 3000, timestampMillis: 3000, operationType: OperationType.Update },
                            { lastUpdatedTimestamp: 1000, timestampMillis: 1000, operationType: OperationType.Update },
                        ];
                        const result = computeAverageUpdateFrequencyInMillis(operations);
                        expect(result).toBe(3000); // (3000-1000 + 7000-3000) / 2
                    });
                });
            });
        });
    });
    describe('mostRecentOperationsTimeSinceInMillis', () => {
        describe('mostRecentOperationsTimeSinceInMillis', () => {
            it('should return the time difference for the most recent operation', () => {
                const now = Date.now();
                const operations: Operation[] = [
                    {
                        lastUpdatedTimestamp: now - 5000,
                        timestampMillis: now - 5000,
                        operationType: OperationType.Update,
                    },
                    {
                        lastUpdatedTimestamp: now - 10000,
                        timestampMillis: now - 10000,
                        operationType: OperationType.Update,
                    },
                    {
                        lastUpdatedTimestamp: now - 20000,
                        timestampMillis: now - 20000,
                        operationType: OperationType.Update,
                    },
                ];
                const result = mostRecentOperationsTimeSinceInMillis(operations, 1);
                expect(result).toStrictEqual([{ delta: 5000, lastUpdatedTimestamp: now - 5000 }]);
            });

            it('should return an empty array for an empty array input', () => {
                const now = Date.now();
                const operations: Operation[] = [];
                const result = mostRecentOperationsTimeSinceInMillis(operations, now);
                expect(result).toStrictEqual([]);
            });

            it('should handle operations with identical timestamps', () => {
                const now = Date.now();
                const operations: Operation[] = [
                    {
                        lastUpdatedTimestamp: now - 5000,
                        timestampMillis: now - 5000,
                        operationType: OperationType.Update,
                    },
                    {
                        lastUpdatedTimestamp: now - 5000,
                        timestampMillis: now - 5000,
                        operationType: OperationType.Update,
                    },
                ];
                const result = mostRecentOperationsTimeSinceInMillis(operations, 2);
                expect(result).toStrictEqual([
                    { delta: 5000, lastUpdatedTimestamp: now - 5000 },
                    { delta: 5000, lastUpdatedTimestamp: now - 5000 },
                ]);
            });

            it('should handle operations with descending timestamps', () => {
                const now = Date.now();
                const operations: Operation[] = [
                    {
                        lastUpdatedTimestamp: now - 20000,
                        timestampMillis: now - 20000,
                        operationType: OperationType.Update,
                    },
                    {
                        lastUpdatedTimestamp: now - 10000,
                        timestampMillis: now - 10000,
                        operationType: OperationType.Update,
                    },
                    {
                        lastUpdatedTimestamp: now - 5000,
                        timestampMillis: now - 5000,
                        operationType: OperationType.Update,
                    },
                ];
                const result = mostRecentOperationsTimeSinceInMillis(operations, 1);
                expect(result).toStrictEqual([{ delta: 20000, lastUpdatedTimestamp: now - 20000 }]);
            });
        });
    });
    describe('getFormattedTimeStringTimeSince', () => {
        describe('getFormattedTimeStringTimeSince', () => {
            it.each([
                [0, 10, 'now'],
                [0, 1000, '1 second ago'],
                [0, 10000, '10 seconds ago'],
                [0, 10500, '10.5 seconds ago'],
                [0, 60000, '1 minute ago'],
                [0, 90000, '1.5 minutes ago'],
                [0, 600000, '10 minutes ago'],
                [0, 630000, '10.5 minutes ago'],
                [0, 3600000, '1 hour ago'],
                [0, 5400000, '1.5 hours ago'],
                [0, 36000000, '10 hours ago'],
                [0, 86400000, 'yesterday'],
                [0, 129600000, 'yesterday'],
                [0, 172800000, '2 days ago'],
                [0, 216000000, '2.5 days ago'],
                [0, 864000000, '1.4 weeks ago'],
                [0, 8640000000, '14.3 weeks ago'],
                [0, 86400000000, '2.7 years ago'],
                [0, 864000000000, '27.4 years ago'],
            ])('should format %d milliseconds as "%s"', (earlierTimestamp, laterTimestamp, expected) => {
                const result = getFormattedTimeStringTimeSince(earlierTimestamp, laterTimestamp);
                expect(result).toBe(expected);
            });
        });
    });
});
