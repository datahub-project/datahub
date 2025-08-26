import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { getTimeFilters } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/filterQueries';
import { CREATED_TIME_FIELD, LAST_MODIFIED_TIME_FIELD } from '@app/searchV2/context/constants';

import { FilterOperator } from '@types';

describe('Query Utils', () => {
    describe('getTimeFilters', () => {
        beforeEach(() => {
            // Mock Date to ensure consistent test results
            vi.useFakeTimers();
            vi.setSystemTime(new Date('2024-01-15T10:30:00.000Z'));
        });

        afterEach(() => {
            vi.useRealTimers();
        });

        it('should create time filters for default 30 days', () => {
            const result = getTimeFilters();

            // Get the mocked "today" at start of day
            const today = new Date('2024-01-15T10:30:00.000Z');
            today.setHours(0, 0, 0, 0);
            const expectedTimestamp = today.getTime() - 30 * 24 * 60 * 60 * 1000;

            expect(result.createdAtFilter).toEqual({
                field: CREATED_TIME_FIELD,
                condition: FilterOperator.GreaterThanOrEqualTo,
                values: [expectedTimestamp.toString()],
            });

            expect(result.lastModifiedAtFilter).toEqual({
                field: LAST_MODIFIED_TIME_FIELD,
                condition: FilterOperator.GreaterThanOrEqualTo,
                values: [expectedTimestamp.toString()],
            });
        });

        it('should create time filters for custom days', () => {
            const result = getTimeFilters(7);

            const today = new Date('2024-01-15T10:30:00.000Z');
            today.setHours(0, 0, 0, 0);
            const expectedTimestamp = today.getTime() - 7 * 24 * 60 * 60 * 1000;

            expect(result.createdAtFilter.values[0]).toBe(expectedTimestamp.toString());
            expect(result.lastModifiedAtFilter.values[0]).toBe(expectedTimestamp.toString());
        });

        it('should handle 0 days (today)', () => {
            const result = getTimeFilters(0);

            const today = new Date('2024-01-15T10:30:00.000Z');
            today.setHours(0, 0, 0, 0);
            const expectedTimestamp = today.getTime();

            expect(result.createdAtFilter.values[0]).toBe(expectedTimestamp.toString());
            expect(result.lastModifiedAtFilter.values[0]).toBe(expectedTimestamp.toString());
        });

        it('should handle large number of days', () => {
            const result = getTimeFilters(365);

            const today = new Date('2024-01-15T10:30:00.000Z');
            today.setHours(0, 0, 0, 0);
            const expectedTimestamp = today.getTime() - 365 * 24 * 60 * 60 * 1000;

            expect(result.createdAtFilter.values[0]).toBe(expectedTimestamp.toString());
            expect(result.lastModifiedAtFilter.values[0]).toBe(expectedTimestamp.toString());
        });

        it('should handle negative days (future dates)', () => {
            const result = getTimeFilters(-10);

            const today = new Date('2024-01-15T10:30:00.000Z');
            today.setHours(0, 0, 0, 0);
            const expectedTimestamp = today.getTime() + 10 * 24 * 60 * 60 * 1000;

            expect(result.createdAtFilter.values[0]).toBe(expectedTimestamp.toString());
            expect(result.lastModifiedAtFilter.values[0]).toBe(expectedTimestamp.toString());
        });

        it('should use correct filter operator', () => {
            const result = getTimeFilters();

            expect(result.createdAtFilter.condition).toBe(FilterOperator.GreaterThanOrEqualTo);
            expect(result.lastModifiedAtFilter.condition).toBe(FilterOperator.GreaterThanOrEqualTo);
        });

        it('should normalize time to start of day', () => {
            // Set time to middle of day
            vi.setSystemTime(new Date('2024-01-15T15:45:30.123Z'));

            const result = getTimeFilters(1);

            // Should normalize to start of day (00:00:00.000)
            const today = new Date('2024-01-15T15:45:30.123Z');
            today.setHours(0, 0, 0, 0);
            const expectedTimestamp = today.getTime() - 1 * 24 * 60 * 60 * 1000;

            expect(result.createdAtFilter.values[0]).toBe(expectedTimestamp.toString());
        });
    });
});
