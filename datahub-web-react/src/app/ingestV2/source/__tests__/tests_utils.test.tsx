import dayjs from 'dayjs';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';
import { afterAll, beforeEach, describe, expect, test, vi } from 'vitest';

import { SortingState } from '@components/components/Table/types';

import {
    EXECUTION_REQUEST_STATUS_LOADING,
    EXECUTION_REQUEST_STATUS_PENDING,
    EXECUTION_REQUEST_STATUS_SUCCESS,
} from '@app/ingestV2/executions/constants';
import {
    buildOwnerEntities,
    capitalizeMonthsAndDays,
    formatExtraArgs,
    formatTimezone,
    getAspectsBySubtypes,
    getEntitiesIngestedByTypeOrSubtype,
    getIngestionContents,
    getIngestionSourceMutationInput,
    getNewIngestionSourcePlaceholder,
    getOtherIngestionContents,
    getSortInput,
    getSourceStatus,
    getStructuredReport,
    getTotalEntitiesIngested,
} from '@app/ingestV2/source/utils';

import {
    EntityType,
    ExecutionRequest,
    ExecutionRequestResult,
    IngestionSource,
    OwnershipTypeEntity,
    SortOrder,
} from '@types';

// Mock entity registry for tests
const mockEntityRegistry = {
    getSearchEntityTypesAsCamelCase: () => [
        'dataset',
        'container',
        'dashboard',
        'chart',
        'dataJob',
        'dataFlow',
        'mlModel',
        'mlModelGroup',
        'mlPrimaryKey',
        'mlFeature',
        'mlFeatureTable',
        'glossaryTerm',
        'tag',
        'corpUser',
        'corpGroup',
        'domain',
        'notebook',
    ],
} as any;

// Extend dayjs with required plugins
dayjs.extend(utc);
dayjs.extend(timezone);

// Mock the structuredReport property of ExecutionRequestResult
const mockExecutionRequestResult = (structuredReportData: any): Partial<ExecutionRequestResult> => {
    return {
        structuredReport: {
            serializedValue: JSON.stringify(structuredReportData),
        },
    } as Partial<ExecutionRequestResult>;
};

describe('getEntitiesIngestedByTypeOrSubtype', () => {
    // Mock for console.error
    const originalConsoleError = console.error;
    console.error = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
    });

    afterAll(() => {
        console.error = originalConsoleError;
    });

    test('returns null when structured report is not available', () => {
        const result = getEntitiesIngestedByTypeOrSubtype({} as Partial<ExecutionRequestResult>, mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('returns null when an exception occurs during processing', () => {
        // Create a malformed structured report to trigger an exception
        const malformedReport = {
            source: {
                report: {
                    // Missing aspects property to trigger exception
                },
            },
        };

        const result = getEntitiesIngestedByTypeOrSubtype(
            mockExecutionRequestResult(malformedReport),
            mockEntityRegistry,
        );
        expect(result).toBeNull();
    });

    test('correctly extracts entity counts from structured report', () => {
        // Create a structured report based on the example in the comments
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        container: {
                            unknown: {
                                containerProperties: 156,
                                container: 117,
                                status: 156,
                            },
                        },
                        dataset: {
                            unknown: {
                                status: 1505,
                                schemaMetadata: 1505,
                                datasetProperties: 1505,
                                container: 1505,
                                operation: 1521,
                            },
                        },
                    },
                },
            },
        };

        const result = getEntitiesIngestedByTypeOrSubtype(
            mockExecutionRequestResult(structuredReport),
            mockEntityRegistry,
        );

        expect(result).toEqual([
            {
                count: 156,
                displayName: 'container',
            },
            {
                count: 1505,
                displayName: 'dataset',
            },
        ]);
    });

    test('handles empty aspects object', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {},
                },
            },
        };

        const result = getEntitiesIngestedByTypeOrSubtype(
            mockExecutionRequestResult(structuredReport),
            mockEntityRegistry,
        );
        expect(result).toBeNull();
    });

    test('handles aspects with non-numeric values', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        container: {
                            unknown: {
                                containerProperties: '156',
                                container: 117,
                                status: 156,
                            },
                        },
                    },
                },
            },
        };

        const result = getEntitiesIngestedByTypeOrSubtype(
            mockExecutionRequestResult(structuredReport),
            mockEntityRegistry,
        );
        expect(result).toEqual([
            {
                count: 156,
                displayName: 'container',
            },
        ]);
    });
});

describe('getAspectsBySubtypes', () => {
    test('returns null when aspects_by_subtypes is not present', () => {
        const structuredReportObject = {
            source: {
                report: {
                    // Missing aspects_by_subtypes property
                },
            },
        };

        const result = getAspectsBySubtypes(structuredReportObject, mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('returns null when structured report object is null', () => {
        const result = getAspectsBySubtypes(null, mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('returns null when structured report object is undefined', () => {
        const result = getAspectsBySubtypes(undefined, mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('returns null when source is missing', () => {
        const structuredReportObject = {
            // Missing source property
        };

        const result = getAspectsBySubtypes(structuredReportObject, mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('returns null when report is missing', () => {
        const structuredReportObject = {
            source: {
                // Missing report property
            },
        };

        const result = getAspectsBySubtypes(structuredReportObject, mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('filters out entities without search card', () => {
        const structuredReportObject = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        // Entities that should be kept (not in entitesWithoutSearchCard list)
                        dataset: {
                            Table: {
                                status: 100,
                                schemaMetadata: 100,
                            },
                        },
                        container: {
                            Container: {
                                status: 50,
                                containerProperties: 50,
                            },
                        },
                        dashboard: {
                            Dashboard: {
                                status: 25,
                                dashboardInfo: 25,
                            },
                        },
                        // Entities that should be filtered out (in entitesWithoutSearchCard list)
                        dataPlatform: {
                            Platform: {
                                status: 10,
                                platformProperties: 10,
                            },
                        },
                        role: {
                            Role: {
                                status: 5,
                                roleProperties: 5,
                            },
                        },
                        dataHubPolicy: {
                            Policy: {
                                status: 3,
                                policyProperties: 3,
                            },
                        },
                        schemaField: {
                            Field: {
                                status: 200,
                                fieldProperties: 200,
                            },
                        },
                        assertion: {
                            Assertion: {
                                status: 15,
                                assertionProperties: 15,
                            },
                        },
                    },
                },
            },
        };

        const result = getAspectsBySubtypes(structuredReportObject, mockEntityRegistry);

        // Should only contain entities that are NOT in the entitesWithoutSearchCard list
        expect(result).toEqual({
            dataset: {
                Table: {
                    status: 100,
                    schemaMetadata: 100,
                },
            },
            container: {
                Container: {
                    status: 50,
                    containerProperties: 50,
                },
            },
            dashboard: {
                Dashboard: {
                    status: 25,
                    dashboardInfo: 25,
                },
            },
        });
    });

    test('returns all entities when none are in the filter list', () => {
        const structuredReportObject = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        dataset: {
                            Table: {
                                status: 100,
                                schemaMetadata: 100,
                            },
                        },
                        container: {
                            Container: {
                                status: 50,
                                containerProperties: 50,
                            },
                        },
                        dashboard: {
                            Dashboard: {
                                status: 25,
                                dashboardInfo: 25,
                            },
                        },
                    },
                },
            },
        };

        const result = getAspectsBySubtypes(structuredReportObject, mockEntityRegistry);

        expect(result).toEqual({
            dataset: {
                Table: {
                    status: 100,
                    schemaMetadata: 100,
                },
            },
            container: {
                Container: {
                    status: 50,
                    containerProperties: 50,
                },
            },
            dashboard: {
                Dashboard: {
                    status: 25,
                    dashboardInfo: 25,
                },
            },
        });
    });

    test('returns empty object when all entities are filtered out', () => {
        const structuredReportObject = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        dataPlatform: {
                            Platform: {
                                status: 10,
                                platformProperties: 10,
                            },
                        },
                        role: {
                            Role: {
                                status: 5,
                                roleProperties: 5,
                            },
                        },
                        schemaField: {
                            Field: {
                                status: 200,
                                fieldProperties: 200,
                            },
                        },
                    },
                },
            },
        };

        const result = getAspectsBySubtypes(structuredReportObject, mockEntityRegistry);

        expect(result).toEqual({});
    });

    test('handles empty aspects_by_subtypes object', () => {
        const structuredReportObject = {
            source: {
                report: {
                    aspects_by_subtypes: {},
                },
            },
        };

        const result = getAspectsBySubtypes(structuredReportObject, mockEntityRegistry);

        expect(result).toEqual({});
    });
});

describe('getSortInput', () => {
    it('returns undefined for original sorting', () => {
        expect(getSortInput('name', SortingState.ORIGINAL)).toBeUndefined();
    });

    it('returns ascending sort input', () => {
        expect(getSortInput('name', SortingState.ASCENDING)).toEqual({
            sortOrder: SortOrder.Ascending,
            field: 'name',
        });
    });

    it('returns descending sort input', () => {
        expect(getSortInput('name', SortingState.DESCENDING)).toEqual({
            sortOrder: SortOrder.Descending,
            field: 'name',
        });
    });
});

describe('getTotalEntitiesIngested', () => {
    test('returns null when structured report is not available', () => {
        const result = getTotalEntitiesIngested({} as Partial<ExecutionRequestResult>, mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('returns null when an exception occurs during processing', () => {
        // Create a malformed structured report to trigger an exception
        const malformedReport = {
            source: {
                report: {
                    // Missing aspects_by_subtypes property to trigger exception
                },
            },
        };

        const result = getTotalEntitiesIngested(mockExecutionRequestResult(malformedReport), mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('returns null when aspects object is empty', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {},
                },
            },
        };

        const result = getTotalEntitiesIngested(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('correctly calculates total from multiple entity types', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        container: {
                            unknown: {
                                containerProperties: 156,
                                container: 117,
                                status: 156,
                            },
                        },
                        dataset: {
                            unknown: {
                                status: 1505,
                                schemaMetadata: 1505,
                                datasetProperties: 1505,
                                container: 1505,
                                operation: 1521,
                            },
                        },
                        dashboard: {
                            unknown: {
                                status: 42,
                                dashboardInfo: 42,
                            },
                        },
                    },
                },
            },
        };

        const result = getTotalEntitiesIngested(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toBe(156 + 1505 + 42); // 1703
    });

    test('correctly calculates total from single entity type', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        container: {
                            unknown: {
                                containerProperties: 156,
                                container: 117,
                                status: 156,
                            },
                        },
                    },
                },
            },
        };

        const result = getTotalEntitiesIngested(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toBe(156);
    });

    test('handles aspects with non-numeric values', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        container: {
                            unknown: {
                                containerProperties: '156',
                                container: 117,
                                status: 156,
                            },
                        },
                    },
                },
            },
        };

        const result = getTotalEntitiesIngested(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toBe(156);
    });
});

describe('formatTimezone', () => {
    it('should return undefined for null input', () => {
        expect(formatTimezone(null)).toBeUndefined();
    });

    it('should return undefined for undefined input', () => {
        expect(formatTimezone(undefined)).toBeUndefined();
    });

    it('should format valid timezone correctly', () => {
        // Mock the current time to ensure consistent testing
        const mockDate = new Date('2024-01-01T12:00:00Z');
        vi.spyOn(global.Date, 'now').mockImplementation(() => mockDate.getTime());

        // Test timezone abbreviations that can vary based on DST
        const nycAbbr = formatTimezone('America/New_York');
        expect(['EST', 'EDT']).toContain(nycAbbr);

        const londonAbbr = formatTimezone('Europe/London');
        expect(['GMT', 'BST']).toContain(londonAbbr);

        // Tokyo doesn't observe DST, so it's always GMT+9
        expect(formatTimezone('Asia/Tokyo')).toBe('GMT+9');

        // Clean up
        vi.restoreAllMocks();
    });

    it('should handle invalid timezone gracefully', () => {
        // Mock the current time to ensure consistent testing
        const mockDate = new Date('2024-01-01T12:00:00Z');
        vi.spyOn(global.Date, 'now').mockImplementation(() => mockDate.getTime());

        // Invalid timezone should return undefined or throw an error
        expect(() => formatTimezone('Invalid/Timezone')).toThrow();

        // Clean up
        vi.restoreAllMocks();
    });
});

describe('capitalizeMonthsAndDays', () => {
    it('should capitalize month names', () => {
        const input = 'january february march april may june july august september october november december';
        const expected = 'January February March April May June July August September October November December';
        expect(capitalizeMonthsAndDays(input)).toBe(expected);
    });

    it('should capitalize day names', () => {
        const input = 'monday tuesday wednesday thursday friday saturday sunday';
        const expected = 'Monday Tuesday Wednesday Thursday Friday Saturday Sunday';
        expect(capitalizeMonthsAndDays(input)).toBe(expected);
    });

    it('should handle mixed case input', () => {
        const input = 'monday January tuesday February';
        const expected = 'Monday January Tuesday February';
        expect(capitalizeMonthsAndDays(input)).toBe(expected);
    });

    it('should not capitalize non-month/day words', () => {
        const input = 'hello world monday january test';
        const expected = 'hello world Monday January test';
        expect(capitalizeMonthsAndDays(input)).toBe(expected);
    });

    it('should handle empty string', () => {
        expect(capitalizeMonthsAndDays('')).toBe('');
    });

    it('should handle string with no month or day names', () => {
        const input = 'this is a test string';
        expect(capitalizeMonthsAndDays(input)).toBe(input);
    });

    it('should handle string with special characters', () => {
        const input = 'monday, january 1st - tuesday, february 2nd';
        const expected = 'Monday, January 1st - Tuesday, February 2nd';
        expect(capitalizeMonthsAndDays(input)).toBe(expected);
    });
});

describe('getSourceStatus', () => {
    const urn = 'urn:li:source:123';

    const createSource = (sourceUrn: string, requests: ExecutionRequest[] = []): IngestionSource => ({
        urn: sourceUrn,
        executions: {
            executionRequests: requests,
        },
        config: {
            executorId: 'executorId',
            recipe: '',
        },
        name: 'source',
        type: 'snowflake',
    });

    const createExecutionRequest = (overrides: Partial<ExecutionRequest> = {}): ExecutionRequest => ({
        result: { status: EXECUTION_REQUEST_STATUS_SUCCESS },
        id: 'request',
        urn: 'urn:li:request',
        type: EntityType.ExecutionRequest,
        input: {
            requestedAt: 0,
            source: {
                type: 'INGESTION_SOURCE',
            },
            task: '',
        },
        ...overrides,
    });

    it('returns Pending when polling and no requests but did not execute', () => {
        const source = createSource(urn, []);
        const result = getSourceStatus(source, new Set([urn]), new Set());
        expect(result).toBe(EXECUTION_REQUEST_STATUS_PENDING);
    });

    it('returns previous status when polling with no active request but did not execute', () => {
        const inactiveRequest = createExecutionRequest({ result: { status: EXECUTION_REQUEST_STATUS_SUCCESS } });
        const source = createSource(urn, [inactiveRequest]);
        const result = getSourceStatus(source, new Set([urn]), new Set());
        expect(result).toBe(EXECUTION_REQUEST_STATUS_SUCCESS);
    });

    it('returns Loading when recently executed but no active requests', () => {
        const source = createSource(urn, []);
        const result = getSourceStatus(source, new Set(), new Set([urn]));
        expect(result).toBe(EXECUTION_REQUEST_STATUS_LOADING);
    });

    it('returns Success from the last request when not polling', () => {
        const source = createSource(urn, [
            createExecutionRequest({ result: { status: EXECUTION_REQUEST_STATUS_SUCCESS } }),
        ]);
        const result = getSourceStatus(source, new Set(), new Set());
        expect(result).toBe(EXECUTION_REQUEST_STATUS_SUCCESS);
    });

    it('returns Pending when not polling, not executed, and no requests', () => {
        const source = createSource(urn);
        const result = getSourceStatus(source, new Set(), new Set());
        expect(result).toBe(EXECUTION_REQUEST_STATUS_PENDING);
    });

    it('returns Pending when not polling, not executed and no request result,', () => {
        const source = createSource(urn, [createExecutionRequest({ result: undefined })]);
        const result = getSourceStatus(source, new Set(), new Set());
        expect(result).toBe(EXECUTION_REQUEST_STATUS_PENDING);
    });
});

describe('getIngestionContents', () => {
    test('returns null when structured report is not available', () => {
        const result = getIngestionContents({} as Partial<ExecutionRequestResult>, mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('returns null when aspects_by_subtypes is empty', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {},
                },
            },
        };

        const result = getIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('processes dataset subtypes with lineage information correctly', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        container: {
                            containerProperties: 156,
                            container: 117,
                        },
                        dataset: {
                            Table: {
                                status: 10,
                                upstreamLineage: 5,
                                datasetProfile: 10,
                            },
                            View: {
                                status: 20,
                                upstreamLineage: 10,
                                datasetProfile: 20,
                            },
                        },
                    },
                },
            },
        };

        const result = getIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toEqual([
            {
                title: 'Table',
                count: 5,
                percent: '50%',
            },
            {
                title: 'View',
                count: 10,
                percent: '50%',
            },
        ]);
    });

    test('filters out subtypes with 0% lineage', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        dataset: {
                            Table: {
                                status: 10,
                                upstreamLineage: 0,
                            },
                            View: {
                                status: 20,
                                upstreamLineage: 5,
                            },
                        },
                    },
                },
            },
        };

        const result = getIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toEqual([
            {
                title: 'View',
                count: 5,
                percent: '25%',
            },
        ]);
    });

    test('filters out subtypes with status count of 0', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        dataset: {
                            Table: {
                                status: 0,
                                upstreamLineage: 5,
                            },
                            View: {
                                status: 20,
                                upstreamLineage: 10,
                            },
                        },
                    },
                },
            },
        };

        const result = getIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toEqual([
            {
                title: 'View',
                count: 10,
                percent: '50%',
            },
        ]);
    });

    test('handles missing upstreamLineage property', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        dataset: {
                            Table: {
                                status: 10,
                                // upstreamLineage is missing
                            },
                        },
                    },
                },
            },
        };

        const result = getIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('handles missing status property', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        dataset: {
                            Table: {
                                // status is missing
                                upstreamLineage: 5,
                            },
                        },
                    },
                },
            },
        };

        const result = getIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('calculates percentage correctly and rounds to nearest integer', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        dataset: {
                            Table: {
                                status: 7,
                                upstreamLineage: 2, // 2/7 = 28.57...% rounds to 29%
                            },
                            View: {
                                status: 3,
                                upstreamLineage: 1, // 1/3 = 33.33...% rounds to 33%
                            },
                        },
                    },
                },
            },
        };

        const result = getIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toEqual([
            {
                title: 'Table',
                count: 2,
                percent: '29%',
            },
            {
                title: 'View',
                count: 1,
                percent: '33%',
            },
        ]);
    });
});

describe('getOtherIngestionContents', () => {
    test('returns null when structured report is not available', () => {
        const result = getOtherIngestionContents({} as Partial<ExecutionRequestResult>, mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('returns null when aspects_by_subtypes is empty', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {},
                },
            },
        };

        const result = getOtherIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toBeNull();
    });

    test('processes multiple dataset subtypes and aggregates profiling and usage entries', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        dataset: {
                            Table: {
                                status: 10,
                                datasetProfile: 5,
                                datasetUsageStatistics: 3,
                            },
                            View: {
                                status: 20,
                                datasetProfile: 8,
                                datasetUsageStatistics: 12,
                            },
                        },
                    },
                },
            },
        };

        const result = getOtherIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toEqual([
            {
                type: 'Profiling',
                count: 13, // 5 + 8
                percent: '43%', // (13 / 30) * 100 = 43.33...% rounds to 43%
            },
            {
                type: 'Usage',
                count: 15, // 3 + 12
                percent: '50%', // (15 / 30) * 100 = 50%
            },
        ]);
    });

    test('filters out subtypes with zero profiling and usage counts', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        dataset: {
                            Table: {
                                status: 10,
                                datasetProfile: 0,
                                datasetUsageStatistics: 0,
                            },
                            View: {
                                status: 20,
                                datasetProfile: 8,
                                datasetUsageStatistics: 12,
                            },
                        },
                    },
                },
            },
        };

        const result = getOtherIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toEqual([
            {
                type: 'Profiling',
                count: 8,
                percent: '27%', // (8 / 30) * 100 = 26.66...% rounds to 27%
            },
            {
                type: 'Usage',
                count: 12,
                percent: '40%', // (12 / 30) * 100 = 40%
            },
        ]);
    });

    test('filters out subtypes with zero status count', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        dataset: {
                            Table: {
                                status: 0,
                                datasetProfile: 5,
                                datasetUsageStatistics: 3,
                            },
                            View: {
                                status: 20,
                                datasetProfile: 8,
                                datasetUsageStatistics: 12,
                            },
                        },
                    },
                },
            },
        };

        const result = getOtherIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toEqual([
            {
                type: 'Profiling',
                count: 8,
                percent: '40%', // (8 / 20) * 100 = 40%
            },
            {
                type: 'Usage',
                count: 12,
                percent: '60%', // (12 / 20) * 100 = 60%
            },
        ]);
    });

    test('handles missing datasetProfile and datasetUsageStatistics properties', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        dataset: {
                            Table: {
                                status: 10,
                                // datasetProfile and datasetUsageStatistics are missing
                            },
                        },
                    },
                },
            },
        };

        const result = getOtherIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toEqual([
            {
                count: 0,
                percent: '0%',
                type: 'Usage',
            },
        ]);
    });

    test('ignores non-dataset entity types', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        container: {
                            Container: {
                                status: 10,
                                datasetProfile: 5,
                                datasetUsageStatistics: 3,
                            },
                        },
                        dataset: {
                            Table: {
                                status: 20,
                                datasetProfile: 8,
                                datasetUsageStatistics: 12,
                            },
                        },
                    },
                },
            },
        };

        const result = getOtherIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toEqual([
            {
                type: 'Profiling',
                count: 8,
                percent: '40%', // (8 / 20) * 100 = 40%
            },
            {
                type: 'Usage',
                count: 12,
                percent: '60%', // (12 / 20) * 100 = 60%
            },
        ]);
    });

    test('calculates percentage correctly and rounds to nearest integer', () => {
        const structuredReport = {
            source: {
                report: {
                    aspects_by_subtypes: {
                        dataset: {
                            Table: {
                                status: 7,
                                datasetProfile: 2, // 2/7 = 28.57...% rounds to 29%
                                datasetUsageStatistics: 1, // 1/7 = 14.28...% rounds to 14%
                            },
                        },
                    },
                },
            },
        };

        const result = getOtherIngestionContents(mockExecutionRequestResult(structuredReport), mockEntityRegistry);
        expect(result).toEqual([
            {
                type: 'Profiling',
                count: 2,
                percent: '29%',
            },
            {
                type: 'Usage',
                count: 1,
                percent: '14%',
            },
        ]);
    });
});

describe('buildOwnerEntities', () => {
    const entityUrn = 'urn:li:entity:123';
    const ownerUrn = 'urn:li:user:123';
    const defaultOwnerType = { urn: 'urn:li:ownershipType:custom', type: EntityType.CustomOwnershipType };

    it('should return an empty array when owners is undefined', () => {
        expect(buildOwnerEntities(entityUrn, undefined, defaultOwnerType)).toEqual([]);
    });

    it('should return an empty array when owners is empty', () => {
        expect(buildOwnerEntities(entityUrn, [], defaultOwnerType)).toEqual([]);
    });

    it('should apply all defaults when owner fields are missing', () => {
        const owners = [{ type: EntityType.CorpUser, urn: ownerUrn }];
        const result = buildOwnerEntities(entityUrn, owners, defaultOwnerType);

        expect(result).toEqual([
            {
                owner: {
                    type: 'CORP_USER',
                    urn: ownerUrn,
                    editableProperties: {
                        email: '',
                        displayName: '',
                        title: '',
                        pictureLink: '',
                    },
                    properties: {
                        displayName: '',
                        email: '',
                        active: true,
                        firstName: '',
                        lastName: '',
                        fullName: '',
                        title: '',
                    },
                    info: {
                        email: '',
                        admins: [],
                        members: [],
                        groups: [],
                        active: true,
                        displayName: '',
                        firstName: '',
                        lastName: '',
                        fullName: '',
                        title: '',
                    },
                },
                attribution: null,
                associatedUrn: entityUrn,
                type: 'CORP_USER',
                ownershipType: defaultOwnerType,
                __typename: 'Owner',
            },
        ]);
    });

    it('should override defaults with owner values', () => {
        const owners = [
            {
                type: EntityType.CorpUser,
                urn: ownerUrn,
                editableProperties: {
                    email: 'test@example.com',
                    displayName: 'Test User',
                },
                properties: {
                    displayName: 'Test User',
                    email: 'test@example.com',
                    active: false,
                },
                info: {
                    email: 'test@example.com',
                    active: false,
                    admins: ['admin1'],
                },
            },
        ];
        const result = buildOwnerEntities(entityUrn, owners, defaultOwnerType);

        expect(result[0].owner.editableProperties.email).toBe('test@example.com');
        expect(result[0].owner.editableProperties.displayName).toBe('Test User');
        expect(result[0].owner.properties.active).toBe(false);
        expect(result[0].owner.info.active).toBe(false);
        expect(result[0].owner.info.admins).toEqual(['admin1']);
    });

    it('should set ownershipType to null if not provided', () => {
        const owners = [{ type: EntityType.CorpUser, urn: ownerUrn }];
        const result = buildOwnerEntities(entityUrn, owners, undefined);
        expect(result[0].ownershipType).toBeNull();
    });

    it('should handle partial owner objects', () => {
        const owners = [
            { type: EntityType.CorpGroup, urn: ownerUrn, editableProperties: { displayName: 'Partial User' } },
        ];
        const result = buildOwnerEntities(entityUrn, owners, defaultOwnerType);
        expect(result[0].owner.editableProperties.displayName).toBe('Partial User');
        expect(result[0].owner.properties.displayName).toBe('');
        expect(result[0].owner.info.admins).toEqual([]);
    });
});

describe('getStructuredReport', () => {
    test('returns null when structured report is not available', () => {
        const result = getStructuredReport({} as Partial<ExecutionRequestResult>);
        expect(result).toBeNull();
    });

    test('returns null when both source and sink reports are missing', () => {
        const structuredReport = {
            // No source or sink
        };
        const result = getStructuredReport(mockExecutionRequestResult(structuredReport));
        expect(result).toBeNull();
    });

    test('extracts errors, warnings, and infos from source report only', () => {
        const structuredReport = {
            source: {
                report: {
                    failures: [
                        {
                            title: 'Source Error',
                            message: 'Failed to connect to source',
                            context: ['connection', 'timeout'],
                        },
                    ],
                    warnings: [
                        {
                            title: 'Source Warning',
                            message: 'Deprecated API used',
                            context: ['api', 'v1'],
                        },
                    ],
                    infos: [
                        {
                            title: 'Source Info',
                            message: 'Processing completed',
                            context: ['summary'],
                        },
                    ],
                },
            },
        };

        const result = getStructuredReport(mockExecutionRequestResult(structuredReport));
        expect(result).not.toBeNull();
        expect(result?.errorCount).toBe(1);
        expect(result?.warnCount).toBe(1);
        expect(result?.infoCount).toBe(1);
        expect(result?.items).toHaveLength(3);
    });

    test('extracts errors, warnings, and infos from sink report only', () => {
        const structuredReport = {
            sink: {
                report: {
                    failures: [
                        {
                            title: 'Sink Error',
                            message: 'Failed to write to sink',
                            context: ['write', 'permission'],
                        },
                    ],
                    warnings: [
                        {
                            title: 'Sink Warning',
                            message: 'Slow write speed',
                            context: ['performance'],
                        },
                    ],
                    infos: [
                        {
                            title: 'Sink Info',
                            message: 'Write completed',
                            context: ['summary'],
                        },
                    ],
                },
            },
        };

        const result = getStructuredReport(mockExecutionRequestResult(structuredReport));
        expect(result).not.toBeNull();
        expect(result?.errorCount).toBe(1);
        expect(result?.warnCount).toBe(1);
        expect(result?.infoCount).toBe(1);
        expect(result?.items).toHaveLength(3);
    });

    test('extracts and combines errors, warnings, and infos from both source and sink reports', () => {
        const structuredReport = {
            source: {
                report: {
                    failures: [
                        {
                            title: 'Source Error 1',
                            message: 'Failed to connect',
                            context: ['connection'],
                        },
                        {
                            title: 'Source Error 2',
                            message: 'Failed to authenticate',
                            context: ['auth'],
                        },
                    ],
                    warnings: [
                        {
                            title: 'Source Warning',
                            message: 'Deprecated field',
                            context: ['field'],
                        },
                    ],
                    infos: [
                        {
                            title: 'Source Info',
                            message: 'Processing started',
                            context: ['start'],
                        },
                    ],
                },
            },
            sink: {
                report: {
                    failures: [
                        {
                            title: 'Sink Error',
                            message: 'Write failed',
                            context: ['write'],
                        },
                    ],
                    warnings: [
                        {
                            title: 'Sink Warning 1',
                            message: 'Slow write',
                            context: ['performance'],
                        },
                        {
                            title: 'Sink Warning 2',
                            message: 'Buffer full',
                            context: ['buffer'],
                        },
                    ],
                    infos: [
                        {
                            title: 'Sink Info',
                            message: 'Write completed',
                            context: ['end'],
                        },
                    ],
                },
            },
        };

        const result = getStructuredReport(mockExecutionRequestResult(structuredReport));
        expect(result).not.toBeNull();
        expect(result?.errorCount).toBe(3); // 2 from source + 1 from sink
        expect(result?.warnCount).toBe(3); // 1 from source + 2 from sink
        expect(result?.infoCount).toBe(2); // 1 from source + 1 from sink
        expect(result?.items).toHaveLength(8);
    });

    test('handles legacy object format for failures and warnings', () => {
        const structuredReport = {
            source: {
                report: {
                    failures: {
                        'Connection error': ['host1', 'host2'],
                        'Authentication error': ['user1'],
                    },
                    warnings: {
                        'Deprecated API': ['endpoint1'],
                    },
                    infos: {},
                },
            },
        };

        const result = getStructuredReport(mockExecutionRequestResult(structuredReport));
        expect(result).not.toBeNull();
        expect(result?.errorCount).toBe(2);
        expect(result?.warnCount).toBe(1);
        expect(result?.infoCount).toBe(0);
        expect(result?.items[0].message).toBe('Connection error');
        expect(result?.items[0].context).toEqual(['host1', 'host2']);
    });

    test('handles mixed array and object formats', () => {
        const structuredReport = {
            source: {
                report: {
                    failures: [
                        {
                            title: 'Source Error',
                            message: 'Array format error',
                            context: ['context1'],
                        },
                    ],
                    warnings: {},
                    infos: [],
                },
            },
            sink: {
                report: {
                    failures: {
                        'Object format error': ['context2'],
                    },
                    warnings: [
                        {
                            title: 'Sink Warning',
                            message: 'Array format warning',
                            context: ['context3'],
                        },
                    ],
                    infos: {},
                },
            },
        };

        const result = getStructuredReport(mockExecutionRequestResult(structuredReport));
        expect(result).not.toBeNull();
        expect(result?.errorCount).toBe(2);
        expect(result?.warnCount).toBe(1);
        expect(result?.infoCount).toBe(0);
        expect(result?.items).toHaveLength(3);
    });

    test('handles empty source and sink reports', () => {
        const structuredReport = {
            source: {
                report: {
                    failures: [],
                    warnings: [],
                    infos: [],
                },
            },
            sink: {
                report: {
                    failures: [],
                    warnings: [],
                    infos: [],
                },
            },
        };

        const result = getStructuredReport(mockExecutionRequestResult(structuredReport));
        expect(result).not.toBeNull();
        expect(result?.errorCount).toBe(0);
        expect(result?.warnCount).toBe(0);
        expect(result?.infoCount).toBe(0);
        expect(result?.items).toHaveLength(0);
    });

    test('filters out string items from array format', () => {
        const structuredReport = {
            source: {
                report: {
                    failures: [
                        'sampled from 100 records',
                        {
                            title: 'Valid Error',
                            message: 'This should be included',
                            context: ['context'],
                        },
                    ],
                    warnings: [],
                    infos: [],
                },
            },
        };

        const result = getStructuredReport(mockExecutionRequestResult(structuredReport));
        expect(result).not.toBeNull();
        expect(result?.errorCount).toBe(1);
        expect(result?.items).toHaveLength(1);
        expect(result?.items[0].message).toBe('This should be included');
    });
});

describe('formatExtraArgs', () => {
    test('should return empty array when input is null', () => {
        expect(formatExtraArgs(null)).toEqual([]);
    });

    test('should return empty array when input is undefined', () => {
        expect(formatExtraArgs(undefined)).toEqual([]);
    });

    test('should return filtered and mapped array when input has valid entries', () => {
        const input = [
            { key: 'key1', value: 'value1' },
            { key: 'key2', value: 'value2' },
        ];
        expect(formatExtraArgs(input)).toEqual(input);
    });

    test('should filter out entries with null, undefined, or empty string values', () => {
        const input = [
            { key: 'key1', value: 'value1' },
            { key: 'key2', value: null },
            { key: 'key3', value: undefined },
            { key: 'key4', value: '' },
            { key: 'key5', value: 'value5' },
        ];
        const expected = [
            { key: 'key1', value: 'value1' },
            { key: 'key5', value: 'value5' },
        ];
        expect(formatExtraArgs(input)).toEqual(expected);
    });

    test('should handle empty array input', () => {
        expect(formatExtraArgs([])).toEqual([]);
    });
});

describe('getNewIngestionSourcePlaceholder', () => {
    test('should create a placeholder ingestion source with correct structure', () => {
        const urn = 'test-urn';
        const data = {
            name: 'test-source',
            type: 'test-type',
            config: {
                recipe: 'test-recipe',
            },
            schedule: {
                interval: '0 * * * *',
                timezone: 'UTC',
            },
            owners: [],
        };

        const defaultOwnershipType = {
            urn: 'urn:li:ownershipType:TEST',
            name: 'TEST',
            description: 'Test ownership type',
            type: EntityType.CustomOwnershipType,
        } as OwnershipTypeEntity;

        const result = getNewIngestionSourcePlaceholder(urn, data, defaultOwnershipType);

        expect(result).toMatchObject({
            urn: 'test-urn',
            name: 'test-source',
            type: 'test-type',
            schedule: {
                interval: '0 * * * *',
                timezone: 'UTC',
            },
            platform: null,
            executions: null,
            source: null,
            config: {
                executorId: '',
                recipe: '',
                version: null,
                debugMode: null,
                extraArgs: null,
            },
            ownership: {
                lastModified: {
                    time: 0,
                },
                __typename: 'Ownership',
            },
            __typename: 'IngestionSource',
        });
    });

    test('should handle data with missing optional fields', () => {
        const urn = 'test-urn';
        const data = {
            name: 'test-source',
            type: 'test-type',
            config: {
                recipe: 'test-recipe',
            },
            // No schedule provided
            owners: [],
        };

        const result = getNewIngestionSourcePlaceholder(urn, data, undefined);

        expect(result.schedule.interval).toBe('');
        expect(result.schedule.timezone).toBe(null);
    });
});

describe('getIngestionSourceMutationInput', () => {
    test('should create mutation input with correct structure', () => {
        const data = {
            name: 'test-source',
            type: 'test-type',
            config: {
                recipe: 'test-recipe',
                version: '1.0.0',
                executorId: 'executor-1',
                debugMode: true,
                extraArgs: [{ key: 'arg1', value: 'value1' }],
            },
            schedule: {
                interval: '0 * * * *',
                timezone: 'UTC',
            },
        };

        const result = getIngestionSourceMutationInput(data);

        expect(result).toEqual({
            type: 'test-type',
            name: 'test-source',
            config: {
                recipe: 'test-recipe',
                version: '1.0.0',
                executorId: 'executor-1',
                debugMode: true,
                extraArgs: [{ key: 'arg1', value: 'value1' }],
            },
            schedule: {
                interval: '0 * * * *',
                timezone: 'UTC',
            },
        });
    });

    test('should use default executor ID when executorId is not provided', () => {
        const data = {
            name: 'test-source',
            type: 'test-type',
            config: {
                recipe: 'test-recipe',
                executorId: '',
            },
            schedule: {
                interval: '0 * * * *',
                timezone: 'UTC',
            },
        };

        const result = getIngestionSourceMutationInput(data);

        expect(result.config.executorId).toBe('default');
    });

    test('should use undefined version when version is empty', () => {
        const data = {
            name: 'test-source',
            type: 'test-type',
            config: {
                recipe: 'test-recipe',
                version: '', // Empty string
            },
            schedule: {
                interval: '0 * * * *',
                timezone: 'UTC',
            },
        };

        const result = getIngestionSourceMutationInput(data);

        expect(result.config.version).toBeUndefined();
    });

    test('should not include schedule when no schedule is provided', () => {
        const data = {
            name: 'test-source',
            type: 'test-type',
            config: {
                recipe: 'test-recipe',
            },
            // No schedule provided
        };

        const result = getIngestionSourceMutationInput(data);

        expect(result.schedule).toBeUndefined();
    });

    test('should preserve source field when editing existing sources', () => {
        const data = {
            name: 'test-source',
            type: 'test-type',
            config: {
                recipe: 'test-recipe',
            },
            schedule: {
                interval: '0 * * * *',
                timezone: 'UTC',
            },
        };

        const existingSource = {
            urn: 'test-urn',
            name: 'test-source',
            type: 'test-type',
            config: { recipe: '', executorId: '' },
            source: {
                type: 'SYSTEM',
            },
        } as IngestionSource;

        const result = getIngestionSourceMutationInput(data, existingSource);

        expect(result.source).toEqual({ type: 'SYSTEM' });
    });

    test('should not include source field for new sources', () => {
        const data = {
            name: 'test-source',
            type: 'test-type',
            config: {
                recipe: 'test-recipe',
            },
            schedule: {
                interval: '0 * * * *',
                timezone: 'UTC',
            },
        };

        const result = getIngestionSourceMutationInput(data);

        expect(result.source).toBeUndefined();
    });

    test('should format extraArgs using formatExtraArgs function', () => {
        const data = {
            name: 'test-source',
            type: 'test-type',
            config: {
                recipe: 'test-recipe',
                extraArgs: [
                    { key: 'key1', value: 'value1' },
                    { key: 'key2', value: '' },
                    { key: 'key3', value: '' },
                ],
            },
            schedule: {
                interval: '0 * * * *',
                timezone: 'UTC',
            },
        };

        const result = getIngestionSourceMutationInput(data);

        expect(result.config.extraArgs).toEqual([{ key: 'key1', value: 'value1' }]);
    });
});
